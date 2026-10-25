//! The node (opening partitions, idle suspend and its events) and
//! point-in-time restore, ported from the Go owner's suspend_test.go and
//! restore_test.go.

mod common;

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use common::*;
use kvd::error::Kind;
use kvd::node::{Event, Node};
use kvd::partition::{Config, Isolation};
use kvd::restore;

#[tokio::test]
async fn concurrent_opens_share_one_takeover() {
    let node = Node::new(cfg(), mem(), None);
    let opens: Vec<_> = (0..8)
        .map(|_| {
            let node = node.clone();
            tokio::spawn(async move { node.open(PID).await.unwrap().epoch })
        })
        .collect();
    let mut epochs = vec![];
    for o in opens {
        epochs.push(o.await.unwrap());
    }
    assert!(epochs.iter().all(|e| *e == epochs[0]), "one takeover: {epochs:?}");
    assert_eq!(node.partitions(), vec![(PID, epochs[0])]);
}

#[tokio::test]
async fn idle_suspend_releases_and_reports_empty() {
    let store = mem();
    let (tx, mut events) = tokio::sync::mpsc::unbounded_channel();
    let node = Node::new(cfg(), store.clone(), Some(tx));
    let p = node.open(PID).await.unwrap();
    put(&p, 1, "kept").await.unwrap();
    // An open transaction keeps the partition awake.
    let open_txn = begin(&p).await;
    node.spawn_idle_loop(Duration::from_millis(300));
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert!(events.try_recv().is_err(), "suspended with a transaction open");
    p.rollback(p.epoch, open_txn).unwrap();
    let ev = tokio::time::timeout(Duration::from_secs(5), events.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        ev,
        Event::Suspended {
            partition: PID,
            epoch: p.epoch
        }
    );
    let ev = tokio::time::timeout(Duration::from_secs(5), events.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ev, Event::Empty);
    assert!(node.partitions().is_empty());
    // Requests to the suspended partition get EpochChanged; the next open wakes it.
    let err = p
        .begin(p.epoch, txn_id(), Isolation::RepeatableRead, false)
        .unwrap_err();
    assert_eq!(err.kind, Kind::EpochChanged);
    assert_eq!(node.partition(PID).err().map(|e| e.kind), Some(Kind::EpochChanged));
    let q = node.open(PID).await.unwrap();
    assert!(q.epoch > p.epoch);
    assert_eq!(get(&q, 1).await, "kept");
}

#[tokio::test]
async fn restore_to_a_time() {
    let store = mem();
    let p = open(&store).await;
    for i in 0..5 {
        put(&p, i, "old").await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(20)).await;
    let at = SystemTime::now();
    tokio::time::sleep(Duration::from_millis(20)).await;
    put(&p, 1, "new").await.unwrap();
    put(&p, 9, "added").await.unwrap();
    let points = restore::restore_points(store.clone(), PID).await.unwrap().unwrap();
    assert!(points.0 <= at && at <= points.1);

    const DST: u64 = 8;
    let r = restore::restore(store.clone(), PID, DST, at).await.unwrap();
    assert_eq!(r.rows, 5);
    // The restored keys carry the new partition id.
    let q = kvd::partition::Partition::takeover(cfg(), store.clone(), DST)
        .await
        .unwrap();
    let id = begin(&q).await;
    let mut start = DST.to_be_bytes().to_vec();
    start.extend_from_slice(&key(0)[8..]);
    let (rows, _) = q.scan(q.epoch, id, &start, &[], 0, ro(1)).await.unwrap();
    let vals: Vec<String> = rows
        .iter()
        .map(|(_, v)| String::from_utf8(v.clone()).unwrap())
        .collect();
    assert_eq!(vals, ["old"; 5]);
    assert!(rows.iter().all(|(k, _)| k.starts_with(&DST.to_be_bytes())));
    // The source is untouched, and a used partition is refused.
    assert_eq!(get(&p, 1).await, "new");
    assert!(restore::restore(store.clone(), PID, DST, at).await.is_err());
}

#[tokio::test]
async fn restore_before_the_window_fails() {
    let store = mem();
    let p = open(&store).await;
    let before = SystemTime::now() - Duration::from_secs(60);
    put(&p, 1, "x").await.unwrap();
    assert!(restore::restore(store, PID, 8, before).await.is_err());
}

#[tokio::test]
async fn restore_window_bounds_gc() {
    // With a zero window, GC drops the history a restore would need.
    let store = mem();
    let p = open_with(
        &store,
        Config {
            restore_window: Duration::ZERO,
            ..cfg()
        },
    )
    .await;
    put(&p, 1, "a").await.unwrap();
    put(&p, 1, "b").await.unwrap();
    assert_eq!(p.collect_garbage().await.unwrap(), 1);
    // With the default window (1 h), it keeps everything.
    let q = open_with(&mem(), cfg()).await;
    put(&q, 1, "a").await.unwrap();
    put(&q, 1, "b").await.unwrap();
    assert_eq!(q.collect_garbage().await.unwrap(), 0);
    let _: Arc<kvd::partition::Partition> = q;
}
