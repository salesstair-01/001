//! Transactions and locks on one partition, ported from the Go owner's
//! internal/kv tests (kv_test.go, locks_test.go).

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use common::*;
use kvd::error::Kind;
use kvd::locks::LockMode;
use kvd::partition::{Config, Guard, Isolation, Mutation, Op, ReadOptions, TxnState};

fn s(v: &Option<Vec<u8>>) -> &str {
    v.as_deref().map(|b| std::str::from_utf8(b).unwrap()).unwrap_or("")
}

#[tokio::test]
async fn commit_and_restart() {
    let store = mem();
    let p = open(&store).await;
    for i in 0..50 {
        put(&p, i, &format!("v{i}")).await.unwrap();
    }
    assert_eq!(get(&p, 3).await, "v3");
    let epoch = p.epoch;
    p.close().await;
    let q = open(&store).await;
    for i in 0..50 {
        assert_eq!(get(&q, i).await, format!("v{i}"), "key {i}");
    }
    assert_eq!(q.epoch, epoch + 1);
}

#[tokio::test]
async fn snapshot_isolation() {
    let p = open(&mem()).await;
    put(&p, 1, "old").await.unwrap();
    let a = begin(&p).await;
    assert_eq!(s(&p.get(p.epoch, a, &[key(1)], ro(1)).await.unwrap()[0]), "old");
    put(&p, 1, "new").await.unwrap();
    assert_eq!(
        s(&p.get(p.epoch, a, &[key(1)], ro(2)).await.unwrap()[0]),
        "old",
        "repeatable read keeps the old value"
    );
    // READ COMMITTED sees the new value on its next statement.
    let rc = txn_id();
    p.begin(p.epoch, rc, Isolation::ReadCommitted, false).unwrap();
    p.get(p.epoch, rc, &[key(1)], ro(1)).await.unwrap();
    put(&p, 1, "newer").await.unwrap();
    assert_eq!(s(&p.get(p.epoch, rc, &[key(1)], ro(2)).await.unwrap()[0]), "newer");
    // A locking read sees the latest committed version, not the snapshot.
    let lock = ReadOptions {
        stmt: 3,
        lock: LockMode::Exclusive,
        ..Default::default()
    };
    assert_eq!(s(&p.get(p.epoch, a, &[key(1)], lock).await.unwrap()[0]), "newer");
}

#[tokio::test]
async fn rollback_and_statement_rollback() {
    let p = open(&mem()).await;
    put(&p, 1, "a").await.unwrap();
    let id = begin(&p).await;
    write(&p, id, 1, 2, "b", Duration::ZERO).await.unwrap();
    write(&p, id, 2, 3, "c", Duration::ZERO).await.unwrap();
    let dup = Mutation {
        op: Op::InsertOnly,
        key: key(1),
        value: b"dup".to_vec(),
    };
    let err = p.mutate(p.epoch, id, 2, vec![dup], Duration::ZERO).await.unwrap_err();
    assert_eq!(err.kind, Kind::DuplicateKey);
    assert_eq!(err.key, key(1));
    p.rollback_statement(p.epoch, id, 2).unwrap();
    let (rows, _) = p.scan(p.epoch, id, &key(0), &key(10), 0, ro(3)).await.unwrap();
    assert_eq!(rows.len(), 2, "scan inside the transaction");
    p.commit(p.epoch, id, vec![]).await.unwrap();
    assert_eq!(get(&p, 2).await, "b");
    assert_eq!(get(&p, 3).await, "", "statement rollback");
    let id2 = begin(&p).await;
    p.mutate(
        p.epoch,
        id2,
        1,
        vec![Mutation {
            op: Op::Delete,
            key: key(1),
            value: vec![],
        }],
        Duration::ZERO,
    )
    .await
    .unwrap();
    p.rollback(p.epoch, id2).unwrap();
    assert_eq!(get(&p, 1).await, "a", "rollback left a trace");
}

#[tokio::test]
async fn scan_merges_intents_and_pages() {
    let p = open(&mem()).await;
    for i in (0..10).step_by(2) {
        put(&p, i, &i.to_string()).await.unwrap();
    }
    let id = begin(&p).await;
    p.mutate(
        p.epoch,
        id,
        1,
        vec![
            put_mut(3, "3"),
            Mutation {
                op: Op::Delete,
                key: key(4),
                value: vec![],
            },
            put_mut(6, "six"),
        ],
        Duration::ZERO,
    )
    .await
    .unwrap();
    let mut got = vec![];
    let mut start = key(0);
    loop {
        let (rows, resume) = p.scan(p.epoch, id, &start, &key(100), 2, ro(2)).await.unwrap();
        assert!(rows.len() <= 2);
        got.extend(rows.into_iter().map(|(_, v)| String::from_utf8(v).unwrap()));
        match resume {
            Some(r) => start = r,
            None => break,
        }
    }
    assert_eq!(got, ["0", "2", "3", "six", "8"]);
}

#[tokio::test]
async fn empty_values_are_rows() {
    // Secondary-index entries have empty values: present, not absent.
    let p = open(&mem()).await;
    let id = begin(&p).await;
    write(&p, id, 1, 1, "", Duration::ZERO).await.unwrap();
    assert_eq!(
        p.get(p.epoch, id, &[key(1)], ro(2)).await.unwrap()[0],
        Some(vec![]),
        "own intent"
    );
    p.commit(p.epoch, id, vec![]).await.unwrap();
    let r = begin(&p).await;
    assert_eq!(
        p.get(p.epoch, r, &[key(1)], ro(1)).await.unwrap()[0],
        Some(vec![]),
        "committed"
    );
    let (rows, _) = p.scan(p.epoch, r, &key(0), &key(5), 0, ro(1)).await.unwrap();
    assert_eq!(rows, vec![(key(1), vec![])]);
}

#[tokio::test]
async fn txn_status_and_idempotent_commit() {
    let store = mem();
    let p = open(&store).await;
    let id = begin(&p).await;
    write(&p, id, 1, 1, "x", Duration::ZERO).await.unwrap();
    let ts = p.commit(p.epoch, id, vec![]).await.unwrap();
    assert_eq!(
        p.commit(p.epoch, id, vec![]).await.unwrap(),
        ts,
        "a retried commit returns the original commit_ts"
    );
    p.close().await;
    let q = open(&store).await;
    assert_eq!(q.txn_status(q.epoch, id).unwrap(), (TxnState::Committed, ts));
    assert_eq!(q.txn_status(q.epoch, txn_id()).unwrap().0, TxnState::Unknown);
}

#[tokio::test]
async fn epoch_mismatch() {
    let p = open(&mem()).await;
    let err = p
        .begin(p.epoch + 1, txn_id(), Isolation::RepeatableRead, false)
        .unwrap_err();
    assert_eq!(err.kind, Kind::EpochChanged);
}

#[tokio::test]
async fn group_commit_concurrent() {
    let store = mem();
    let p = open(&store).await;
    let mut tasks = vec![];
    for i in 0..200 {
        let p = p.clone();
        tasks.push(tokio::spawn(async move { put(&p, i, "c").await }));
    }
    for t in tasks {
        t.await.unwrap().unwrap();
    }
    p.close().await;
    let q = open(&store).await;
    for i in 0..200 {
        assert_eq!(get(&q, i).await, "c", "key {i} lost");
    }
}

#[tokio::test]
async fn auto_increment_survives_restart() {
    let store = mem();
    let p = open(&store).await;
    let id = begin(&p).await;
    assert_eq!(p.auto_increment(p.epoch, id, 2000, 3, 0).await.unwrap(), 1);
    // A rolled-back transaction's values are not handed out again.
    let rb = begin(&p).await;
    assert_eq!(p.auto_increment(p.epoch, rb, 2000, 1, 0).await.unwrap(), 4);
    p.rollback(p.epoch, rb).unwrap();
    assert_eq!(
        p.auto_increment(p.epoch, id, 2000, 0, 100).await.unwrap(),
        100,
        "an explicit value raises the counter"
    );
    write(&p, id, 1, 1, "x", Duration::ZERO).await.unwrap();
    p.commit(p.epoch, id, vec![]).await.unwrap();
    p.close().await;
    let q = open(&store).await;
    assert_eq!(
        q.auto_increment(q.epoch, [0; 16], 2000, 0, 0).await.unwrap(),
        100,
        "counter after restart"
    );
}

// A snapshot taken before anything was committed has timestamp 0; reads in
// it must stay repeatable.
#[tokio::test]
async fn snapshot_on_empty_partition_is_repeatable() {
    for iso in [Isolation::RepeatableRead, Isolation::ReadCommitted] {
        let p = open(&mem()).await;
        let r = txn_id();
        p.begin(p.epoch, r, iso, false).unwrap();
        assert_eq!(p.get(p.epoch, r, &[key(100)], ro(1)).await.unwrap()[0], None);
        put(&p, 100, "later").await.unwrap();
        assert_eq!(
            p.get(p.epoch, r, &[key(100)], ro(1)).await.unwrap()[0],
            None,
            "{iso:?}: same statement"
        );
    }
}

#[tokio::test]
async fn commit_guards() {
    let p = open(&mem()).await;
    put(&p, 1, "v1").await.unwrap();
    let ok = begin(&p).await;
    write(&p, ok, 1, 2, "x", Duration::ZERO).await.unwrap();
    let guards = vec![
        Guard {
            key: key(1),
            value: b"v1".to_vec(),
        },
        Guard {
            key: key(9),
            value: vec![],
        },
    ];
    p.commit(p.epoch, ok, guards).await.expect("guards that hold");
    let stale = begin(&p).await;
    write(&p, stale, 1, 3, "y", Duration::ZERO).await.unwrap();
    put(&p, 1, "v2").await.unwrap();
    let err = p
        .commit(
            p.epoch,
            stale,
            vec![Guard {
                key: key(1),
                value: b"v1".to_vec(),
            }],
        )
        .await
        .unwrap_err();
    assert_eq!(err.kind, Kind::SchemaChanged);
    assert_eq!(get(&p, 3).await, "", "a failed guard commits nothing");
}

#[tokio::test]
async fn mvcc_gc_drops_invisible_versions() {
    let p = open_with(
        &mem(),
        Config {
            restore_window: Duration::ZERO,
            ..cfg()
        },
    )
    .await;
    for v in 0..5 {
        put(&p, 1, &v.to_string()).await.unwrap();
    }
    put(&p, 2, "gone").await.unwrap();
    let id = begin(&p).await;
    p.mutate(
        p.epoch,
        id,
        1,
        vec![Mutation {
            op: Op::Delete,
            key: key(2),
            value: vec![],
        }],
        Duration::ZERO,
    )
    .await
    .unwrap();
    p.commit(p.epoch, id, vec![]).await.unwrap();
    assert_eq!(p.collect_garbage().await.unwrap(), 4 + 2);
    assert_eq!(get(&p, 1).await, "4");
    assert_eq!(get(&p, 2).await, "");
    assert_eq!(p.collect_garbage().await.unwrap(), 0, "nothing left to drop");
}

#[tokio::test]
async fn mvcc_gc_keeps_open_snapshots_and_the_restore_window() {
    // An open snapshot keeps what it can read.
    let p = open_with(
        &mem(),
        Config {
            restore_window: Duration::ZERO,
            ..cfg()
        },
    )
    .await;
    put(&p, 1, "a").await.unwrap();
    let r = begin(&p).await;
    p.get(p.epoch, r, &[key(1)], ro(1)).await.unwrap();
    put(&p, 1, "b").await.unwrap();
    assert_eq!(p.collect_garbage().await.unwrap(), 0);
    assert_eq!(s(&p.get(p.epoch, r, &[key(1)], ro(2)).await.unwrap()[0]), "a");
    // The restore window keeps every version newer than it.
    let q = open_with(&mem(), cfg()).await;
    for v in 0..5 {
        put(&q, 1, &v.to_string()).await.unwrap();
    }
    assert_eq!(q.collect_garbage().await.unwrap(), 0);
}

// ---- locks ------------------------------------------------------------------

#[tokio::test]
async fn write_waits_for_lock() {
    let p = open(&mem()).await;
    put(&p, 1, "0").await.unwrap();
    let (a, b) = (begin(&p).await, begin(&p).await);
    write(&p, a, 1, 1, "a", Duration::ZERO).await.unwrap();
    let p2 = p.clone();
    let waiter = tokio::spawn(async move { write(&p2, b, 1, 1, "b", Duration::ZERO).await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!waiter.is_finished(), "b must wait for a's lock");
    p.commit(p.epoch, a, vec![]).await.unwrap();
    waiter.await.unwrap().expect("b after a committed");
    p.commit(p.epoch, b, vec![]).await.unwrap();
    assert_eq!(get(&p, 1).await, "b");
}

#[tokio::test]
async fn lock_wait_timeout() {
    let p = open(&mem()).await;
    let (a, b) = (begin(&p).await, begin(&p).await);
    write(&p, a, 1, 1, "a", Duration::ZERO).await.unwrap();
    let start = Instant::now();
    let err = write(&p, b, 1, 1, "b", Duration::from_millis(100)).await.unwrap_err();
    assert_eq!(err.kind, Kind::LockTimeout);
    assert!(start.elapsed() >= Duration::from_millis(100));
    // The transaction survives a lock timeout; only the statement failed.
    write(&p, b, 2, 2, "b2", Duration::ZERO).await.unwrap();
    p.commit(p.epoch, a, vec![]).await.unwrap();
    p.commit(p.epoch, b, vec![]).await.unwrap();
}

#[tokio::test]
async fn deadlock_youngest_is_victim() {
    let p = open(&mem()).await;
    let old = begin(&p).await;
    tokio::time::sleep(Duration::from_millis(2)).await;
    let young = begin(&p).await;
    write(&p, old, 1, 1, "old", Duration::ZERO).await.unwrap();
    write(&p, young, 1, 2, "young", Duration::ZERO).await.unwrap();
    let p2 = p.clone();
    let old_waits = tokio::spawn(async move { write(&p2, old, 2, 2, "old", Duration::ZERO).await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    let err = write(&p, young, 2, 1, "young", Duration::ZERO).await.unwrap_err(); // closes the cycle
    assert_eq!(err.kind, Kind::Deadlock);
    old_waits.await.unwrap().expect("the survivor gets the lock");
    assert_eq!(
        p.commit(p.epoch, young, vec![]).await.unwrap_err().kind,
        Kind::TxnNotFound
    );
    p.commit(p.epoch, old, vec![]).await.unwrap();
    assert_eq!((get(&p, 1).await, get(&p, 2).await), ("old".into(), "old".into()));
}

#[tokio::test]
async fn deadlock_older_waiter_victimized() {
    let p = open(&mem()).await;
    let old = begin(&p).await;
    tokio::time::sleep(Duration::from_millis(2)).await;
    let young = begin(&p).await;
    write(&p, old, 1, 1, "old", Duration::ZERO).await.unwrap();
    write(&p, young, 1, 2, "young", Duration::ZERO).await.unwrap();
    let p2 = p.clone();
    let young_waits = tokio::spawn(async move { write(&p2, young, 2, 1, "young", Duration::ZERO).await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    write(&p, old, 2, 2, "old", Duration::ZERO)
        .await
        .expect("the older transaction wins");
    assert_eq!(young_waits.await.unwrap().unwrap_err().kind, Kind::Deadlock);
}

#[tokio::test]
async fn shared_locks() {
    let p = open(&mem()).await;
    put(&p, 1, "v").await.unwrap();
    let (a, b, c) = (begin(&p).await, begin(&p).await, begin(&p).await);
    let shared = ReadOptions {
        stmt: 1,
        lock: LockMode::Shared,
        ..Default::default()
    };
    for id in [a, b] {
        p.get(p.epoch, id, &[key(1)], shared).await.unwrap();
    }
    let err = write(&p, c, 1, 1, "c", Duration::from_millis(50)).await.unwrap_err();
    assert_eq!(err.kind, Kind::LockTimeout, "a writer waits for shared holders");
    p.commit(p.epoch, a, vec![]).await.unwrap();
    p.commit(p.epoch, b, vec![]).await.unwrap();
    write(&p, c, 2, 1, "c", Duration::ZERO).await.unwrap();
}

#[tokio::test]
async fn locking_scan_reads_latest() {
    let p = open(&mem()).await;
    for i in 0..5 {
        put(&p, i, "0").await.unwrap();
    }
    let a = begin(&p).await;
    p.scan(p.epoch, a, &key(0), &key(10), 0, ro(1)).await.unwrap(); // takes the snapshot
    put(&p, 2, "1").await.unwrap();
    let lock = ReadOptions {
        stmt: 2,
        lock: LockMode::Exclusive,
        ..Default::default()
    };
    let (rows, _) = p.scan(p.epoch, a, &key(0), &key(10), 0, lock).await.unwrap();
    assert_eq!(rows.len(), 5);
    assert_eq!(rows[2].1, b"1", "a locking scan sees the latest versions");
    let b = begin(&p).await;
    let err = write(&p, b, 1, 4, "b", Duration::from_millis(30)).await.unwrap_err();
    assert_eq!(err.kind, Kind::LockTimeout, "rows read FOR UPDATE are locked");
}

#[tokio::test]
async fn lease_expiry_frees_locks() {
    let p = open_with(
        &mem(),
        Config {
            txn_lease: Duration::from_millis(500),
            ..cfg()
        },
    )
    .await;
    let dead = begin(&p).await;
    write(&p, dead, 1, 1, "dead", Duration::ZERO).await.unwrap(); // its query engine dies here
    let b = begin(&p).await;
    let p2 = p.clone();
    let beat = tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(100)).await;
            if p2.heartbeat(p2.epoch, &[b]).is_err() {
                return;
            }
        }
    });
    let start = Instant::now();
    write(&p, b, 1, 1, "b", Duration::from_secs(10))
        .await
        .expect("b gets the lock once the lease expires");
    assert!(start.elapsed() < Duration::from_secs(4), "lease expiry too slow");
    beat.abort();
    assert_eq!(p.txn_status(p.epoch, dead).unwrap().0, TxnState::Unknown);
}

/// Many transactions move money between accounts with locking reads; the
/// total never changes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn money_transfers() {
    let p = open(&mem()).await;
    const ACCOUNTS: i64 = 10;
    for i in 0..ACCOUNTS {
        put(&p, i, "1000").await.unwrap();
    }
    let committed = Arc::new(AtomicU64::new(0));
    let mut tasks = vec![];
    for w in 0..8i64 {
        let (p, committed) = (p.clone(), committed.clone());
        tasks.push(tokio::spawn(async move {
            for i in 0..50i64 {
                let (from, to) = ((w + i) % ACCOUNTS, (w * 3 + i * 7 + 1) % ACCOUNTS);
                if from == to {
                    continue;
                }
                let id = begin(&p).await;
                let lock = ReadOptions {
                    stmt: 1,
                    lock: LockMode::Exclusive,
                    ..Default::default()
                };
                let res = async {
                    let vs = p.get(p.epoch, id, &[key(from), key(to)], lock).await?;
                    let n = |v: &Option<Vec<u8>>| s(v).parse::<i64>().unwrap();
                    let (a, b) = (n(&vs[0]), n(&vs[1]));
                    p.mutate(
                        p.epoch,
                        id,
                        2,
                        vec![put_mut(from, &(a - 1).to_string()), put_mut(to, &(b + 1).to_string())],
                        Duration::ZERO,
                    )
                    .await?;
                    p.commit(p.epoch, id, vec![]).await
                }
                .await;
                match res {
                    Ok(_) => {
                        committed.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        let _ = p.rollback(p.epoch, id);
                    }
                }
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    let r = begin(&p).await;
    let (rows, _) = p.scan(p.epoch, r, &key(0), &key(ACCOUNTS), 0, ro(1)).await.unwrap();
    let total: i64 = rows
        .iter()
        .map(|(_, v)| std::str::from_utf8(v).unwrap().parse::<i64>().unwrap())
        .sum();
    assert_eq!(total, ACCOUNTS * 1000);
    assert!(committed.load(Ordering::Relaxed) > 0);
}

#[tokio::test]
async fn scan_skips_many_versions() {
    // The restore window keeps every version: a scan seeks past the ones
    // it does not need, at an old snapshot and at a new one.
    let p = open(&mem()).await;
    for i in 1..=4 {
        put(&p, i, "v0").await.unwrap();
    }
    let old = begin(&p).await;
    let (rows, _) = p.scan(p.epoch, old, &key(0), &key(10), 0, ro(1)).await.unwrap();
    assert_eq!(rows.len(), 4);
    for n in 1..=50 {
        put(&p, 2, &format!("v{n}")).await.unwrap();
        let id = begin(&p).await;
        let del = Mutation {
            op: Op::Delete,
            key: key(3),
            value: vec![],
        };
        let m = if n % 2 == 0 { put_mut(3, &format!("v{n}")) } else { del };
        p.mutate(p.epoch, id, 1, vec![m], Duration::ZERO).await.unwrap();
        p.commit(p.epoch, id, vec![]).await.unwrap();
    }
    let vals = |rows: Vec<(Vec<u8>, Vec<u8>)>| {
        rows.into_iter()
            .map(|(_, v)| String::from_utf8(v).unwrap())
            .collect::<Vec<_>>()
    };
    let (rows, _) = p.scan(p.epoch, old, &key(0), &key(10), 0, ro(1)).await.unwrap();
    assert_eq!(vals(rows), ["v0", "v0", "v0", "v0"], "the old snapshot");
    let new = begin(&p).await;
    let (rows, _) = p.scan(p.epoch, new, &key(0), &key(10), 0, ro(1)).await.unwrap();
    assert_eq!(vals(rows), ["v0", "v50", "v50", "v0"], "the newest versions");
    let (rows, _) = p.scan(p.epoch, new, &key(2), &key(3), 0, ro(1)).await.unwrap();
    assert_eq!(vals(rows), ["v50"], "a range ending after the rewritten key");
    let (rows, _) = p.scan(p.epoch, new, &key(1), &key(4), 2, ro(1)).await.unwrap();
    assert_eq!(vals(rows), ["v0", "v50"], "a page");
}
