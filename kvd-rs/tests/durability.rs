//! Fencing, takeover and object-store faults, ported from the Go owner's
//! kv_test.go and fault_test.go: an acknowledged commit is never lost, and
//! an owner that has been taken over can no longer commit.
//!
//!     KVD_FAULT_SEEDS=1000 cargo test --release --test durability faults

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use common::*;
use kvd::error::Kind;
use kvd::partition::Config;
use parking_lot::Mutex;

#[tokio::test]
async fn fencing() {
    let store = mem();
    let a = open(&store).await;
    put(&a, 1, "a1").await.unwrap();
    let b = open(&store).await;
    assert!(b.epoch > a.epoch);
    let err = put(&a, 2, "a2").await.unwrap_err();
    assert_eq!(err.kind, Kind::EpochChanged, "the fenced owner's commit fails");
    assert!(a.stopped().is_some(), "and it stops serving");
    assert_eq!(get(&b, 1).await, "a1");
    assert_eq!(get(&b, 2).await, "", "the fenced write is not visible");
    put(&b, 3, "b3").await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn takeover_during_commits() {
    let store = mem();
    let a = open(&store).await;
    let acked = Arc::new(Mutex::new(Vec::new()));
    let mut tasks = vec![];
    for w in 0..4i64 {
        let (a, acked) = (a.clone(), acked.clone());
        tasks.push(tokio::spawn(async move {
            for i in 0..200i64 {
                let k = w * 1000 + i;
                match put(&a, k, "v").await {
                    Ok(_) => acked.lock().push(k),
                    Err(_) => return, // fenced: the owner stopped
                }
            }
        }));
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    let b = open(&store).await;
    for t in tasks {
        t.await.unwrap();
    }
    let acked = acked.lock().clone();
    assert!(!acked.is_empty());
    for k in acked {
        assert_eq!(get(&b, k).await, "v", "acknowledged key {k} lost in the takeover");
    }
}

/// One seeded run: writers commit while store writes fail or lose their
/// responses; the owner stops at the first failure it can't hide. A new
/// owner then sees every acknowledged commit.
async fn fault_run(seed: u64, heavy: bool) -> (usize, u64) {
    let mem = mem();
    let faulty = FaultyStore::new(mem.clone(), seed);
    let store: Arc<dyn object_store::ObjectStore> = faulty.clone();
    let p = open_with(
        &store,
        Config {
            flush_interval: Duration::from_millis(5),
            ..cfg()
        },
    )
    .await;
    let mut r = rng(seed);
    let level = if heavy { 0.25 } else { 0.02 };
    faulty.set(Faults {
        fail: level + gen_range(&mut r, 5) as f64 / 100.0,
        drop: level,
    });
    let acked = Arc::new(Mutex::new(BTreeMap::new()));
    let mut tasks = vec![];
    for w in 0..4i64 {
        let (p, acked) = (p.clone(), acked.clone());
        tasks.push(tokio::spawn(async move {
            for i in 0..30i64 {
                let k = w * 100 + i % 10; // keys are rewritten: the last acknowledged value must win
                let v = format!("{w}-{i}");
                let res = tokio::time::timeout(Duration::from_secs(20), put(&p, k, &v)).await;
                match res {
                    Ok(Ok(_)) => {
                        acked.lock().insert(k, v);
                    }
                    Ok(Err(e)) if e.kind == Kind::Unavailable || e.kind == Kind::EpochChanged => return,
                    Ok(Err(_)) => {}
                    Err(_) => panic!("seed {seed}: a commit hung"),
                }
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    p.close().await;
    // A new owner on the healthy store sees every acknowledged write.
    let q = open(&mem).await;
    let acked = acked.lock().clone();
    for (k, v) in &acked {
        assert_eq!(&get(&q, *k).await, v, "seed {seed}: key {k}");
    }
    (acked.len(), faulty.injected.load(std::sync::atomic::Ordering::Relaxed))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn faults() {
    let seeds: u64 = std::env::var("KVD_FAULT_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(20);
    let (mut acked, mut injected) = (0, 0);
    for seed in 1..=seeds {
        let (a, i) = fault_run(seed, seed % 2 == 0).await; // every other run with heavy faults
        acked += a;
        injected += i;
    }
    eprintln!("{seeds} seeds: {acked} keys acknowledged, {injected} faults injected");
    assert!(acked > 0 && injected > 0, "the runs must commit and see faults");
}

/// A WAL write that lands but whose response is lost: the commit is reported
/// as failed (its outcome unknown), and TxnStatus on the next owner tells
/// the truth.
#[tokio::test]
async fn lost_acknowledgement() {
    let mem = mem();
    let faulty = FaultyStore::new(mem.clone(), 1);
    let store: Arc<dyn object_store::ObjectStore> = faulty.clone();
    let p = open(&store).await;
    put(&p, 1, "before").await.unwrap();
    faulty.set(Faults { fail: 0.0, drop: 1.0 });
    let id = begin(&p).await;
    write(&p, id, 1, 2, "maybe", Duration::ZERO).await.unwrap();
    let res = tokio::time::timeout(Duration::from_secs(30), p.commit(p.epoch, id, vec![])).await;
    faulty.set(Faults::default());
    let q = open(&mem).await;
    let (state, _) = q.txn_status(q.epoch, id).unwrap();
    match res {
        Ok(Ok(_)) => assert_eq!(state, kvd::partition::TxnState::Committed, "acknowledged, so committed"),
        _ => {
            // Not acknowledged: either outcome is allowed, but TxnStatus and the data agree.
            let visible = get(&q, 2).await == "maybe";
            assert_eq!(visible, state == kvd::partition::TxnState::Committed);
        }
    }
    assert_eq!(get(&q, 1).await, "before");
}

/// With the compactor and garbage collector started after takeover, the
/// partition still commits, recovers and closes.
#[tokio::test]
async fn deferred_background() {
    let store = mem();
    let cfg = Config {
        deferred_background: true,
        l0_sst_bytes: 4 << 10,
        ..cfg()
    };
    for round in 0..3i64 {
        let p = tokio::time::timeout(Duration::from_secs(10), open_with(&store, cfg.clone()))
            .await
            .expect("takeover");
        for i in 0..50 {
            put(&p, round * 100 + i, "v").await.unwrap();
        }
        if round > 0 {
            assert_eq!(get(&p, (round - 1) * 100 + 7).await, "v");
        }
        tokio::time::timeout(Duration::from_secs(10), p.close())
            .await
            .expect("close");
    }
}
