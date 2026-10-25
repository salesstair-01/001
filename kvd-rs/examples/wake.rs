//! Wake latency: takes a partition over repeatedly on the S3 store from
//! MINIO_* and reports the takeover and first-read times, with the
//! compactor and garbage collector started inside the open or deferred.
//!
//!     cargo run --release --example wake -- [rows] [rounds]

use std::sync::Arc;
use std::time::{Duration, Instant};

use kvd::locks::LockMode;
use kvd::partition::{Config, Isolation, Mutation, Op, Partition, ReadOptions};
use object_store::aws::AmazonS3Builder;

fn env(k: &str, d: &str) -> String {
    std::env::var(k).unwrap_or_else(|_| d.into())
}

fn key(pid: u64, i: u64) -> Vec<u8> {
    let mut k = pid.to_be_bytes().to_vec();
    k.extend_from_slice(&1024u32.to_be_bytes());
    k.extend_from_slice(&0u32.to_be_bytes());
    k.extend_from_slice(&i.to_be_bytes());
    k
}

fn pct(v: &mut [Duration], p: f64) -> Duration {
    v.sort();
    v[((v.len() as f64 - 1.0) * p).round() as usize]
}

#[tokio::main]
async fn main() {
    let args: Vec<u64> = std::env::args().skip(1).filter_map(|a| a.parse().ok()).collect();
    let rows = args.first().copied().unwrap_or(10_000);
    let rounds = args.get(1).copied().unwrap_or(10);
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(
        AmazonS3Builder::new()
            .with_endpoint(format!("http://{}", env("MINIO_ENDPOINT", "127.0.0.1:8000")))
            .with_allow_http(true)
            .with_access_key_id(env("MINIO_ACCESS_KEY", "minioadmin"))
            .with_secret_access_key(env("MINIO_SECRET_KEY", "minioadmin"))
            .with_bucket_name(env("MINIO_BUCKET", "s2z"))
            .with_region(env("MINIO_REGION", "us-east-1"))
            .build()
            .unwrap(),
    );
    let pid = 900_000_000 + (std::process::id() as u64);
    let base = Config {
        owner_id: "bench".into(),
        ..Config::default()
    };

    // Load, in transactions of 1,000 rows of 1 KiB.
    let p = Partition::takeover(base.clone(), store.clone(), pid).await.unwrap();
    let value = vec![b'x'; 1024];
    let mut n = 0u64;
    while n < rows {
        let mut id = [0u8; 16];
        id[8..].copy_from_slice(&n.to_be_bytes());
        p.begin(p.epoch, id, Isolation::RepeatableRead, false).unwrap();
        let muts = (n..(n + 1000).min(rows))
            .map(|i| Mutation {
                op: Op::Put,
                key: key(pid, i),
                value: value.clone(),
            })
            .collect();
        p.mutate(p.epoch, id, 1, muts, Duration::ZERO).await.unwrap();
        p.commit(p.epoch, id, vec![]).await.unwrap();
        n += 1000;
    }
    p.close().await;
    println!("partition {pid}: {rows} rows of 1 KiB");

    for deferred in [false, true] {
        let cfg = Config {
            deferred_background: deferred,
            ..base.clone()
        };
        let (mut takeovers, mut firsts) = (vec![], vec![]);
        for r in 0..rounds {
            let t0 = Instant::now();
            let p = Partition::takeover(cfg.clone(), store.clone(), pid).await.unwrap();
            let took = t0.elapsed();
            let mut id = [0u8; 16];
            id[0] = 1;
            id[8..].copy_from_slice(&r.to_be_bytes());
            p.begin(p.epoch, id, Isolation::RepeatableRead, false).unwrap();
            let o = ReadOptions {
                stmt: 1,
                lock: LockMode::None,
                ..Default::default()
            };
            let v = p.get(p.epoch, id, &[key(pid, (r * 7919) % rows)], o).await.unwrap();
            assert!(v[0].is_some());
            firsts.push(t0.elapsed() - took);
            takeovers.push(took);
            p.rollback(p.epoch, id).unwrap();
            p.close().await;
        }
        println!(
            "deferred_background={deferred}: takeover p50 {:?} p90 {:?}; first read p50 {:?}",
            pct(&mut takeovers, 0.5),
            pct(&mut takeovers, 0.9),
            pct(&mut firsts, 0.5)
        );
    }
}
