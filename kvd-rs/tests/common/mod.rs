//! Helpers shared by the owner tests: partitions on an in-memory store, a
//! fault-injecting store, and the key and transaction helpers of the Go
//! owner's tests these replace.
#![allow(dead_code)]

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use futures::stream::BoxStream;
use kvd::locks::TxnId;
use kvd::partition::{Config, Mutation, Op, Partition, ReadOptions};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions,
    PutOptions, PutPayload, PutResult,
};
use parking_lot::Mutex;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

pub const PID: u64 = 7;

pub fn mem() -> Arc<dyn ObjectStore> {
    Arc::new(InMemory::new())
}

/// The configuration of a test partition: short intervals, no shared cache.
pub fn cfg() -> Config {
    Config {
        owner_id: "test".into(),
        ..Config::default()
    }
}

pub async fn open(store: &Arc<dyn ObjectStore>) -> Arc<Partition> {
    open_with(store, cfg()).await
}

pub async fn open_with(store: &Arc<dyn ObjectStore>, cfg: Config) -> Arc<Partition> {
    Partition::takeover(cfg, store.clone(), PID).await.expect("takeover")
}

static NEXT: AtomicU64 = AtomicU64::new(1);

/// A fresh transaction id.
pub fn txn_id() -> TxnId {
    let mut id = [0u8; 16];
    id[..8].copy_from_slice(&std::process::id().to_be_bytes().repeat(2));
    id[8..].copy_from_slice(&NEXT.fetch_add(1, Ordering::Relaxed).to_be_bytes());
    id
}

/// User key i of table 1024 in the test partition (an INT primary key).
pub fn key(i: i64) -> Vec<u8> {
    let mut k = PID.to_be_bytes().to_vec();
    k.extend_from_slice(&1024u32.to_be_bytes());
    k.extend_from_slice(&0u32.to_be_bytes());
    k.push(0x01);
    k.extend_from_slice(&((i as u64) ^ (1 << 63)).to_be_bytes());
    k
}

pub fn ro(stmt: u64) -> ReadOptions {
    ReadOptions {
        stmt,
        ..Default::default()
    }
}

pub fn put_mut(i: i64, v: &str) -> Mutation {
    Mutation {
        op: Op::Put,
        key: key(i),
        value: v.as_bytes().to_vec(),
    }
}

pub async fn begin(p: &Partition) -> TxnId {
    let id = txn_id();
    p.begin(p.epoch, id, kvd::partition::Isolation::RepeatableRead, false)
        .expect("begin");
    id
}

/// Commits key i = v in its own transaction.
pub async fn put(p: &Partition, i: i64, v: &str) -> kvd::error::Result<u64> {
    let id = txn_id();
    p.begin(p.epoch, id, kvd::partition::Isolation::RepeatableRead, false)?;
    p.mutate(p.epoch, id, 1, vec![put_mut(i, v)], Duration::ZERO).await?;
    p.commit(p.epoch, id, vec![]).await
}

/// Writes key i = v in statement stmt of transaction id.
pub async fn write(p: &Partition, id: TxnId, stmt: u64, i: i64, v: &str, wait: Duration) -> kvd::error::Result<()> {
    p.mutate(p.epoch, id, stmt, vec![put_mut(i, v)], wait).await
}

/// Reads key i in its own transaction ("" if absent).
pub async fn get(p: &Partition, i: i64) -> String {
    let id = begin(p).await;
    let vs = p.get(p.epoch, id, &[key(i)], ro(1)).await.expect("get");
    p.rollback(p.epoch, id).unwrap();
    String::from_utf8(vs[0].clone().unwrap_or_default()).unwrap()
}

// ---- a store that fails ------------------------------------------------------

/// Faults injected into writes: fail before the write lands, or let it
/// land and lose the response.
#[derive(Clone, Copy, Debug, Default)]
pub struct Faults {
    pub fail: f64,
    pub drop: f64,
}

#[derive(Debug)]
pub struct FaultyStore {
    inner: Arc<dyn ObjectStore>,
    rng: Mutex<StdRng>,
    faults: Mutex<Faults>,
    pub injected: AtomicU64,
}

impl FaultyStore {
    pub fn new(inner: Arc<dyn ObjectStore>, seed: u64) -> Arc<FaultyStore> {
        Arc::new(FaultyStore {
            inner,
            rng: Mutex::new(StdRng::seed_from_u64(seed)),
            faults: Mutex::new(Faults::default()),
            injected: AtomicU64::new(0),
        })
    }

    pub fn set(&self, f: Faults) {
        *self.faults.lock() = f;
    }

    fn roll(&self) -> (bool, bool) {
        let f = *self.faults.lock();
        let mut r = self.rng.lock();
        (
            r.random_bool(f.fail.clamp(0.0, 1.0)),
            r.random_bool(f.drop.clamp(0.0, 1.0)),
        )
    }

    fn injected_err(&self, path: &Path) -> object_store::Error {
        self.injected.fetch_add(1, Ordering::Relaxed);
        object_store::Error::Generic {
            store: "FaultyStore",
            source: format!("injected fault on {path}").into(),
        }
    }
}

impl fmt::Display for FaultyStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FaultyStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for FaultyStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let (fail, drop) = self.roll();
        if fail {
            return Err(self.injected_err(location));
        }
        let res = self.inner.put_opts(location, payload, opts).await;
        if drop && res.is_ok() {
            return Err(self.injected_err(location));
        }
        res
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

pub fn rng(seed: u64) -> StdRng {
    StdRng::seed_from_u64(seed)
}

pub fn gen_range(r: &mut StdRng, n: usize) -> usize {
    r.random_range(0..n)
}
