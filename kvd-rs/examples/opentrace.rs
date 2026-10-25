//! The object-store requests of a takeover, in order: each takes a fixed
//! delay (an S3 round trip), so the timeline shows which are sequential.
//!
//!     cargo run --release --example opentrace -- [delay_ms]

use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::BoxStream;
use kvd::partition::{Config, Isolation, Mutation, Op, Partition};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions,
    PutOptions, PutPayload, PutResult,
};
use parking_lot::Mutex;

#[derive(Debug)]
struct Traced {
    inner: Arc<dyn ObjectStore>,
    delay: Duration,
    t0: Mutex<Option<Instant>>,
}

impl Traced {
    async fn op(&self, what: &str, path: &str) {
        let t0 = *self.t0.lock();
        if let Some(t0) = t0 {
            println!("{:>7.1} ms  {what:<6} {path}", t0.elapsed().as_secs_f64() * 1e3);
        }
        tokio::time::sleep(self.delay).await;
    }
}

impl fmt::Display for Traced {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Traced({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for Traced {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.op("put", location.as_ref()).await;
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.op("mpu", location.as_ref()).await;
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        let what = if options.head { "head" } else { "get" };
        self.op(what, location.as_ref()).await;
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let t0 = *self.t0.lock();
        if let Some(t0) = t0 {
            println!(
                "{:>7.1} ms  list   {}",
                t0.elapsed().as_secs_f64() * 1e3,
                prefix.map_or("", |p| p.as_ref())
            );
        }
        let delay = self.delay;
        let inner = self.inner.list(prefix);
        futures::stream::once(async move {
            tokio::time::sleep(delay).await;
            inner
        })
        .flatten()
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.op("listd", prefix.map_or("", |p| p.as_ref())).await;
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> object_store::Result<()> {
        self.op("copy", from.as_ref()).await;
        self.inner.copy_opts(from, to, options).await
    }
}

#[tokio::main]
async fn main() {
    let delay_ms: u64 = std::env::args().nth(1).and_then(|a| a.parse().ok()).unwrap_or(5);
    let traced = Arc::new(Traced {
        inner: Arc::new(InMemory::new()),
        delay: Duration::from_millis(delay_ms),
        t0: Mutex::new(None),
    });
    let store: Arc<dyn ObjectStore> = traced.clone();
    let cfg = Config {
        owner_id: "trace".into(),
        ..Config::default()
    };
    // A partition with some history: a few commits, then a clean close.
    let p = Partition::takeover(cfg.clone(), store.clone(), 1).await.unwrap();
    for n in 0..20u64 {
        let mut id = [0u8; 16];
        id[8..].copy_from_slice(&n.to_be_bytes());
        p.begin(p.epoch, id, Isolation::RepeatableRead, false).unwrap();
        let mut k = 1u64.to_be_bytes().to_vec();
        k.extend_from_slice(&n.to_be_bytes());
        let m = Mutation {
            op: Op::Put,
            key: k,
            value: vec![b'x'; 100],
        };
        p.mutate(p.epoch, id, 1, vec![m], Duration::ZERO).await.unwrap();
        p.commit(p.epoch, id, vec![]).await.unwrap();
    }
    p.close().await;

    println!("takeover, {delay_ms} ms per request:");
    *traced.t0.lock() = Some(Instant::now());
    let t = Instant::now();
    let p = Partition::takeover(cfg, store, 1).await.unwrap();
    println!(
        "{:>7.1} ms  serving (epoch {})",
        t.elapsed().as_secs_f64() * 1e3,
        p.epoch
    );
    *traced.t0.lock() = None;
    p.close().await;
}
