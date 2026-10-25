//! Prometheus metrics, with the Go owner's names (internal/kv/metrics.go):
//! `s2z_kv_commit_seconds`, `s2z_kv_takeover_seconds`,
//! `s2z_kv_puts_total{partition}` and the `s2z_kv_partitions` gauge, served
//! as text at /metrics.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions,
    PutOptions, PutPayload, PutResult,
};
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::node::Node;

/// From a millisecond to a minute, in seconds (metrics.LatencyBuckets in Go).
const BUCKETS: [f64; 15] = [
    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0,
];

pub struct Histogram {
    name: &'static str,
    help: &'static str,
    labels: &'static str,               // fixed label pairs, e.g. cache="none"
    state: Mutex<(Vec<u64>, u64, f64)>, // per-bucket counts, count, sum
}

impl Histogram {
    const fn new(name: &'static str, help: &'static str, labels: &'static str) -> Self {
        Histogram {
            name,
            help,
            labels,
            state: Mutex::new((Vec::new(), 0, 0.0)),
        }
    }

    pub fn observe(&self, d: Duration) {
        let v = d.as_secs_f64();
        let mut st = self.state.lock();
        if st.0.is_empty() {
            st.0 = vec![0; BUCKETS.len()];
        }
        if let Some(i) = BUCKETS.iter().position(|ub| v <= *ub) {
            st.0[i] += 1;
        }
        st.1 += 1;
        st.2 += v;
    }

    fn write(&self, out: &mut String) {
        let st = self.state.lock();
        let sep = if self.labels.is_empty() { "" } else { "," };
        let _ = writeln!(
            out,
            "# HELP {} {}\n# TYPE {} histogram",
            self.name, self.help, self.name
        );
        let mut cum = 0;
        for (i, ub) in BUCKETS.iter().enumerate() {
            cum += st.0.get(i).copied().unwrap_or(0);
            let _ = writeln!(out, "{}_bucket{{{}{sep}le=\"{ub}\"}} {cum}", self.name, self.labels);
        }
        let _ = writeln!(out, "{}_bucket{{{}{sep}le=\"+Inf\"}} {}", self.name, self.labels, st.1);
        let braces = if self.labels.is_empty() {
            String::new()
        } else {
            format!("{{{}}}", self.labels)
        };
        let _ = writeln!(out, "{}_sum{braces} {}", self.name, st.2);
        let _ = writeln!(out, "{}_count{braces} {}", self.name, st.1);
    }
}

pub static COMMIT: LazyLock<Histogram> = LazyLock::new(|| {
    Histogram::new(
        "s2z_kv_commit_seconds",
        "Commit latency at the owner, from the commit request to its durable WAL write.",
        "",
    )
});

// SlateDB keeps no local file cache, so every takeover is labelled cache="none".
pub static TAKEOVER: LazyLock<Histogram> =
    LazyLock::new(|| Histogram::new("s2z_kv_takeover_seconds", "Partition takeover time.", "cache=\"none\""));

/// Object-store PUTs by partition: SlateDB makes them (WAL objects, SSTs,
/// manifests), so they are counted at the store, by the partition's prefix
/// `p/<id>/`. A multipart upload counts once.
static PUTS: LazyLock<Mutex<BTreeMap<u64, u64>>> = LazyLock::new(Default::default);

/// The PUTs counted so far for partition pid.
pub fn puts(pid: u64) -> u64 {
    PUTS.lock().get(&pid).copied().unwrap_or(0)
}

fn count_put(location: &Path) {
    let mut parts = location.parts();
    if parts.next().is_some_and(|p| p.as_ref() == "p")
        && let Some(pid) = parts.next().and_then(|p| p.as_ref().parse::<u64>().ok())
    {
        *PUTS.lock().entry(pid).or_default() += 1;
    }
}

/// Wraps store so that its PUTs count in s2z_kv_puts_total.
pub fn count_puts(store: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
    Arc::new(CountingStore(store))
}

#[derive(Debug)]
struct CountingStore(Arc<dyn ObjectStore>);

impl std::fmt::Display for CountingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CountingStore({})", self.0)
    }
}

#[async_trait]
impl ObjectStore for CountingStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        count_put(location);
        self.0.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        count_put(location);
        self.0.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        self.0.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.0.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.0.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.0.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> object_store::Result<()> {
        count_put(to);
        self.0.copy_opts(from, to, options).await
    }
}

/// The metrics as Prometheus text; node adds its partition gauge.
pub fn render(node: Option<&Node>) -> String {
    let mut out = String::new();
    COMMIT.write(&mut out);
    TAKEOVER.write(&mut out);
    let _ = writeln!(
        out,
        "# HELP s2z_kv_puts_total Object-store PUTs by partition (WAL objects, SSTs, manifests).\n# TYPE s2z_kv_puts_total counter"
    );
    for (pid, n) in PUTS.lock().iter() {
        let _ = writeln!(out, "s2z_kv_puts_total{{partition=\"{pid}\"}} {n}");
    }
    let Some(node) = node else {
        return out;
    };
    let _ = writeln!(
        out,
        "# HELP s2z_kv_partitions Partitions this node serves.\n# TYPE s2z_kv_partitions gauge\ns2z_kv_partitions {}",
        node.partitions().len()
    );
    out
}

/// Serves http://addr/metrics until the process exits.
pub async fn serve(addr: &str, node: Arc<Node>) -> std::io::Result<()> {
    let l = TcpListener::bind(addr).await?;
    tracing::info!(addr = %l.local_addr()?, "metrics listening");
    tokio::spawn(async move {
        loop {
            let Ok((mut conn, _)) = l.accept().await else { continue };
            let node = node.clone();
            tokio::spawn(async move {
                let mut buf = vec![0u8; 4096];
                let n = conn.read(&mut buf).await.unwrap_or(0);
                let req = String::from_utf8_lossy(&buf[..n]);
                let (status, body) = if req.starts_with("GET /metrics") {
                    ("200 OK", render(Some(&node)))
                } else {
                    ("404 Not Found", String::from("not found\n"))
                };
                let resp = format!(
                    "HTTP/1.1 {status}\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = conn.write_all(resp.as_bytes()).await;
            });
        }
    });
    Ok(())
}
