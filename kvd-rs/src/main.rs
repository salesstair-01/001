//! kvd: the KV owner on SlateDB. It serves the same gRPC API as the Go
//! `cmd/kvd` (proto/s2z/kv/kv.proto) and takes the same flags; flags for
//! features not ported yet are accepted and reported.

mod error;
mod keys;
mod locks;
mod node;
mod partition;
mod service;

pub mod pb {
    tonic::include_proto!("s2z.kv");
}

use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use object_store::aws::AmazonS3Builder;

#[derive(Parser, Debug)]
#[command(about = "KV owner on SlateDB")]
struct Args {
    /// gRPC listen address
    #[arg(long, env = "KVD_LISTEN", default_value = "127.0.0.1:7070")]
    listen: String,
    /// local data directory (unused: SlateDB keeps no local state)
    #[arg(long, env = "KVD_DATA", default_value = "./data/kv")]
    data: String,
    /// owner id (default: random)
    #[arg(long, env = "KVD_OWNER", default_value = "")]
    owner: String,
    /// S3 endpoint host:port
    #[arg(long = "minio-endpoint", env = "MINIO_ENDPOINT", default_value = "127.0.0.1:9000")]
    endpoint: String,
    #[arg(long = "minio-access-key", env = "MINIO_ACCESS_KEY", default_value = "minioadmin")]
    access_key: String,
    #[arg(long = "minio-secret-key", env = "MINIO_SECRET_KEY", default_value = "minioadmin")]
    secret_key: String,
    #[arg(long, env = "MINIO_BUCKET", default_value = "s2z")]
    bucket: String,
    #[arg(long, env = "MINIO_REGION", default_value = "us-east-1")]
    region: String,
    /// use TLS to the object store
    #[arg(long = "minio-ssl", num_args = 0..=1, default_missing_value = "true", default_value = "false")]
    ssl: bool,
    /// take this partition over at startup (0 = none)
    #[arg(long, default_value_t = 0)]
    partition: u64,
    /// take a partition over when a query engine opens it
    #[arg(long = "auto-open", num_args = 0..=1, default_missing_value = "true", default_value = "true")]
    auto_open: bool,
    /// group-commit (WAL flush) interval
    #[arg(long = "flush-interval", value_parser = humantime_ms, default_value = "10ms")]
    flush_interval: Duration,

    // Node features of the Go owner, not ported yet (phase 4).
    #[arg(long, env = "KVD_DIRECTORY", default_value = "")]
    directory: String,
    #[arg(long = "node-id", env = "KVD_NODE_ID", default_value = "")]
    node_id: String,
    #[arg(long, default_value = "")]
    advertise: String,
    #[arg(long = "idle-suspend", default_value = "0s")]
    idle_suspend: String,
    #[arg(long = "exit-when-idle", num_args = 0..=1, default_missing_value = "true", default_value = "false")]
    exit_when_idle: bool,
    #[arg(long = "metrics-listen", default_value = "")]
    metrics_listen: String,
    #[arg(long = "cache-dir", env = "KVD_CACHE_DIR", default_value = "")]
    cache_dir: String,
    #[arg(long = "block-cache-mb", default_value_t = 256)]
    block_cache_mb: i64,
    #[arg(long = "memtable-mb", default_value_t = 4)]
    memtable_mb: i64,
    #[arg(long = "put-concurrency", default_value_t = 32)]
    put_concurrency: i64,
    #[arg(long = "put-rate", default_value_t = 100.0)]
    put_rate: f64,
    #[arg(long = "put-burst", default_value_t = 200)]
    put_burst: i64,
}

/// Parses a Go-style duration with an ms or s suffix.
fn humantime_ms(s: &str) -> Result<Duration, String> {
    if let Some(n) = s.strip_suffix("ms") {
        return n.parse().map(Duration::from_millis).map_err(|e| e.to_string());
    }
    if let Some(n) = s.strip_suffix('s') {
        return n.parse::<f64>().map(Duration::from_secs_f64).map_err(|e| e.to_string());
    }
    Err(format!("bad duration {s:?}"))
}

/// Go flags take one dash (`-listen x`); clap wants two.
fn go_style_args() -> Vec<String> {
    std::env::args()
        .enumerate()
        .map(|(i, a)| {
            if i > 0 && a.starts_with('-') && !a.starts_with("--") && a.len() > 2 {
                format!("-{a}")
            } else {
                a
            }
        })
        .collect()
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info,slatedb=warn".into()),
        )
        .with_writer(std::io::stderr)
        .with_ansi(std::io::IsTerminal::is_terminal(&std::io::stderr()))
        .init();
    let args = Args::parse_from(go_style_args());
    if let Err(e) = run(args).await {
        tracing::error!("{e}");
        std::process::exit(1);
    }
}

async fn run(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    if !args.directory.is_empty() || args.exit_when_idle || args.idle_suspend != "0s" || !args.metrics_listen.is_empty()
    {
        tracing::warn!("-directory, -idle-suspend, -exit-when-idle and -metrics-listen are not supported yet; ignored");
    }
    let scheme = if args.ssl { "https" } else { "http" };
    let store = AmazonS3Builder::new()
        .with_endpoint(format!("{scheme}://{}", args.endpoint))
        .with_allow_http(!args.ssl)
        .with_access_key_id(&args.access_key)
        .with_secret_access_key(&args.secret_key)
        .with_bucket_name(&args.bucket)
        .with_region(&args.region)
        .with_virtual_hosted_style_request(false)
        .build()?;
    let mut owner = args.owner.clone();
    if owner.is_empty() {
        owner = format!("kv-{:012x}", rand::random::<u64>() & 0xffff_ffff_ffff);
    }
    let cfg = partition::Config {
        owner_id: owner.clone(),
        flush_interval: args.flush_interval,
        ..Default::default()
    };
    let node = node::Node::new(cfg, Arc::new(store));
    if args.partition != 0 {
        let p = node.open(args.partition).await?;
        tracing::info!(partition = args.partition, epoch = p.epoch, "serving partition");
    }

    let addr: std::net::SocketAddr = tokio::net::lookup_host(&args.listen)
        .await?
        .next()
        .ok_or("bad -listen")?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let local = listener.local_addr()?;
    tracing::info!(addr = %local, owner, bucket = args.bucket, "kvd listening");
    let svc = service::Service {
        node: node.clone(),
        auto_open: args.auto_open,
    };
    let shutdown = async {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
        tracing::info!("shutting down");
    };
    // Without TCP_NODELAY each small response waits ~40 ms for a delayed ACK.
    let incoming = tokio_stream::StreamExt::map(tokio_stream::wrappers::TcpListenerStream::new(listener), |s| {
        s.inspect(|s| {
            let _ = s.set_nodelay(true);
        })
    });
    tonic::transport::Server::builder()
        .add_service(pb::kv_server::KvServer::new(svc))
        .serve_with_incoming_shutdown(incoming, shutdown)
        .await?;
    node.close().await;
    Ok(())
}
