//! kvd: the KV owner on SlateDB. It serves the same gRPC API as the Go
//! `cmd/kvd` (proto/s2z/kv/kv.proto) and takes the same flags, so `dird`
//! can spawn it; flags for features it doesn't have are accepted and
//! reported.

use kvd::{agent, localstore, metrics, node, partition, pb, restore, service};
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use object_store::aws::AmazonS3Builder;
use slatedb::db_cache::foyer::{FoyerCache, FoyerCacheOptions};
use tokio::sync::Notify;

/// The object store (S3 API; Ceph RGW in development).
#[derive(clap::Args, Debug)]
struct StoreArgs {
    /// keep the data in this local directory instead of an S3 store
    /// (development and tests; processes on one host may share it)
    #[arg(long = "store-dir", env = "KVD_STORE_DIR", default_value = "")]
    store_dir: String,
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
}

impl StoreArgs {
    fn open(&self) -> Result<Arc<dyn object_store::ObjectStore>, Box<dyn std::error::Error>> {
        if !self.store_dir.is_empty() {
            return Ok(Arc::new(localstore::LocalStore::new(&self.store_dir)?));
        }
        let scheme = if self.ssl { "https" } else { "http" };
        let store = AmazonS3Builder::new()
            .with_endpoint(format!("{scheme}://{}", self.endpoint))
            .with_allow_http(!self.ssl)
            .with_access_key_id(&self.access_key)
            .with_secret_access_key(&self.secret_key)
            .with_bucket_name(&self.bucket)
            .with_region(&self.region)
            .with_virtual_hosted_style_request(false)
            .build()?;
        Ok(Arc::new(store))
    }
}

/// kvd restore: write partition dst as partition src was at a time.
#[derive(Parser, Debug)]
#[command(name = "kvd restore")]
struct RestoreArgs {
    #[command(flatten)]
    store: StoreArgs,
    /// partition to restore from
    #[arg(long)]
    src: u64,
    /// partition to write (must never have been used)
    #[arg(long)]
    dst: u64,
    /// time to restore to, RFC 3339 (e.g. 2026-10-16T09:30:00Z)
    #[arg(long)]
    at: String,
}

/// kvd restore-points: the times a partition can be restored to.
#[derive(Parser, Debug)]
#[command(name = "kvd restore-points")]
struct PointsArgs {
    #[command(flatten)]
    store: StoreArgs,
    #[arg(long)]
    partition: u64,
}

#[derive(Parser, Debug)]
#[command(about = "KV owner on SlateDB; also `kvd restore` and `kvd restore-points`")]
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
    #[command(flatten)]
    store: StoreArgs,
    /// take this partition over at startup (0 = none)
    #[arg(long, default_value_t = 0)]
    partition: u64,
    /// take a partition over when a query engine opens it
    #[arg(long = "auto-open", num_args = 0..=1, default_missing_value = "true", default_value = "true")]
    auto_open: bool,
    /// group-commit (WAL flush) interval
    #[arg(long = "flush-interval", value_parser = go_duration, default_value = "10ms")]
    flush_interval: Duration,

    /// directory service address (empty = run standalone)
    #[arg(long, env = "KVD_DIRECTORY", default_value = "")]
    directory: String,
    /// node id in the directory (default: the owner id)
    #[arg(long = "node-id", env = "KVD_NODE_ID", default_value = "")]
    node_id: String,
    /// address query engines reach this node at (default: the listen address)
    #[arg(long, default_value = "")]
    advertise: String,
    /// suspend a partition idle this long (0 = never)
    #[arg(long = "idle-suspend", value_parser = go_duration, default_value = "0s")]
    idle_suspend: Duration,
    /// exit once no partition is served (a dedicated per-tenant process)
    #[arg(long = "exit-when-idle", num_args = 0..=1, default_missing_value = "true", default_value = "false")]
    exit_when_idle: bool,
    /// serve Prometheus metrics at http://<addr>/metrics (empty = off)
    #[arg(long = "metrics-listen", default_value = "")]
    metrics_listen: String,
    /// block cache shared by all partitions, MiB
    #[arg(long = "block-cache-mb", default_value_t = 256)]
    block_cache_mb: u64,
    /// memtable size per partition before it is flushed to an SST, MiB
    #[arg(long = "memtable-mb", default_value_t = 4)]
    memtable_mb: usize,
    /// retries of a failing object-store operation (about 1 s apart) before
    /// a commit fails and the partition stops serving (0 = retry forever)
    #[arg(long = "store-retries", default_value_t = 60)]
    store_retries: u32,
    /// abort a transaction without a heartbeat for this long
    #[arg(long = "txn-lease", value_parser = go_duration, default_value = "10s")]
    txn_lease: Duration,
    /// how far back a point-in-time restore can go (MVCC history kept)
    #[arg(long = "restore-window", value_parser = go_duration, default_value = "1h")]
    restore_window: Duration,
    /// unflushed data a partition may hold in memory before writes wait, MiB
    #[arg(long = "max-unflushed-mb", default_value_t = 64)]
    max_unflushed_mb: usize,

    // Flags of the Go owner without a meaning here: SlateDB keeps no local
    // file cache and makes its own PUTs.
    #[arg(long = "cache-dir", env = "KVD_CACHE_DIR", default_value = "")]
    cache_dir: String,
    #[arg(long = "put-concurrency", default_value_t = 32)]
    put_concurrency: i64,
    #[arg(long = "put-rate", default_value_t = 100.0)]
    put_rate: f64,
    #[arg(long = "put-burst", default_value_t = 200)]
    put_burst: i64,
}

/// Parses a Go duration ("10ms", "5m", "1m30s", "0").
fn go_duration(s: &str) -> Result<Duration, String> {
    if s == "0" {
        return Ok(Duration::ZERO);
    }
    let mut total = Duration::ZERO;
    let mut rest = s;
    while !rest.is_empty() {
        let n = rest
            .find(|c: char| !(c.is_ascii_digit() || c == '.'))
            .ok_or(format!("bad duration {s:?}"))?;
        let v: f64 = rest[..n].parse().map_err(|_| format!("bad duration {s:?}"))?;
        rest = &rest[n..];
        let (unit, len) = [("ms", 0.001), ("h", 3600.0), ("m", 60.0), ("s", 1.0)]
            .iter()
            .find(|(u, _)| rest.starts_with(u))
            .map(|(u, f)| (*f, u.len()))
            .ok_or(format!("bad duration {s:?}"))?;
        total += Duration::from_secs_f64(v * unit);
        rest = &rest[len..];
    }
    Ok(total)
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
    let argv = go_style_args();
    let res = match argv.get(1).map(String::as_str) {
        Some("restore") => run_restore(RestoreArgs::parse_from(argv.iter().skip(1))).await,
        Some("restore-points") => run_points(PointsArgs::parse_from(argv.iter().skip(1))).await,
        _ => run(Args::parse_from(argv)).await,
    };
    if let Err(e) = res {
        tracing::error!("{e}");
        std::process::exit(1);
    }
}

async fn run_restore(a: RestoreArgs) -> Result<(), Box<dyn std::error::Error>> {
    let at = humantime::parse_rfc3339_weak(&a.at)?;
    let t0 = std::time::Instant::now();
    let r = restore::restore(a.store.open()?, a.src, a.dst, at).await?;
    println!(
        "restored partition {} at {} into partition {} in {:?}\n  {} rows, as of commit {} ({})",
        a.src,
        a.at,
        a.dst,
        t0.elapsed(),
        r.rows,
        r.commit_ts,
        humantime::format_rfc3339_micros(r.commit_time)
    );
    Ok(())
}

async fn run_points(a: PointsArgs) -> Result<(), Box<dyn std::error::Error>> {
    match restore::restore_points(a.store.open()?, a.partition).await? {
        None => println!("no restore points"),
        Some((from, to)) => println!(
            "partition {} can be restored to any time from {} on (last commit {})",
            a.partition,
            humantime::format_rfc3339_micros(from),
            humantime::format_rfc3339_micros(to)
        ),
    }
    Ok(())
}

async fn run(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    let store = args.store.open()?;
    if args.store.store_dir.is_empty() {
        // Fail now, not on the first takeover, when the bucket is missing.
        if let Err(e) = store.list_with_delimiter(None).await {
            return Err(format!(
                "bucket {:?} on {}: {e} (create it with: go run ./cmd/s3bucket)",
                args.store.bucket, args.store.endpoint
            )
            .into());
        }
    }
    let mut owner = args.owner.clone();
    if owner.is_empty() {
        owner = format!("kv-{:012x}", rand::random::<u64>() & 0xffff_ffff_ffff);
    }
    let cache = FoyerCache::new_with_opts(FoyerCacheOptions {
        max_capacity: args.block_cache_mb << 20,
        ..Default::default()
    });
    let cfg = partition::Config {
        owner_id: owner.clone(),
        cache: Some(Arc::new(cache)),
        flush_interval: args.flush_interval,
        l0_sst_bytes: args.memtable_mb << 20,
        max_unflushed_bytes: args.max_unflushed_mb << 20,
        restore_window: args.restore_window,
        txn_lease: args.txn_lease,
        store_retries: (args.store_retries > 0).then_some(args.store_retries),
        ..Default::default()
    };
    let (events_tx, events) = tokio::sync::mpsc::unbounded_channel();
    let with_dir = !args.directory.is_empty();
    let node = node::Node::new(cfg, store, with_dir.then_some(events_tx));
    if !args.idle_suspend.is_zero() {
        node.spawn_idle_loop(args.idle_suspend);
    }
    if !args.metrics_listen.is_empty() {
        metrics::serve(&args.metrics_listen, node.clone()).await?;
    }
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
    let advertise = if args.advertise.is_empty() {
        local.to_string()
    } else {
        args.advertise.clone()
    };
    let exit = Arc::new(Notify::new());
    if with_dir {
        let a = agent::Agent {
            node_id: if args.node_id.is_empty() {
                owner.clone()
            } else {
                args.node_id.clone()
            },
            addr: advertise.clone(),
            exit_when_idle: args.exit_when_idle,
            idle_exit: if args.exit_when_idle {
                args.idle_suspend + Duration::from_secs(30)
            } else {
                Duration::ZERO
            },
            exit: exit.clone(),
        };
        tokio::spawn(a.run(args.directory.clone(), node.clone(), events));
    }
    tracing::info!(addr = %advertise, owner, bucket = args.store.bucket, directory = args.directory, "kvd listening");
    let svc = service::Service {
        node: node.clone(),
        auto_open: args.auto_open,
    };
    let shutdown = async move {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
            _ = exit.notified() => {}
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn go_durations() {
        let ms = Duration::from_millis;
        assert_eq!(go_duration("0"), Ok(Duration::ZERO));
        assert_eq!(go_duration("0s"), Ok(Duration::ZERO));
        assert_eq!(go_duration("10ms"), Ok(ms(10)));
        assert_eq!(go_duration("3s"), Ok(ms(3000)));
        assert_eq!(go_duration("5m0s"), Ok(ms(300_000)));
        assert_eq!(go_duration("1h1m1.5s"), Ok(ms(3_661_500)));
        assert!(go_duration("5x").is_err());
        assert!(go_duration("s").is_err());
    }
}
