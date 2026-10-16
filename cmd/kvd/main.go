// Command kvd runs a KV owner: it serves partitions over gRPC, with MinIO as
// the only durable state.
package main

import (
	"context"
	"flag"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/salesstair-01/001/internal/directory"
	"github.com/salesstair-01/001/internal/kv"
	"github.com/salesstair-01/001/internal/kvrpc"
	"github.com/salesstair-01/001/internal/metrics"
	"github.com/salesstair-01/001/internal/objstore"
)

func env(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

func main() {
	listen := flag.String("listen", env("KVD_LISTEN", "127.0.0.1:7070"), "gRPC listen address")
	dataDir := flag.String("data", env("KVD_DATA", "./data/kv"), "local directory for Pebble instances")
	owner := flag.String("owner", env("KVD_OWNER", ""), "owner id recorded in manifests (default: random)")
	endpoint := flag.String("minio-endpoint", env("MINIO_ENDPOINT", "127.0.0.1:9000"), "MinIO host:port")
	access := flag.String("minio-access-key", env("MINIO_ACCESS_KEY", "minioadmin"), "MinIO access key")
	secret := flag.String("minio-secret-key", env("MINIO_SECRET_KEY", "minioadmin"), "MinIO secret key")
	bucket := flag.String("bucket", env("MINIO_BUCKET", "s2z"), "MinIO bucket")
	useSSL := flag.Bool("minio-ssl", false, "use TLS to MinIO")
	partition := flag.Uint64("partition", 0, "take this partition over at startup (0 = none)")
	autoOpen := flag.Bool("auto-open", true, "take a partition over when a query engine opens it")
	dirAddr := flag.String("directory", env("KVD_DIRECTORY", ""), "directory service address (empty = run standalone)")
	nodeID := flag.String("node-id", env("KVD_NODE_ID", ""), "node id in the directory (default: the owner id)")
	advertise := flag.String("advertise", "", "address query engines reach this node at (default: the listen address)")
	idleSuspend := flag.Duration("idle-suspend", 0, "suspend a partition idle this long (0 = never; 5m is the v1 default with a directory)")
	cacheDir := flag.String("cache-dir", env("KVD_CACHE_DIR", ""), "local SST cache kept across restarts (default: <data>/cache)")
	exitWhenIdle := flag.Bool("exit-when-idle", false, "exit once no partition is served (a dedicated per-tenant process)")
	blockCache := flag.Int64("block-cache-mb", 256, "Pebble block cache shared by all partitions, MiB")
	memtable := flag.Int64("memtable-mb", 4, "memtable size per partition, MiB")
	putConc := flag.Int("put-concurrency", 32, "concurrent object-store PUTs for the node")
	putRate := flag.Float64("put-rate", 100, "object-store PUTs per second per partition (<0 = unlimited)")
	putBurst := flag.Int("put-burst", 200, "PUT burst per partition")
	metricsListen := flag.String("metrics-listen", "", "serve Prometheus metrics at http://<addr>/metrics (empty = off)")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if err := metrics.Serve(*metricsListen); err != nil {
		log.Error("metrics listener", "err", err)
		os.Exit(1)
	}
	slog.SetDefault(log)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store, err := objstore.NewMinIO(ctx, objstore.MinIOConfig{
		Endpoint: *endpoint, AccessKey: *access, SecretKey: *secret, Bucket: *bucket, UseSSL: *useSSL,
	})
	if err != nil {
		log.Error("connect to MinIO", "err", err)
		os.Exit(1)
	}
	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Error("listen", "err", err)
		os.Exit(1)
	}
	addr := lis.Addr().String()
	if *advertise != "" {
		addr = *advertise
	}

	cfg := kv.Config{Store: store, DataDir: *dataDir, CacheDir: *cacheDir, OwnerID: *owner, Logger: log, IdleSuspend: *idleSuspend,
		BlockCacheBytes: *blockCache << 20, MemTableBytes: *memtable << 20,
		PutConcurrency: *putConc, PutRate: *putRate, PutBurst: *putBurst}
	var agent *directory.Agent
	if *dirAddr != "" {
		cc, err := grpc.NewClient(*dirAddr, append(kvrpc.DialOptions(), grpc.WithTransportCredentials(insecure.NewCredentials()))...)
		if err != nil {
			log.Error("dial directory", "err", err)
			os.Exit(1)
		}
		agent = &directory.Agent{Dir: directory.NewClient(cc), Addr: addr, ExitWhenIdle: *exitWhenIdle, Exit: stop, Logger: log}
		cfg.OnSuspend, cfg.OnEmpty = agent.OnSuspend, agent.OnEmpty
	}
	node := kv.NewNode(cfg)
	node.RegisterMetrics(metrics.Default)
	if agent != nil {
		agent.NodeID = *nodeID
		if agent.NodeID == "" {
			agent.NodeID = node.OwnerID()
		}
		agent.Attach(node)
		idleExit := time.Duration(0)
		if *exitWhenIdle {
			idleExit = *idleSuspend + 30*time.Second
		}
		go agent.Run(ctx, idleExit)
	}
	if *partition != 0 {
		p, err := node.Open(ctx, *partition)
		if err != nil {
			log.Error("take over partition", "partition", *partition, "err", err)
			os.Exit(1)
		}
		log.Info("serving partition", "partition", *partition, "epoch", p.Epoch())
	}

	g := grpc.NewServer(kvrpc.ServerOption())
	kvrpc.Register(g, &kvrpc.Server{Node: node, AutoOpen: *autoOpen})
	go func() {
		<-ctx.Done()
		log.Info("shutting down")
		g.GracefulStop()
	}()
	log.Info("kvd listening", "addr", addr, "owner", node.OwnerID(), "bucket", *bucket, "directory", *dirAddr)
	if err := g.Serve(lis); err != nil {
		log.Error("serve", "err", err)
	}
	_ = node.Close()
}
