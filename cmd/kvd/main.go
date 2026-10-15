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

	"google.golang.org/grpc"

	"github.com/salesstair-01/001/internal/kv"
	"github.com/salesstair-01/001/internal/kvrpc"
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
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
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
	node := kv.NewNode(kv.Config{Store: store, DataDir: *dataDir, OwnerID: *owner, Logger: log})
	if *partition != 0 {
		p, err := node.Open(ctx, *partition)
		if err != nil {
			log.Error("take over partition", "partition", *partition, "err", err)
			os.Exit(1)
		}
		log.Info("serving partition", "partition", *partition, "epoch", p.Epoch())
	}

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Error("listen", "err", err)
		os.Exit(1)
	}
	g := grpc.NewServer(kvrpc.ServerOption())
	kvrpc.Register(g, &kvrpc.Server{Node: node, AutoOpen: *autoOpen})
	go func() {
		<-ctx.Done()
		log.Info("shutting down")
		g.GracefulStop()
	}()
	log.Info("kvd listening", "addr", lis.Addr().String(), "owner", node.OwnerID(), "bucket", *bucket)
	if err := g.Serve(lis); err != nil {
		log.Error("serve", "err", err)
	}
	_ = node.Close()
}
