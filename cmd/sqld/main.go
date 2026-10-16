// Command sqld runs a query engine: a MySQL-protocol server whose tables live
// in a KV owner reached over gRPC.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/salesstair-01/001/internal/directory"
	"github.com/salesstair-01/001/internal/kvrpc"
	"github.com/salesstair-01/001/internal/metrics"
	"github.com/salesstair-01/001/internal/sqlengine"
)

func env(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

func main() {
	listen := flag.String("listen", env("SQLD_LISTEN", "127.0.0.1:3306"), "MySQL protocol listen address")
	kvAddr := flag.String("kv", env("SQLD_KV", "127.0.0.1:7070"), "KV owner gRPC address")
	partition := flag.Uint64("partition", 1, "tenant partition id")
	dirAddr := flag.String("directory", env("SQLD_DIRECTORY", ""), "directory service address; routes to the partition's owner and wakes it (overrides -kv)")
	generic := flag.Bool("generic", false, "serve any tenant: the connecting user s2z_p<partition> picks it (used behind proxyd; needs -directory)")
	createDB := flag.String("create-db", "app", "create this database at startup if it does not exist (empty = none)")
	metricsListen := flag.String("metrics-listen", "", "serve Prometheus metrics at http://<addr>/metrics (empty = off)")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := metrics.Serve(*metricsListen); err != nil {
		log.Error("metrics listener", "err", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cc, err := grpc.NewClient(*kvAddr, append(kvrpc.DialOptions(), grpc.WithTransportCredentials(insecure.NewCredentials()))...)
	if err != nil {
		log.Error("dial kv", "err", err)
		os.Exit(1)
	}
	store := sqlengine.NewStore(kvrpc.NewClient(cc), *partition)
	if *dirAddr != "" {
		dcc, err := grpc.NewClient(*dirAddr, append(kvrpc.DialOptions(), grpc.WithTransportCredentials(insecure.NewCredentials()))...)
		if err != nil {
			log.Error("dial directory", "err", err)
			os.Exit(1)
		}
		store = sqlengine.NewRoutedStore(&directory.Router{Dir: directory.NewClient(dcc), Dial: kvrpc.Dial}, *partition)
	}
	var engine *sqlengine.Engine
	if *generic {
		if *dirAddr == "" {
			log.Error("-generic needs -directory")
			os.Exit(1)
		}
		dcc, _ := grpc.NewClient(*dirAddr, append(kvrpc.DialOptions(), grpc.WithTransportCredentials(insecure.NewCredentials()))...)
		engine = sqlengine.NewGenericEngine(&directory.Router{Dir: directory.NewClient(dcc), Dial: kvrpc.Dial}, *createDB)
	} else {
		engine = sqlengine.NewEngine(store)
	}
	if *createDB != "" && !*generic {
		if err := engine.EnsureDatabase(ctx, *createDB); err != nil {
			log.Error("create database", "db", *createDB, "err", err)
			os.Exit(1)
		}
	}
	srv, err := engine.NewServer(*listen)
	if err != nil {
		log.Error("server", "err", err)
		os.Exit(1)
	}
	go func() {
		<-ctx.Done()
		_ = srv.Close()
	}()
	log.Info("sqld listening", "addr", *listen, "kv", *kvAddr, "partition", *partition, "generic", *generic)
	if err := srv.Start(); err != nil && ctx.Err() == nil {
		log.Error("serve", "err", err)
		os.Exit(1)
	}
}
