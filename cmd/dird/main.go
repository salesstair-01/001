// Command dird runs the directory service: it maps partitions to their owner
// and epoch, and wakes suspended partitions by starting KV processes.
package main

import (
	"context"
	"flag"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"google.golang.org/grpc"

	"github.com/salesstair-01/001/internal/directory"
	"github.com/salesstair-01/001/internal/kvrpc"
	"github.com/salesstair-01/001/internal/metrics"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:7000", "gRPC listen address")
	dbPath := flag.String("db", "./data/directory.db", "SQLite file holding the partition table")
	kvdBin := flag.String("spawn-kvd", "", "kvd binary to start KV processes on demand (empty = only use registered nodes)")
	kvdArgs := flag.String("spawn-args", "", "extra arguments for spawned kvd processes (MinIO flags, -idle-suspend ...)")
	kvdData := flag.String("spawn-data", "./data/spawned", "base directory for spawned kvd processes (logs, per-process directories)")
	dedicated := flag.Bool("dedicated", true, "with -spawn-kvd, give each woken tenant its own kvd that exits when idle")
	metricsListen := flag.String("metrics-listen", "", "serve Prometheus metrics at http://<addr>/metrics (empty = off)")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := metrics.Serve(*metricsListen); err != nil {
		log.Error("metrics listener", "err", err)
		os.Exit(1)
	}
	slog.SetDefault(log)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := os.MkdirAll(filepath.Dir(*dbPath), 0o755); err != nil {
		log.Error("data dir", "err", err)
		os.Exit(1)
	}
	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Error("listen", "err", err)
		os.Exit(1)
	}
	cfg := directory.Config{DBPath: *dbPath, Dial: kvrpc.Dial, Logger: log, DedicatedNodes: *dedicated}
	if *kvdBin != "" {
		dirAddr := lis.Addr().String()
		cfg.Spawn = func(_ context.Context, id string) (<-chan struct{}, error) {
			dir := filepath.Join(*kvdData, id)
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return nil, err
			}
			args := append(strings.Fields(*kvdArgs),
				"-directory", dirAddr, "-node-id", id, "-listen", "127.0.0.1:0", "-auto-open=true",
				"-data", dir, "-cache-dir", filepath.Join(*kvdData, "cache"))
			if *dedicated {
				args = append(args, "-exit-when-idle")
			}
			logf, err := os.Create(filepath.Join(*kvdData, id+".log"))
			if err != nil {
				return nil, err
			}
			cmd := exec.Command(*kvdBin, args...)
			cmd.Stdout, cmd.Stderr = logf, logf
			if err := cmd.Start(); err != nil {
				logf.Close()
				return nil, err
			}
			log.Info("started kvd", "node", id, "pid", cmd.Process.Pid)
			exited := make(chan struct{})
			go func() {
				err := cmd.Wait()
				logf.Close()
				os.RemoveAll(dir) // its Pebble copy; the shared cache stays
				log.Info("kvd exited", "node", id, "err", err)
				close(exited)
			}()
			return exited, nil
		}
	}
	d, err := directory.Open(cfg)
	if err != nil {
		log.Error("open directory", "err", err)
		os.Exit(1)
	}
	defer d.Close()
	g := grpc.NewServer(kvrpc.ServerOption())
	directory.Register(g, d)
	go func() {
		<-ctx.Done()
		g.GracefulStop()
	}()
	log.Info("dird listening", "addr", lis.Addr().String(), "db", *dbPath, "spawn", *kvdBin)
	if err := g.Serve(lis); err != nil {
		log.Error("serve", "err", err)
	}
}
