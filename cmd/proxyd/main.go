// Command proxyd is the always-on MySQL proxy: it authenticates clients,
// routes each tenant's sessions to generic query engines it starts on demand,
// and keeps client connections open while every compute layer sleeps.
package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/salesstair-01/001/internal/proxy"
	"github.com/salesstair-01/001/internal/sqlengine"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:3306", "MySQL listen address for clients")
	tenantsFile := flag.String("tenants", "./tenants.json", `tenants: [{"name":"acme","partition":42,"users":{"app":"secret"}}]`)
	domain := flag.String("domain", "", "TLS hostnames <tenant>.<domain> select the tenant (bare user names then work)")
	certFile := flag.String("tls-cert", "", "TLS certificate (default: a self-signed one)")
	keyFile := flag.String("tls-key", "", "TLS key")
	requireTLS := flag.Bool("require-tls", false, "refuse clients without TLS")
	engines := flag.String("engines", "", "comma-separated addresses of always-running generic sqld processes")
	sqldBin := flag.String("spawn-sqld", "", "sqld binary to start generic query engines on demand")
	dirAddr := flag.String("directory", "127.0.0.1:7000", "directory address passed to spawned query engines")
	workDir := flag.String("spawn-data", "./data/sqld", "directory for spawned query engines' logs")
	warm := flag.Int("warm", 1, "idle query engines kept ready (0 lets query engines reach zero)")
	engineIdle := flag.Duration("engine-idle", 5*time.Minute, "stop a spawned query engine with no sessions for this long")
	backendIdle := flag.Duration("backend-idle", 30*time.Second, "release a session's query-engine connection after this long idle")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	slog.SetDefault(log)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	tenants, err := proxy.LoadTenants(*tenantsFile, *domain)
	if err != nil {
		log.Error("tenants", "err", err)
		os.Exit(1)
	}
	var tlsCfg *tls.Config
	if *certFile != "" {
		cert, err := tls.LoadX509KeyPair(*certFile, *keyFile)
		if err != nil {
			log.Error("tls", "err", err)
			os.Exit(1)
		}
		tlsCfg = &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	} else {
		hosts := []string{"localhost"}
		if *domain != "" {
			hosts = append(hosts, "*."+*domain)
		}
		if tlsCfg, err = proxy.SelfSignedTLS(hosts...); err != nil {
			log.Error("tls", "err", err)
			os.Exit(1)
		}
	}

	pcfg := proxy.PoolConfig{Warm: *warm, Idle: *engineIdle, Logger: log}
	if *engines != "" {
		pcfg.Static = strings.Split(*engines, ",")
	}
	if *sqldBin != "" {
		os.MkdirAll(*workDir, 0o755)
		n := 0
		pcfg.Spawn = func(ctx context.Context) (*proxy.Engine, error) {
			n++
			addr, err := freeAddr()
			if err != nil {
				return nil, err
			}
			logf, err := os.Create(filepath.Join(*workDir, fmt.Sprintf("sqld-%d-%d.log", os.Getpid(), n)))
			if err != nil {
				return nil, err
			}
			cmd := exec.Command(*sqldBin, "-generic", "-directory", *dirAddr, "-listen", addr, "-create-db", "")
			cmd.Stdout, cmd.Stderr = logf, logf
			if err := cmd.Start(); err != nil {
				logf.Close()
				return nil, err
			}
			exited := make(chan struct{})
			go func() { cmd.Wait(); logf.Close(); close(exited) }()
			if err := proxy.WaitReady(ctx, addr); err != nil {
				cmd.Process.Kill()
				return nil, err
			}
			return &proxy.Engine{Addr: addr, Exited: exited, Stop: func() { cmd.Process.Signal(syscall.SIGTERM) }}, nil
		}
	}
	pool := proxy.NewPool(pcfg)
	defer pool.Close()
	p, err := proxy.New(*listen, proxy.Config{
		Tenants: tenants, Pool: pool, TenantUser: sqlengine.TenantUser,
		BackendIdle: *backendIdle, TLS: tlsCfg, RequireTLS: *requireTLS, Logger: log,
	})
	if err != nil {
		log.Error("listen", "err", err)
		os.Exit(1)
	}
	log.Info("proxyd listening", "addr", p.Addr(), "warm", *warm, "spawn", *sqldBin)
	<-ctx.Done()
	p.Close()
}

func freeAddr() (string, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	defer l.Close()
	return l.Addr().String(), nil
}
