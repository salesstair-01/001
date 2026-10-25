package kv_test

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/salesstair-01/001/internal/kv"
	"github.com/salesstair-01/001/internal/kvsuite"
	"github.com/salesstair-01/001/internal/objstore"
)

// The shared suites (internal/kvsuite) against the Go owner in process.
//
//	go test ./internal/kv -run Metamorphic -meta-seed 1 -meta-runs 50 -meta-ops 500

var (
	metaSeed = flag.Int64("meta-seed", 1, "first seed of the metamorphic test")
	metaRuns = flag.Int("meta-runs", 8, "seeds the metamorphic test runs")
	metaOps  = flag.Int("meta-ops", 300, "operations per seed")
)

const suitePid = 7

type goInstance struct {
	t   testing.TB
	cfg kv.Config
	p   *kv.Partition
	n   int
}

func (g *goInstance) Pid() uint64          { return suitePid }
func (g *goInstance) Owner() kvsuite.Owner { return g.p }

func (g *goInstance) takeover() {
	g.n++
	cfg := g.cfg
	cfg.OwnerID = fmt.Sprintf("suite-%d", g.n)
	cfg.DataDir = g.t.TempDir()
	p, err := kv.Takeover(context.Background(), cfg, suitePid)
	if err != nil {
		g.t.Fatalf("takeover: %v", err)
	}
	g.p = p
}

func (g *goInstance) Restart(testing.TB) {
	_ = g.p.Close()
	g.takeover()
}

func (g *goInstance) Maintain(_ testing.TB, what string) error {
	if what == "checkpoint" {
		return g.p.Checkpoint(context.Background())
	}
	_, err := kv.GCVersions(g.p)
	return err
}

// goTarget runs each instance on a fresh in-memory store, with set applied
// to the configuration.
func goTarget(set func(*kv.Config)) kvsuite.Target {
	return func(t testing.TB) kvsuite.Instance {
		cfg := kv.Config{
			Store: objstore.NewMem(), GCGrace: time.Hour, CommitRetryTimeout: 2 * time.Second,
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		}
		set(&cfg)
		g := &goInstance{t: t, cfg: cfg}
		g.takeover()
		t.Cleanup(func() { g.p.Close() })
		return g
	}
}

// metaConfigs change how the owner stores data, never what it returns.
var metaConfigs = map[string]kvsuite.Target{
	"default":          goTarget(func(*kv.Config) {}),
	"checkpoint-often": goTarget(func(c *kv.Config) { c.CheckpointBytes = 1; c.CheckpointInterval = 10 * time.Millisecond }),
	"no-cache-small-memtable": goTarget(func(c *kv.Config) {
		c.CacheMaxBytes = -1
		c.MemTableBytes = 64 << 10
		c.BlockCacheBytes = 1 << 20
	}),
	"no-group-window": goTarget(func(c *kv.Config) { c.GroupWindow = time.Microsecond; c.GroupMaxBytes = 64 }),
	"remote-ssts": goTarget(func(c *kv.Config) {
		c.RemoteSSTs = true
		c.CacheMaxBytes = -1
		c.MemTableBytes = 64 << 10
		c.CheckpointBytes = 1
		c.CheckpointInterval = 10 * time.Millisecond
		c.GCGrace = time.Millisecond
	}),
}

func TestMetamorphic(t *testing.T) {
	kvsuite.Metamorphic(t, *metaSeed, *metaRuns, *metaOps, metaConfigs)
}

func TestSemantics(t *testing.T) {
	kvsuite.Semantics(t, goTarget(func(*kv.Config) {}))
}
