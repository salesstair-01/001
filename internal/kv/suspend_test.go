package kv

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/salesstair-01/001/internal/objstore"
)

func TestTrySuspend(t *testing.T) {
	s := objstore.NewMem()
	p := open(t, s, "a")
	put(p, 1, "v")
	if ok, _ := p.TrySuspend(context.Background(), time.Hour); ok {
		t.Fatal("suspended before the idle time")
	}
	id := txnID()
	p.Begin(p.epoch, id, BeginOptions{})
	if ok, _ := p.TrySuspend(context.Background(), 0); ok {
		t.Fatal("suspended with an open transaction")
	}
	p.Rollback(p.epoch, id)
	ok, err := p.TrySuspend(context.Background(), 0)
	if err != nil || !ok {
		t.Fatalf("suspend: %v %v", ok, err)
	}
	if err := p.Begin(p.epoch, txnID(), BeginOptions{}); KindOf(err) != KindEpochChanged {
		t.Fatalf("a suspended partition must refuse new transactions with EpochChanged: %v", err)
	}
	q := open(t, s, "b")
	if q.lastAppliedSeq != q.manifest.ReplayAfterWALSeq+1 || get(t, q, 1) != "v" {
		t.Fatal("wake after suspend")
	}
}

func TestNodeIdleSuspendAndCache(t *testing.T) {
	s := objstore.NewMem()
	cache := filepath.Join(t.TempDir(), "cache")
	suspended := make(chan uint64, 4)
	empty := make(chan struct{}, 4)
	node := NewNode(Config{
		Store: s, DataDir: t.TempDir(), CacheDir: cache, Logger: discardLogger,
		IdleSuspend: 300 * time.Millisecond,
		OnSuspend:   func(pid, _ uint64) { suspended <- pid },
		OnEmpty:     func() { empty <- struct{}{} },
	})
	defer node.Close()
	p, err := node.Open(context.Background(), pid)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		put(p, i, "x")
	}
	select {
	case got := <-suspended:
		if got != pid {
			t.Fatalf("suspended %d", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("idle partition was not suspended")
	}
	select {
	case <-empty:
	case <-time.After(5 * time.Second):
		t.Fatal("OnEmpty not called")
	}
	if len(node.Partitions()) != 0 {
		t.Fatal("suspended partition still listed")
	}
	// Waking on the same host takes the checkpoint from the cache.
	cfg := testConfig(t, s, "b")
	cfg.CacheDir = cache
	q, err := Takeover(context.Background(), cfg, pid)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	if q.CacheHits() == 0 || q.CacheHits() != q.FileCount() {
		t.Fatalf("cache hits %d of %d files", q.CacheHits(), q.FileCount())
	}
	if get(t, q, 7) != "x" {
		t.Fatal("data")
	}
}
