package proxy

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

func TestPoolScaling(t *testing.T) {
	var started atomic.Int32
	spawn := func(context.Context) (*Engine, error) {
		n := started.Add(1)
		return &Engine{Addr: fmt.Sprintf("e%d", n), Stop: func() {}}, nil
	}
	// Concurrent cold acquires share one engine start.
	p := NewPool(PoolConfig{Spawn: spawn, MaxSessions: 4, Logger: quiet})
	defer p.Close()
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := p.Acquire(context.Background()); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if n := started.Load(); n != 1 {
		t.Fatalf("3 concurrent acquires started %d engines", n)
	}
	// A full engine makes the next acquire start another.
	p.Acquire(context.Background())
	e, _ := p.Acquire(context.Background())
	if e.Addr != "e2" || started.Load() != 2 {
		t.Fatalf("5th session went to %s with %d engines", e.Addr, started.Load())
	}

	// With Warm, an engine is started ahead once every engine is nearly full.
	started.Store(0)
	w := NewPool(PoolConfig{Spawn: spawn, MaxSessions: 5, Warm: 1, Logger: quiet})
	defer w.Close()
	waitFor(t, "the warm engine", func() bool { return w.Running() == 1 })
	for i := 0; i < 4; i++ {
		w.Acquire(context.Background())
	}
	waitFor(t, "an engine started ahead", func() bool { return w.Running() == 2 })
}
