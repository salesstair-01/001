package objstore

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"math/rand"
	"strings"
	"sync"
	"time"
)

// ErrInjected is the error returned by injected faults.
var ErrInjected = errors.New("objstore: injected fault")

// Op names an object-store operation.
type Op string

const (
	OpPut    Op = "put"
	OpGet    Op = "get"
	OpList   Op = "list"
	OpDelete Op = "delete"
)

// FaultConfig sets fault probabilities. Each decision is derived from the seed,
// the operation, the key and how many times that operation has hit that key,
// so a failing seed replays exactly even when goroutines interleave differently.
type FaultConfig struct {
	Seed int64
	// FailProb fails an operation before it reaches the store.
	FailProb float64
	// DropResponseProb lets a write land and then returns an error, as if the
	// response was lost (the "lost acknowledgement" case).
	DropResponseProb float64
	// DelayProb delays an operation by up to MaxDelay.
	DelayProb float64
	MaxDelay  time.Duration
	// KeyFilter limits faults to keys containing this substring (empty = all).
	KeyFilter string
}

// Hook runs before (after=false) and after (after=true) each operation. A
// non-nil error from the before-hook fails the call without touching the store.
type Hook func(op Op, key string, after bool) error

// Faulty wraps a Store with seeded fault injection.
type Faulty struct {
	inner Store

	mu    sync.Mutex
	calls map[string]uint64
	cfg   FaultConfig
	hook  Hook
	off   bool
}

func NewFaulty(inner Store, cfg FaultConfig) *Faulty {
	return &Faulty{inner: inner, cfg: cfg, calls: map[string]uint64{}}
}

// SetHook installs a hook (nil removes it).
func (f *Faulty) SetHook(h Hook) {
	f.mu.Lock()
	f.hook = h
	f.mu.Unlock()
}

// SetEnabled turns random faults on or off; hooks always run.
func (f *Faulty) SetEnabled(on bool) {
	f.mu.Lock()
	f.off = !on
	f.mu.Unlock()
}

type decision struct {
	fail, drop bool
	delay      time.Duration
	hook       Hook
}

func (f *Faulty) decide(op Op, key string) decision {
	f.mu.Lock()
	defer f.mu.Unlock()
	d := decision{hook: f.hook}
	if f.off || (f.cfg.KeyFilter != "" && !strings.Contains(key, f.cfg.KeyFilter)) {
		return d
	}
	id := string(op) + " " + key
	n := f.calls[id]
	f.calls[id] = n + 1
	h := fnv.New64a()
	fmt.Fprintf(h, "%d %s %d", f.cfg.Seed, id, n)
	rng := rand.New(rand.NewSource(int64(h.Sum64())))
	if rng.Float64() < f.cfg.DelayProb && f.cfg.MaxDelay > 0 {
		d.delay = time.Duration(rng.Int63n(int64(f.cfg.MaxDelay)))
	}
	d.fail = rng.Float64() < f.cfg.FailProb
	d.drop = rng.Float64() < f.cfg.DropResponseProb
	return d
}

func (f *Faulty) run(ctx context.Context, op Op, key string, write bool, call func() error) error {
	d := f.decide(op, key)
	if d.hook != nil {
		if err := d.hook(op, key, false); err != nil {
			return err
		}
	}
	if d.delay > 0 {
		select {
		case <-time.After(d.delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if d.fail {
		return ErrInjected
	}
	err := call()
	if d.hook != nil {
		_ = d.hook(op, key, true)
	}
	if err == nil && write && d.drop {
		return ErrInjected
	}
	return err
}

func (f *Faulty) PutIfAbsent(ctx context.Context, key string, data []byte) error {
	return f.run(ctx, OpPut, key, true, func() error { return f.inner.PutIfAbsent(ctx, key, data) })
}

func (f *Faulty) Get(ctx context.Context, key string) (b []byte, err error) {
	err = f.run(ctx, OpGet, key, false, func() error {
		var e error
		b, e = f.inner.Get(ctx, key)
		return e
	})
	return b, err
}

func (f *Faulty) List(ctx context.Context, prefix, startAfter string) (keys []string, err error) {
	err = f.run(ctx, OpList, prefix, false, func() error {
		var e error
		keys, e = f.inner.List(ctx, prefix, startAfter)
		return e
	})
	return keys, err
}

func (f *Faulty) Delete(ctx context.Context, key string) error {
	return f.run(ctx, OpDelete, key, true, func() error { return f.inner.Delete(ctx, key) })
}
