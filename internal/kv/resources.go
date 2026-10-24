package kv

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/salesstair-01/001/internal/objstore"
)

// Many partitions share one node (slice 9). Each one's Pebble gets a small
// memtable and one compaction at a time, all share one block cache, and
// object-store PUTs go through a node-wide limit on concurrent PUTs plus a
// per-partition token bucket. A tenant over its PUT rate waits for tokens;
// its commits meanwhile pile into larger group commits, so a hot tenant pays
// in latency while other tenants' PUTs keep their slots.

// nodeResources are shared by the partitions of a node.
type nodeResources struct {
	cache    *blockCache // nil = a private cache per engine
	putSlots chan struct{}
}

func newNodeResources(c *Config) *nodeResources {
	r := &nodeResources{putSlots: make(chan struct{}, c.PutConcurrency)}
	if c.BlockCacheBytes > 0 {
		r.cache = newBlockCache(c.BlockCacheBytes)
	}
	return r
}

func (r *nodeResources) close() {
	if r.cache != nil {
		r.cache.unref()
	}
}

func (p *Partition) engineOptions() engineOptions {
	return engineOptions{cache: p.cfg.res.cache, memTableBytes: p.cfg.MemTableBytes}
}

// fairStore is a partition's view of the object store: PUTs take a node-wide
// slot and a token from the partition's bucket, and are counted.
type fairStore struct {
	objstore.Store
	slots  chan struct{}
	bucket *tokenBucket // nil = no rate limit
	puts   *atomic.Int64
	label  string // partition id, for metrics
}

func (f *fairStore) PutIfAbsent(ctx context.Context, key string, data []byte) error {
	if f.bucket != nil {
		if err := f.bucket.wait(ctx); err != nil {
			return err
		}
	}
	select {
	case f.slots <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-f.slots }()
	f.puts.Add(1)
	putsTotal.Inc(f.label)
	return f.Store.PutIfAbsent(ctx, key, data)
}

// tokenBucket allows rate events per second with bursts of up to burst.
type tokenBucket struct {
	mu     sync.Mutex
	rate   float64
	burst  float64
	tokens float64
	last   time.Time
}

func newTokenBucket(rate float64, burst int) *tokenBucket {
	if burst < 1 {
		burst = 1
	}
	return &tokenBucket{rate: rate, burst: float64(burst), tokens: float64(burst), last: time.Now()}
}

func (b *tokenBucket) wait(ctx context.Context) error {
	for {
		b.mu.Lock()
		now := time.Now()
		b.tokens += now.Sub(b.last).Seconds() * b.rate
		if b.tokens > b.burst {
			b.tokens = b.burst
		}
		b.last = now
		if b.tokens >= 1 {
			b.tokens--
			b.mu.Unlock()
			return nil
		}
		d := time.Duration((1 - b.tokens) / b.rate * float64(time.Second))
		b.mu.Unlock()
		select {
		case <-time.After(d):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Puts returns how many object-store PUTs this partition has made.
func (p *Partition) Puts() int64 { return p.puts.Load() }
