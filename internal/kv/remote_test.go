package kv

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/salesstair-01/001/internal/objstore"
)

// countingStore counts operations on keys containing a substring.
type countingStore struct {
	objstore.Store
	sub string
	mu  sync.Mutex
	ops map[string]int
}

func newCounting(s objstore.Store, sub string) *countingStore {
	return &countingStore{Store: s, sub: sub, ops: map[string]int{}}
}

func (c *countingStore) count(op, key string) {
	if strings.Contains(key, c.sub) {
		c.mu.Lock()
		c.ops[op]++
		c.mu.Unlock()
	}
}

func (c *countingStore) get(op string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ops[op]
}

func (c *countingStore) PutIfAbsent(ctx context.Context, k string, d []byte) error {
	c.count("put", k)
	return c.Store.PutIfAbsent(ctx, k, d)
}
func (c *countingStore) Get(ctx context.Context, k string) ([]byte, error) {
	c.count("get", k)
	return c.Store.Get(ctx, k)
}
func (c *countingStore) GetRange(ctx context.Context, k string, off, n int64) ([]byte, error) {
	c.count("range", k)
	return c.Store.GetRange(ctx, k, off, n)
}
func (c *countingStore) Size(ctx context.Context, k string) (int64, error) {
	c.count("size", k)
	return c.Store.Size(ctx, k)
}

func remoteTestConfig(t *testing.T, s objstore.Store, owner string) Config {
	cfg := testConfig(t, s, owner)
	cfg.RemoteSSTs = true
	cfg.CacheMaxBytes = -1
	cfg.MemTableBytes = 64 << 10
	cfg.GroupWindow = time.Microsecond // serial commits; the window only adds latency
	cfg.PutRate = -1                   // one WAL PUT per commit; the quota would pace them
	return cfg
}

func val(tag string, i int) string { return fmt.Sprintf("%s-%d-%s", tag, i, strings.Repeat("x", 200)) }

func fill(t *testing.T, p *Partition, from, to int, tag string) {
	t.Helper()
	for i := from; i < to; i++ {
		if _, err := put(p, i, val(tag, i)); err != nil {
			t.Fatal(err)
		}
	}
}

func checkpoint(t *testing.T, p *Partition) {
	t.Helper()
	if err := p.Checkpoint(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// remoteKeys returns the partition's remote objects in the store.
func remoteKeys(t *testing.T, s objstore.Store) []string {
	t.Helper()
	keys, err := s.List(context.Background(), remotePrefix(pid), "")
	if err != nil {
		t.Fatal(err)
	}
	return keys
}

// Every SST lives on the object store, under the owner's epoch; a wake reads
// them in place (ranged GETs) and downloads only Pebble's metadata files.
func TestRemoteSSTsWakeWithoutDownload(t *testing.T) {
	ctx := context.Background()
	mem := objstore.NewMem()
	p, err := Takeover(ctx, remoteTestConfig(t, mem, "a"), pid)
	if err != nil {
		t.Fatal(err)
	}
	fill(t, p, 0, 2000, "v")
	checkpoint(t, p)
	m := p.manifest
	for _, f := range m.Files {
		if strings.HasSuffix(f.Name, ".sst") {
			t.Fatalf("local SST %s in a remote manifest", f.Name)
		}
	}
	if len(m.Remote) == 0 {
		t.Fatal("manifest lists no remote SSTs")
	}
	for _, r := range m.Remote {
		if !strings.HasPrefix(r.Object, remotePrefix(pid)+fmt.Sprintf("e%d/", p.epoch)) {
			t.Fatalf("remote SST %s is not under epoch %d", r.Object, p.epoch)
		}
	}
	p.Close()

	cs := newCounting(mem, "/rsst/")
	p, err = Takeover(ctx, remoteTestConfig(t, cs, "b"), pid)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if n := cs.get("get"); n != 0 {
		t.Fatalf("wake downloaded %d remote SSTs", n)
	}
	for _, i := range []int{0, 999, 1999} {
		if got := get(t, p, i); got != val("v", i) {
			t.Fatalf("key %d = %q", i, got)
		}
	}
	if cs.get("range") == 0 {
		t.Fatal("reads made no ranged GETs")
	}
	if n := cs.get("get") + cs.get("size"); n != 0 {
		t.Fatalf("reads made %d whole-object GETs or HEADs", n)
	}
}

// Restarts, compactions and checkpoints keep every value, and GC leaves
// only objects that a manifest lists or that still wait out their grace.
func TestRemoteSSTsRestartsAndGC(t *testing.T) {
	ctx := context.Background()
	s := objstore.NewMem()
	want := map[int]string{}
	for round := 0; round < 4; round++ {
		cfg := remoteTestConfig(t, s, "a")
		cfg.GCGrace = time.Nanosecond
		p, err := Takeover(ctx, cfg, pid)
		if err != nil {
			t.Fatalf("round %d: %v", round, err)
		}
		for i, v := range want {
			if got := get(t, p, i); got != v {
				t.Fatalf("round %d: key %d = %.20q, want %.20q", round, i, got, v)
			}
		}
		for c := 0; c < 4; c++ {
			tag := fmt.Sprint(round, ".", c)
			fill(t, p, round*100, round*100+300, tag)
			for i := round * 100; i < round*100+300; i++ {
				want[i] = val(tag, i)
			}
			checkpoint(t, p)
		}
		// Objects an earlier owner dropped after its last checkpoint are only
		// found by the sweep (its in-memory list of them died with it).
		if err := p.sweepRemote(ctx); err != nil {
			t.Fatal(err)
		}
		p.collectGarbage(ctx)

		listed := map[string]bool{}
		for _, r := range p.manifest.Remote {
			listed[r.Object] = true
		}
		pending := map[string]bool{}
		for _, g := range p.garbage {
			pending[g.key] = true
		}
		for _, k := range remoteKeys(t, s) {
			if !listed[k] && !pending[k] && !strings.Contains(k, fmt.Sprintf("/e%d/", p.epoch)) {
				t.Fatalf("round %d: %s is neither listed nor pending", round, k)
			}
		}
		p.Close()
	}
}

// A fenced owner that keeps flushing writes only under its own epoch, so it
// can never overwrite or delete the new owner's SSTs.
func TestRemoteSSTsFencedOwner(t *testing.T) {
	ctx := context.Background()
	s := objstore.NewMem()
	a, err := Takeover(ctx, remoteTestConfig(t, s, "a"), pid)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	fill(t, a, 0, 500, "a")
	checkpoint(t, a)

	b, err := Takeover(ctx, remoteTestConfig(t, s, "b"), pid)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	fill(t, b, 0, 500, "b")
	checkpoint(t, b)

	// The old owner does not know it is fenced yet and its Pebble flushes
	// again (written to the engine directly: its commits would fail).
	b2 := a.db.NewBatch()
	for i := 0; i < 2000; i++ {
		_ = b2.Set(append(key(i), make([]byte, 8)...), []byte(strings.Repeat("z", 300)))
	}
	if err := b2.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := a.db.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := a.Checkpoint(ctx); err == nil {
		t.Fatal("fenced owner wrote a manifest")
	}
	ea, eb := fmt.Sprintf("/e%d/", a.epoch), fmt.Sprintf("/e%d/", b.epoch)
	var fromA, fromB int
	for _, k := range remoteKeys(t, s) {
		switch {
		case strings.Contains(k, ea):
			fromA++
		case strings.Contains(k, eb):
			fromB++
		}
	}
	if fromA == 0 || fromB == 0 {
		t.Fatalf("objects: epoch %d: %d, epoch %d: %d", a.epoch, fromA, b.epoch, fromB)
	}
	for i := 0; i < 500; i += 50 {
		if got := get(t, b, i); got != val("b", i) {
			t.Fatalf("key %d = %.20q", i, got)
		}
	}
}

// Objects of earlier epochs that no manifest lists (a crashed owner's
// compaction outputs) are swept after takeover; listed ones stay.
func TestRemoteSSTsSweep(t *testing.T) {
	ctx := context.Background()
	s := objstore.NewMem()
	p, err := Takeover(ctx, remoteTestConfig(t, s, "a"), pid)
	if err != nil {
		t.Fatal(err)
	}
	fill(t, p, 0, 500, "v")
	checkpoint(t, p)
	leak := remotePrefix(pid) + fmt.Sprintf("e%d/", p.epoch) + "0000-8-999999.sst"
	if err := s.PutIfAbsent(ctx, leak, []byte("partial")); err != nil {
		t.Fatal(err)
	}
	p.Close()

	cfg := remoteTestConfig(t, s, "b")
	cfg.GCGrace = time.Nanosecond
	p, err = Takeover(ctx, cfg, pid)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.sweepRemote(ctx); err != nil {
		t.Fatal(err)
	}
	p.collectGarbage(ctx)
	for _, k := range remoteKeys(t, s) {
		if k == leak {
			t.Fatal("leaked object survived the sweep")
		}
	}
	for _, r := range p.manifest.Remote {
		if _, err := s.Size(ctx, r.Object); err != nil {
			t.Fatalf("listed object %s: %v", r.Object, err)
		}
	}
	if got := get(t, p, 250); got != val("v", 250) {
		t.Fatalf("key 250 = %.20q", got)
	}
}

// Point-in-time restore reads the source's remote SSTs in place.
func TestRemoteSSTsRestore(t *testing.T) {
	ctx := context.Background()
	s := objstore.NewMem()
	p, err := Takeover(ctx, remoteTestConfig(t, s, "a"), pid)
	if err != nil {
		t.Fatal(err)
	}
	fill(t, p, 0, 1000, "v")
	checkpoint(t, p)
	time.Sleep(2 * time.Millisecond)
	at := time.Now()
	time.Sleep(2 * time.Millisecond)
	fill(t, p, 0, 10, "late")
	p.Close()

	if _, err := Restore(ctx, s, pid, pid+1, at, t.TempDir()); err != nil {
		t.Fatal(err)
	}
	r, err := Takeover(ctx, testConfig(t, s, "r"), pid+1)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	id := txnID()
	if err := r.Begin(r.epoch, id, BeginOptions{}); err != nil {
		t.Fatal(err)
	}
	defer r.Rollback(r.epoch, id)
	for _, i := range []int{0, 5, 999} {
		vs, err := r.Get(r.epoch, id, [][]byte{keyIn(pid+1, i)}, ReadOptions{Stmt: 1})
		if err != nil {
			t.Fatal(err)
		}
		if string(vs[0]) != val("v", i) {
			t.Fatalf("restored key %d = %.20q", i, vs[0])
		}
	}
}
