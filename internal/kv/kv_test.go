package kv

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/salesstair-01/001/internal/encoding"
	"github.com/salesstair-01/001/internal/objstore"
)

const pid = 7

func testConfig(t *testing.T, s objstore.Store, owner string) Config {
	return Config{
		Store: s, DataDir: t.TempDir(), OwnerID: owner,
		GCGrace: time.Hour, CommitRetryTimeout: 2 * time.Second, Logger: discardLogger,
	}
}

func open(t *testing.T, s objstore.Store, owner string) *Partition {
	t.Helper()
	p, err := Takeover(context.Background(), testConfig(t, s, owner), pid)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close() })
	return p
}

func txnID() TxnID {
	var id TxnID
	_, _ = rand.Read(id[:])
	return id
}

func key(i int) []byte {
	return encoding.AppendInt(encoding.Prefix(pid, encoding.FirstUserTableID, 0), int64(i), false)
}

// put commits key i = v in its own transaction.
func put(p *Partition, i int, v string) (uint64, error) {
	id := txnID()
	if err := p.Begin(p.epoch, id, BeginOptions{}); err != nil {
		return 0, err
	}
	if err := p.Mutate(p.epoch, id, 1, []Mutation{{Op: OpPut, Key: key(i), Value: []byte(v)}}, 0); err != nil {
		return 0, err
	}
	return p.Commit(p.epoch, id)
}

func get(t *testing.T, p *Partition, i int) string {
	t.Helper()
	id := txnID()
	if err := p.Begin(p.epoch, id, BeginOptions{}); err != nil {
		t.Fatal(err)
	}
	defer p.Rollback(p.epoch, id)
	vs, err := p.Get(p.epoch, id, [][]byte{key(i)}, ReadOptions{Stmt: 1})
	if err != nil {
		t.Fatal(err)
	}
	return string(vs[0])
}

func TestCommitAndRestart(t *testing.T) {
	s := objstore.NewMem()
	p := open(t, s, "a")
	for i := 0; i < 50; i++ {
		if _, err := put(p, i, fmt.Sprint("v", i)); err != nil {
			t.Fatal(err)
		}
	}
	if got := get(t, p, 3); got != "v3" {
		t.Fatalf("got %q", got)
	}
	// Simulate kill -9: drop the process without a checkpoint; a new owner
	// recovers everything from the WAL.
	p.Close()
	q := open(t, s, "b")
	for i := 0; i < 50; i++ {
		if got := get(t, q, i); got != fmt.Sprint("v", i) {
			t.Fatalf("key %d: got %q", i, got)
		}
	}
	if q.epoch != p.epoch+1 {
		t.Fatalf("epoch %d -> %d", p.epoch, q.epoch)
	}
}

func TestCheckpointAndReplay(t *testing.T) {
	s := objstore.NewMem()
	p := open(t, s, "a")
	for i := 0; i < 20; i++ {
		put(p, i, "before")
	}
	if err := p.Checkpoint(context.Background()); err != nil {
		t.Fatal(err)
	}
	for i := 10; i < 30; i++ {
		put(p, i, "after")
	}
	p.Close()
	q := open(t, s, "b")
	if q.manifest.ReplayAfterWALSeq == 0 {
		t.Fatal("manifest should record a checkpoint")
	}
	for i := 0; i < 30; i++ {
		want := "before"
		if i >= 10 {
			want = "after"
		}
		if got := get(t, q, i); got != want {
			t.Fatalf("key %d: got %q want %q", i, got, want)
		}
	}
	// Suspend: checkpoint then release; the next wake replays nothing but the fence.
	if err := q.Suspend(context.Background()); err != nil {
		t.Fatal(err)
	}
	r := open(t, s, "c")
	if r.lastAppliedSeq != r.manifest.ReplayAfterWALSeq+1 {
		t.Fatalf("expected only the fence after the checkpoint: applied %d, replay_after %d", r.lastAppliedSeq, r.manifest.ReplayAfterWALSeq)
	}
	if get(t, r, 25) != "after" {
		t.Fatal("lost data across suspend")
	}
}

func TestFencing(t *testing.T) {
	s := objstore.NewMem()
	a := open(t, s, "a")
	if _, err := put(a, 1, "a1"); err != nil {
		t.Fatal(err)
	}
	b := open(t, s, "b") // takes over while a is alive
	if _, err := put(a, 2, "a2"); KindOf(err) != KindEpochChanged {
		t.Fatalf("old owner must be fenced, got %v", err)
	}
	if a.Stopped() == nil {
		t.Fatal("old owner should stop serving")
	}
	if get(t, b, 1) != "a1" || get(t, b, 2) != "" {
		t.Fatal("new owner state")
	}
	if _, err := put(b, 3, "b3"); err != nil {
		t.Fatal(err)
	}
	// A checkpoint by the fenced owner must fail too.
	if err := a.Checkpoint(context.Background()); err == nil {
		t.Fatal("fenced checkpoint succeeded")
	}
}

func TestLostAcknowledgement(t *testing.T) {
	mem := objstore.NewMem()
	p := open(t, mem, "a")
	// Drop the response of the next WAL PUT after it lands; the retry gets a
	// 412 and the GET shows the object is ours.
	dropper := objstore.NewFaulty(mem, objstore.FaultConfig{DropResponseProb: 1})
	p.store = storeSwitch{first: dropper, rest: mem, once: &sync.Once{}}
	ts, err := put(p, 1, "x")
	if err != nil {
		t.Fatalf("commit should succeed via compare-on-412: %v", err)
	}
	if ts == 0 || get(t, p, 1) != "x" {
		t.Fatal("value")
	}
}

// storeSwitch routes the first PutIfAbsent to first and everything else to rest.
type storeSwitch struct {
	first, rest objstore.Store
	once        *sync.Once
}

func (s storeSwitch) PutIfAbsent(ctx context.Context, k string, d []byte) error {
	var err error
	used := false
	s.once.Do(func() { used = true; err = s.first.PutIfAbsent(ctx, k, d) })
	if used {
		return err
	}
	return s.rest.PutIfAbsent(ctx, k, d)
}
func (s storeSwitch) Get(ctx context.Context, k string) ([]byte, error) { return s.rest.Get(ctx, k) }
func (s storeSwitch) List(ctx context.Context, p, a string) ([]string, error) {
	return s.rest.List(ctx, p, a)
}
func (s storeSwitch) Delete(ctx context.Context, k string) error { return s.rest.Delete(ctx, k) }

func TestCrashBetweenSSTUploadAndManifest(t *testing.T) {
	mem := objstore.NewMem()
	f := objstore.NewFaulty(mem, objstore.FaultConfig{})
	p := open(t, f, "a")
	for i := 0; i < 10; i++ {
		put(p, i, "v")
	}
	f.SetHook(func(op objstore.Op, k string, after bool) error {
		if op == objstore.OpPut && strings.Contains(k, "/manifest/") && !after {
			return errors.New("crash before manifest")
		}
		return nil
	})
	if err := p.Checkpoint(context.Background()); err == nil {
		t.Fatal("expected checkpoint failure")
	}
	p.Close()
	f.SetHook(nil)
	q := open(t, f, "b")
	for i := 0; i < 10; i++ {
		if get(t, q, i) != "v" {
			t.Fatalf("key %d lost", i)
		}
	}
}

func TestTakeoverDuringGroupFlush(t *testing.T) {
	mem := objstore.NewMem()
	f := objstore.NewFaulty(mem, objstore.FaultConfig{})
	a := open(t, f, "a")
	put(a, 1, "first")
	// Hold a's next WAL PUT until b has fenced.
	release := make(chan struct{})
	entered := make(chan struct{}, 1)
	f.SetHook(func(op objstore.Op, k string, after bool) error {
		if op == objstore.OpPut && strings.Contains(k, "/wal/") && !after && a.Stopped() == nil {
			select {
			case entered <- struct{}{}:
				<-release
			default:
			}
		}
		return nil
	})
	errc := make(chan error, 1)
	go func() { _, err := put(a, 2, "second"); errc <- err }()
	<-entered
	f.SetHook(nil)
	b := open(t, mem, "b")
	close(release)
	err := <-errc
	if KindOf(err) != KindEpochChanged {
		t.Fatalf("in-flight commit on fenced owner: %v", err)
	}
	if get(t, b, 2) != "" || get(t, b, 1) != "first" {
		t.Fatal("state after takeover")
	}
}

func TestSnapshotIsolation(t *testing.T) {
	p := open(t, objstore.NewMem(), "a")
	put(p, 1, "old")
	a := txnID()
	p.Begin(p.epoch, a, BeginOptions{})
	if vs, _ := p.Get(p.epoch, a, [][]byte{key(1)}, ReadOptions{Stmt: 1}); string(vs[0]) != "old" {
		t.Fatal("first read")
	}
	put(p, 1, "new")
	if vs, _ := p.Get(p.epoch, a, [][]byte{key(1)}, ReadOptions{Stmt: 2}); string(vs[0]) != "old" {
		t.Fatal("repeatable read must keep the old value")
	}
	// READ COMMITTED sees the new value on its next statement.
	rc := txnID()
	p.Begin(p.epoch, rc, BeginOptions{Isolation: ReadCommitted})
	p.Get(p.epoch, rc, [][]byte{key(1)}, ReadOptions{Stmt: 1})
	put(p, 1, "newer")
	if vs, _ := p.Get(p.epoch, rc, [][]byte{key(1)}, ReadOptions{Stmt: 2}); string(vs[0]) != "newer" {
		t.Fatal("read committed must see the latest commit")
	}
	// A locking read sees the latest committed version, not the snapshot.
	if vs, _ := p.Get(p.epoch, a, [][]byte{key(1)}, ReadOptions{Stmt: 3, Lock: LockExclusive}); string(vs[0]) != "newer" {
		t.Fatalf("locking read must read the latest version, got %q", vs[0])
	}
	p.Rollback(p.epoch, a)
}

func TestRollbackAndStatementRollback(t *testing.T) {
	p := open(t, objstore.NewMem(), "a")
	put(p, 1, "a")
	id := txnID()
	p.Begin(p.epoch, id, BeginOptions{})
	p.Mutate(p.epoch, id, 1, []Mutation{{Op: OpPut, Key: key(2), Value: []byte("b")}}, 0)
	p.Mutate(p.epoch, id, 2, []Mutation{{Op: OpPut, Key: key(3), Value: []byte("c")}}, 0)
	err := p.Mutate(p.epoch, id, 2, []Mutation{{Op: OpInsertOnly, Key: key(1), Value: []byte("dup")}}, 0)
	if KindOf(err) != KindDuplicateKey {
		t.Fatalf("expected duplicate, got %v", err)
	}
	p.RollbackStatement(p.epoch, id, 2)
	rows, _, err := p.Scan(p.epoch, id, key(0), key(10), 0, ReadOptions{Stmt: 3})
	if err != nil || len(rows) != 2 {
		t.Fatalf("scan inside txn: %d rows %v", len(rows), err)
	}
	if _, err := p.Commit(p.epoch, id); err != nil {
		t.Fatal(err)
	}
	if get(t, p, 2) != "b" || get(t, p, 3) != "" {
		t.Fatal("statement rollback")
	}
	id2 := txnID()
	p.Begin(p.epoch, id2, BeginOptions{})
	p.Mutate(p.epoch, id2, 1, []Mutation{{Op: OpDelete, Key: key(1)}}, 0)
	p.Rollback(p.epoch, id2)
	if get(t, p, 1) != "a" {
		t.Fatal("rollback left a trace")
	}
}

func TestScanMergesIntents(t *testing.T) {
	p := open(t, objstore.NewMem(), "a")
	for i := 0; i < 10; i += 2 {
		put(p, i, fmt.Sprint(i))
	}
	id := txnID()
	p.Begin(p.epoch, id, BeginOptions{})
	p.Mutate(p.epoch, id, 1, []Mutation{
		{Op: OpPut, Key: key(3), Value: []byte("3")},
		{Op: OpDelete, Key: key(4)},
		{Op: OpPut, Key: key(6), Value: []byte("six")},
	}, 0)
	var got []string
	start := key(0)
	for {
		rows, resume, err := p.Scan(p.epoch, id, start, key(100), 2, ReadOptions{Stmt: 2})
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range rows {
			got = append(got, string(r.Value))
		}
		if resume == nil {
			break
		}
		start = resume
	}
	if fmt.Sprint(got) != "[0 2 3 six 8]" {
		t.Fatalf("got %v", got)
	}
}

func TestTxnStatusAndIdempotentCommit(t *testing.T) {
	s := objstore.NewMem()
	p := open(t, s, "a")
	id := txnID()
	p.Begin(p.epoch, id, BeginOptions{})
	p.Mutate(p.epoch, id, 1, []Mutation{{Op: OpPut, Key: key(1), Value: []byte("x")}}, 0)
	ts, err := p.Commit(p.epoch, id)
	if err != nil {
		t.Fatal(err)
	}
	if ts2, err := p.Commit(p.epoch, id); err != nil || ts2 != ts {
		t.Fatal("retried commit must return the original commit_ts")
	}
	p.Checkpoint(context.Background())
	p.Close()
	q := open(t, s, "b")
	st, ts3, _ := q.TxnStatus(q.epoch, id)
	if st != TxnCommitted || ts3 != ts {
		t.Fatalf("status after takeover: %v %d", st, ts3)
	}
	if st, _, _ := q.TxnStatus(q.epoch, txnID()); st != TxnUnknown {
		t.Fatal("unknown id")
	}
}

func TestMVCCGC(t *testing.T) {
	p := open(t, objstore.NewMem(), "a")
	for v := 0; v < 5; v++ {
		put(p, 1, fmt.Sprint(v))
	}
	put(p, 2, "gone")
	id := txnID()
	p.Begin(p.epoch, id, BeginOptions{})
	p.Mutate(p.epoch, id, 1, []Mutation{{Op: OpDelete, Key: key(2)}}, 0)
	p.Commit(p.epoch, id)
	n, err := p.gcVersions()
	if err != nil {
		t.Fatal(err)
	}
	if n != 4+2 {
		t.Fatalf("dropped %d versions, want 6", n)
	}
	if get(t, p, 1) != "4" || get(t, p, 2) != "" {
		t.Fatal("gc changed visible data")
	}
}

func TestEpochMismatch(t *testing.T) {
	p := open(t, objstore.NewMem(), "a")
	if err := p.Begin(p.epoch+1, txnID(), BeginOptions{}); KindOf(err) != KindEpochChanged {
		t.Fatal("epoch mismatch must return EpochChanged")
	}
}

func TestGroupCommitConcurrent(t *testing.T) {
	mem := objstore.NewMem()
	f := objstore.NewFaulty(mem, objstore.FaultConfig{DelayProb: 1, MaxDelay: 5 * time.Millisecond, KeyFilter: "/wal/", Seed: 1})
	p := open(t, f, "a")
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := put(p, i, "c"); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	segs, _ := mem.List(context.Background(), walPrefix(pid), "")
	if len(segs) >= 200 {
		t.Fatalf("group commit should batch: %d segments for 200 commits", len(segs))
	}
	p.Close()
	q := open(t, mem, "b")
	for i := 0; i < 200; i++ {
		if get(t, q, i) != "c" {
			t.Fatalf("key %d lost", i)
		}
	}
}

var discardLogger = slog.New(slog.NewTextHandler(io.Discard, nil))

func TestAutoIncrementSurvivesRestart(t *testing.T) {
	s := objstore.NewMem()
	p := open(t, s, "a")
	id := txnID()
	p.Begin(p.epoch, id, BeginOptions{})
	first, _ := p.AutoIncrement(p.epoch, id, 2000, 3, 0)
	if first != 1 {
		t.Fatalf("first value %d", first)
	}
	// A rolled-back transaction's values are not handed out again.
	rb := txnID()
	p.Begin(p.epoch, rb, BeginOptions{})
	if v, _ := p.AutoIncrement(p.epoch, rb, 2000, 1, 0); v != 4 {
		t.Fatalf("second allocation %d", v)
	}
	p.Rollback(p.epoch, rb)
	if v, _ := p.AutoIncrement(p.epoch, id, 2000, 0, 100); v != 100 {
		t.Fatalf("explicit value raises the counter: %d", v)
	}
	p.Mutate(p.epoch, id, 1, []Mutation{{Op: OpPut, Key: key(1), Value: []byte("x")}}, 0)
	if _, err := p.Commit(p.epoch, id); err != nil {
		t.Fatal(err)
	}
	p.Close()
	q := open(t, s, "b")
	if v, _ := q.AutoIncrement(q.epoch, TxnID{}, 2000, 0, 0); v != 100 {
		t.Fatalf("counter after restart: %d", v)
	}
}

// A snapshot taken before anything was committed has timestamp 0, which once
// also meant "no snapshot yet": the next read took a new one, so reads on a
// fresh partition were not repeatable (found by the metamorphic test).
func TestSnapshotOnEmptyPartitionIsRepeatable(t *testing.T) {
	for _, iso := range []Isolation{RepeatableRead, ReadCommitted} {
		p := open(t, objstore.NewMem(), "kv-a")
		reader := txnID()
		if err := p.Begin(p.epoch, reader, BeginOptions{Isolation: iso}); err != nil {
			t.Fatal(err)
		}
		k := key(int(iso) + 100)
		if vs, err := p.Get(p.epoch, reader, [][]byte{k}, ReadOptions{Stmt: 1}); err != nil || vs[0] != nil {
			t.Fatalf("first read: %q %v", vs, err)
		}
		if _, err := put(p, int(iso)+100, "later"); err != nil {
			t.Fatal(err)
		}
		// Same statement: neither RR nor RC may see the later commit.
		if vs, err := p.Get(p.epoch, reader, [][]byte{k}, ReadOptions{Stmt: 1}); err != nil || vs[0] != nil {
			t.Fatalf("isolation %d: second read in the statement saw %q (%v)", iso, vs[0], err)
		}
		_ = p.Rollback(p.epoch, reader)
	}
}
