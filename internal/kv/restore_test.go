package kv

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/salesstair-01/001/internal/encoding"
	"github.com/salesstair-01/001/internal/objstore"
)

func keyIn(partition uint64, i int) []byte {
	return encoding.AppendInt(encoding.Prefix(partition, encoding.FirstUserTableID, 0), int64(i), false)
}

func TestPointInTimeRestore(t *testing.T) {
	ctx := context.Background()
	s := objstore.NewMem()
	p := open(t, s, "kv-a")
	mustPut := func(i int, v string) {
		t.Helper()
		if _, err := put(p, i, v); err != nil {
			t.Fatal(err)
		}
	}
	mustPut(1, "a")
	if err := p.Checkpoint(ctx); err != nil {
		t.Fatal(err)
	}
	mustPut(2, "b")
	mustPut(1, "a2")
	time.Sleep(5 * time.Millisecond)
	at := time.Now()
	time.Sleep(5 * time.Millisecond)
	mustPut(3, "c")
	mustPut(1, "a3")
	if err := p.Checkpoint(ctx); err != nil {
		t.Fatal(err)
	}
	mustPut(4, "d")

	res, err := Restore(ctx, s, pid, 99, at, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if res.Replayed != 2 {
		t.Fatalf("replayed %d records, want 2", res.Replayed)
	}
	q, err := Takeover(ctx, testConfig(t, s, "kv-b"), 99)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	id := txnID()
	if err := q.Begin(q.Epoch(), id, BeginOptions{}); err != nil {
		t.Fatal(err)
	}
	keys := [][]byte{keyIn(99, 1), keyIn(99, 2), keyIn(99, 3), keyIn(99, 4)}
	vs, err := q.Get(q.Epoch(), id, keys, ReadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	got := []string{string(vs[0]), string(vs[1]), string(vs[2]), string(vs[3])}
	if strings.Join(got, ",") != "a2,b,," {
		t.Fatalf("restored values %q", got)
	}
	// The restored partition takes writes, past the restored commit_ts.
	if err := q.Mutate(q.Epoch(), id, 1, []Mutation{{Op: OpPut, Key: keyIn(99, 5), Value: []byte("e")}}, 0); err != nil {
		t.Fatal(err)
	}
	if ts, err := q.Commit(q.Epoch(), id); err != nil || ts <= res.MaxCommitTS {
		t.Fatalf("commit on restored partition: ts %d (restored %d), %v", ts, res.MaxCommitTS, err)
	}

	// Restoring onto a used partition, or before the first restore point, fails.
	if _, err := Restore(ctx, s, pid, 99, at, t.TempDir()); err == nil {
		t.Fatal("restore over an existing partition")
	}
	if _, err := Restore(ctx, s, pid, 100, at.Add(-time.Hour), t.TempDir()); err == nil {
		t.Fatal("restore before the oldest restore point")
	}
	// A time after everything restores the latest state.
	if res, err := Restore(ctx, s, pid, 101, time.Now(), t.TempDir()); err != nil || res.Replayed != 1 {
		t.Fatalf("restore to now: %+v %v", res, err)
	}
}
