package kvsuite

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/salesstair-01/001/internal/encoding"
	"github.com/salesstair-01/001/internal/kv"
)

// Semantics runs the transaction and lock tests against target, each on a
// fresh instance.
func Semantics(t *testing.T, target Target) {
	tests := []struct {
		name string
		f    func(*testing.T, Instance)
	}{
		{"CommitAndRestart", testCommitAndRestart},
		{"SnapshotIsolation", testSnapshotIsolation},
		{"RollbackAndStatementRollback", testRollbackAndStatementRollback},
		{"ScanMergesIntents", testScanMergesIntents},
		{"TxnStatusAndIdempotentCommit", testTxnStatusAndIdempotentCommit},
		{"EpochMismatch", testEpochMismatch},
		{"GroupCommitConcurrent", testGroupCommitConcurrent},
		{"AutoIncrementSurvivesRestart", testAutoIncrementSurvivesRestart},
		{"SnapshotOnEmptyPartitionIsRepeatable", testSnapshotOnEmptyPartitionIsRepeatable},
		{"CommitGuards", testCommitGuards},
		{"WriteWaitsForLock", testWriteWaitsForLock},
		{"LockWaitTimeout", testLockWaitTimeout},
		{"DeadlockYoungestIsVictim", testDeadlockYoungestIsVictim},
		{"DeadlockOlderWaiterVictimized", testDeadlockOlderWaiterVictimized},
		{"SharedLocks", testSharedLocks},
		{"LockingScanReadsLatest", testLockingScanReadsLatest},
		{"MoneyTransfers", testMoneyTransfers},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.f(t, target(t))
		})
	}
}

// ---- helpers --------------------------------------------------------------------

func key(in Instance, i int) []byte {
	return encoding.AppendInt(encoding.Prefix(in.Pid(), encoding.FirstUserTableID, 0), int64(i), false)
}

// put commits key i = v in its own transaction.
func put(in Instance, i int, v string) (uint64, error) {
	p := in.Owner()
	id := txnID()
	if err := p.Begin(p.Epoch(), id, kv.BeginOptions{}); err != nil {
		return 0, err
	}
	if err := p.Mutate(p.Epoch(), id, 1, []kv.Mutation{{Op: kv.OpPut, Key: key(in, i), Value: []byte(v)}}, 0); err != nil {
		return 0, err
	}
	return p.CommitGuarded(p.Epoch(), id, nil)
}

func mustPut(t *testing.T, in Instance, i int, v string) {
	t.Helper()
	if _, err := put(in, i, v); err != nil {
		t.Fatal(err)
	}
}

func get(t *testing.T, in Instance, i int) string {
	t.Helper()
	p := in.Owner()
	id := txnID()
	if err := p.Begin(p.Epoch(), id, kv.BeginOptions{}); err != nil {
		t.Fatal(err)
	}
	defer p.Rollback(p.Epoch(), id)
	vs, err := p.Get(p.Epoch(), id, [][]byte{key(in, i)}, kv.ReadOptions{Stmt: 1})
	if err != nil {
		t.Fatal(err)
	}
	return string(vs[0])
}

func begin(t *testing.T, in Instance) kv.TxnID {
	t.Helper()
	p := in.Owner()
	id := txnID()
	if err := p.Begin(p.Epoch(), id, kv.BeginOptions{}); err != nil {
		t.Fatal(err)
	}
	return id
}

func write(in Instance, id kv.TxnID, stmt uint64, k int, v string, wait time.Duration) error {
	p := in.Owner()
	return p.Mutate(p.Epoch(), id, stmt, []kv.Mutation{{Op: kv.OpPut, Key: key(in, k), Value: []byte(v)}}, wait)
}

func commit(in Instance, id kv.TxnID) (uint64, error) {
	p := in.Owner()
	return p.CommitGuarded(p.Epoch(), id, nil)
}

// ---- transactions -----------------------------------------------------------------

func testCommitAndRestart(t *testing.T, in Instance) {
	for i := 0; i < 50; i++ {
		mustPut(t, in, i, fmt.Sprint("v", i))
	}
	if got := get(t, in, 3); got != "v3" {
		t.Fatalf("got %q", got)
	}
	epoch := in.Owner().Epoch()
	in.Restart(t)
	for i := 0; i < 50; i++ {
		if got := get(t, in, i); got != fmt.Sprint("v", i) {
			t.Fatalf("key %d: got %q", i, got)
		}
	}
	if in.Owner().Epoch() <= epoch {
		t.Fatalf("epoch %d -> %d", epoch, in.Owner().Epoch())
	}
}

func testSnapshotIsolation(t *testing.T, in Instance) {
	p := in.Owner()
	mustPut(t, in, 1, "old")
	a := begin(t, in)
	if vs, _ := p.Get(p.Epoch(), a, [][]byte{key(in, 1)}, kv.ReadOptions{Stmt: 1}); string(vs[0]) != "old" {
		t.Fatal("first read")
	}
	mustPut(t, in, 1, "new")
	if vs, _ := p.Get(p.Epoch(), a, [][]byte{key(in, 1)}, kv.ReadOptions{Stmt: 2}); string(vs[0]) != "old" {
		t.Fatal("repeatable read must keep the old value")
	}
	// READ COMMITTED sees the new value on its next statement.
	rc := txnID()
	p.Begin(p.Epoch(), rc, kv.BeginOptions{Isolation: kv.ReadCommitted})
	p.Get(p.Epoch(), rc, [][]byte{key(in, 1)}, kv.ReadOptions{Stmt: 1})
	mustPut(t, in, 1, "newer")
	if vs, _ := p.Get(p.Epoch(), rc, [][]byte{key(in, 1)}, kv.ReadOptions{Stmt: 2}); string(vs[0]) != "newer" {
		t.Fatal("read committed must see the latest commit")
	}
	// A locking read sees the latest committed version, not the snapshot.
	if vs, _ := p.Get(p.Epoch(), a, [][]byte{key(in, 1)}, kv.ReadOptions{Stmt: 3, Lock: kv.LockExclusive}); string(vs[0]) != "newer" {
		t.Fatalf("locking read must read the latest version, got %q", vs[0])
	}
	p.Rollback(p.Epoch(), a)
}

func testRollbackAndStatementRollback(t *testing.T, in Instance) {
	p := in.Owner()
	mustPut(t, in, 1, "a")
	id := begin(t, in)
	p.Mutate(p.Epoch(), id, 1, []kv.Mutation{{Op: kv.OpPut, Key: key(in, 2), Value: []byte("b")}}, 0)
	p.Mutate(p.Epoch(), id, 2, []kv.Mutation{{Op: kv.OpPut, Key: key(in, 3), Value: []byte("c")}}, 0)
	err := p.Mutate(p.Epoch(), id, 2, []kv.Mutation{{Op: kv.OpInsertOnly, Key: key(in, 1), Value: []byte("dup")}}, 0)
	if kv.KindOf(err) != kv.KindDuplicateKey {
		t.Fatalf("expected duplicate, got %v", err)
	}
	p.RollbackStatement(p.Epoch(), id, 2)
	rows, _, err := p.Scan(p.Epoch(), id, key(in, 0), key(in, 10), 0, kv.ReadOptions{Stmt: 3})
	if err != nil || len(rows) != 2 {
		t.Fatalf("scan inside txn: %d rows %v", len(rows), err)
	}
	if _, err := commit(in, id); err != nil {
		t.Fatal(err)
	}
	if get(t, in, 2) != "b" || get(t, in, 3) != "" {
		t.Fatal("statement rollback")
	}
	id2 := begin(t, in)
	p.Mutate(p.Epoch(), id2, 1, []kv.Mutation{{Op: kv.OpDelete, Key: key(in, 1)}}, 0)
	p.Rollback(p.Epoch(), id2)
	if get(t, in, 1) != "a" {
		t.Fatal("rollback left a trace")
	}
}

func testScanMergesIntents(t *testing.T, in Instance) {
	p := in.Owner()
	for i := 0; i < 10; i += 2 {
		mustPut(t, in, i, fmt.Sprint(i))
	}
	id := begin(t, in)
	p.Mutate(p.Epoch(), id, 1, []kv.Mutation{
		{Op: kv.OpPut, Key: key(in, 3), Value: []byte("3")},
		{Op: kv.OpDelete, Key: key(in, 4)},
		{Op: kv.OpPut, Key: key(in, 6), Value: []byte("six")},
	}, 0)
	var got []string
	start := key(in, 0)
	for {
		rows, resume, err := p.Scan(p.Epoch(), id, start, key(in, 100), 2, kv.ReadOptions{Stmt: 2})
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

func testTxnStatusAndIdempotentCommit(t *testing.T, in Instance) {
	p := in.Owner()
	id := begin(t, in)
	p.Mutate(p.Epoch(), id, 1, []kv.Mutation{{Op: kv.OpPut, Key: key(in, 1), Value: []byte("x")}}, 0)
	ts, err := commit(in, id)
	if err != nil {
		t.Fatal(err)
	}
	if ts2, err := commit(in, id); err != nil || ts2 != ts {
		t.Fatal("retried commit must return the original commit_ts")
	}
	in.Restart(t)
	q := in.Owner()
	st, ts3, _ := q.TxnStatus(q.Epoch(), id)
	if st != kv.TxnCommitted || ts3 != ts {
		t.Fatalf("status after takeover: %v %d", st, ts3)
	}
	if st, _, _ := q.TxnStatus(q.Epoch(), txnID()); st != kv.TxnUnknown {
		t.Fatal("unknown id")
	}
}

func testEpochMismatch(t *testing.T, in Instance) {
	p := in.Owner()
	if err := p.Begin(p.Epoch()+1, txnID(), kv.BeginOptions{}); kv.KindOf(err) != kv.KindEpochChanged {
		t.Fatalf("epoch mismatch must return EpochChanged, got %v", err)
	}
}

func testGroupCommitConcurrent(t *testing.T, in Instance) {
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := put(in, i, "c"); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	in.Restart(t)
	for i := 0; i < 200; i++ {
		if get(t, in, i) != "c" {
			t.Fatalf("key %d lost", i)
		}
	}
}

func testAutoIncrementSurvivesRestart(t *testing.T, in Instance) {
	p := in.Owner()
	id := begin(t, in)
	first, _ := p.AutoIncrement(p.Epoch(), id, 2000, 3, 0)
	if first != 1 {
		t.Fatalf("first value %d", first)
	}
	// A rolled-back transaction's values are not handed out again.
	rb := begin(t, in)
	if v, _ := p.AutoIncrement(p.Epoch(), rb, 2000, 1, 0); v != 4 {
		t.Fatalf("second allocation %d", v)
	}
	p.Rollback(p.Epoch(), rb)
	if v, _ := p.AutoIncrement(p.Epoch(), id, 2000, 0, 100); v != 100 {
		t.Fatalf("explicit value raises the counter: %d", v)
	}
	p.Mutate(p.Epoch(), id, 1, []kv.Mutation{{Op: kv.OpPut, Key: key(in, 1), Value: []byte("x")}}, 0)
	if _, err := commit(in, id); err != nil {
		t.Fatal(err)
	}
	in.Restart(t)
	q := in.Owner()
	if v, _ := q.AutoIncrement(q.Epoch(), kv.TxnID{}, 2000, 0, 0); v != 100 {
		t.Fatalf("counter after restart: %d", v)
	}
}

// A snapshot taken before anything was committed has timestamp 0, which once
// also meant "no snapshot yet": the next read took a new one, so reads on a
// fresh partition were not repeatable.
func testSnapshotOnEmptyPartitionIsRepeatable(t *testing.T, in Instance) {
	p := in.Owner()
	for _, iso := range []kv.Isolation{kv.RepeatableRead, kv.ReadCommitted} {
		reader := txnID()
		if err := p.Begin(p.Epoch(), reader, kv.BeginOptions{Isolation: iso}); err != nil {
			t.Fatal(err)
		}
		k := key(in, int(iso)+100)
		if vs, err := p.Get(p.Epoch(), reader, [][]byte{k}, kv.ReadOptions{Stmt: 1}); err != nil || vs[0] != nil {
			t.Fatalf("first read: %q %v", vs, err)
		}
		mustPut(t, in, int(iso)+100, "later")
		// Same statement: neither RR nor RC may see the later commit.
		if vs, err := p.Get(p.Epoch(), reader, [][]byte{k}, kv.ReadOptions{Stmt: 1}); err != nil || vs[0] != nil {
			t.Fatalf("isolation %d: second read in the statement saw %q (%v)", iso, vs[0], err)
		}
		_ = p.Rollback(p.Epoch(), reader)
	}
}

// A commit guard fails once the guarded value changed: the schema-version
// check of the query engines.
func testCommitGuards(t *testing.T, in Instance) {
	p := in.Owner()
	mustPut(t, in, 1, "v1")
	ok := begin(t, in)
	write(in, ok, 1, 2, "x", 0)
	if _, err := p.CommitGuarded(p.Epoch(), ok, []kv.Guard{{Key: key(in, 1), Value: []byte("v1")}, {Key: key(in, 9)}}); err != nil {
		t.Fatalf("guards that hold: %v", err)
	}
	stale := begin(t, in)
	write(in, stale, 1, 3, "y", 0)
	mustPut(t, in, 1, "v2")
	if _, err := p.CommitGuarded(p.Epoch(), stale, []kv.Guard{{Key: key(in, 1), Value: []byte("v1")}}); kv.KindOf(err) != kv.KindSchemaChanged {
		t.Fatalf("a stale guard must fail with SchemaChanged, got %v", err)
	}
	if get(t, in, 3) != "" {
		t.Fatal("a failed guard must not commit")
	}
}

// ---- locks --------------------------------------------------------------------------

func testWriteWaitsForLock(t *testing.T, in Instance) {
	mustPut(t, in, 1, "0")
	a, b := begin(t, in), begin(t, in)
	if err := write(in, a, 1, 1, "a", 0); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- write(in, b, 1, 1, "b", 0) }()
	select {
	case err := <-done:
		t.Fatalf("b must wait for a's lock, got %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	if _, err := commit(in, a); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatalf("b after a committed: %v", err)
	}
	if _, err := commit(in, b); err != nil {
		t.Fatal(err)
	}
	if get(t, in, 1) != "b" {
		t.Fatal("last writer wins")
	}
}

func testLockWaitTimeout(t *testing.T, in Instance) {
	a, b := begin(t, in), begin(t, in)
	write(in, a, 1, 1, "a", 0)
	start := time.Now()
	err := write(in, b, 1, 1, "b", 100*time.Millisecond)
	if kv.KindOf(err) != kv.KindLockTimeout {
		t.Fatalf("want LockTimeout, got %v", err)
	}
	if time.Since(start) < 100*time.Millisecond {
		t.Fatal("returned before the timeout")
	}
	// The transaction survives a lock timeout; only the statement failed.
	if err := write(in, b, 2, 2, "b2", 0); err != nil {
		t.Fatal(err)
	}
	commit(in, a)
	if _, err := commit(in, b); err != nil {
		t.Fatal(err)
	}
}

func testDeadlockYoungestIsVictim(t *testing.T, in Instance) {
	old := begin(t, in)
	time.Sleep(time.Millisecond)
	young := begin(t, in)
	write(in, old, 1, 1, "old", 0)
	write(in, young, 1, 2, "young", 0)
	oldDone := make(chan error, 1)
	go func() { oldDone <- write(in, old, 2, 2, "old", 0) }() // old waits for young
	time.Sleep(50 * time.Millisecond)
	err := write(in, young, 2, 1, "young", 0) // closes the cycle
	if kv.KindOf(err) != kv.KindDeadlock {
		t.Fatalf("the youngest transaction must be the victim, got %v", err)
	}
	if err := <-oldDone; err != nil {
		t.Fatalf("the survivor should get the lock: %v", err)
	}
	if _, err := commit(in, young); kv.KindOf(err) != kv.KindTxnNotFound {
		t.Fatalf("the victim is rolled back, got %v", err)
	}
	if _, err := commit(in, old); err != nil {
		t.Fatal(err)
	}
	if get(t, in, 1) != "old" || get(t, in, 2) != "old" {
		t.Fatal("survivor's writes")
	}
}

// The cycle is closed by the older transaction, so the younger one, already
// waiting, is the victim and is woken with Deadlock.
func testDeadlockOlderWaiterVictimized(t *testing.T, in Instance) {
	old := begin(t, in)
	time.Sleep(time.Millisecond)
	young := begin(t, in)
	write(in, old, 1, 1, "old", 0)
	write(in, young, 1, 2, "young", 0)
	youngDone := make(chan error, 1)
	go func() { youngDone <- write(in, young, 2, 1, "young", 0) }()
	time.Sleep(50 * time.Millisecond)
	if err := write(in, old, 2, 2, "old", 0); err != nil {
		t.Fatalf("older transaction should win: %v", err)
	}
	if err := <-youngDone; kv.KindOf(err) != kv.KindDeadlock {
		t.Fatalf("waiting younger transaction must be the victim, got %v", err)
	}
}

func testSharedLocks(t *testing.T, in Instance) {
	p := in.Owner()
	mustPut(t, in, 1, "v")
	a, b, c := begin(t, in), begin(t, in), begin(t, in)
	for _, id := range []kv.TxnID{a, b} {
		if _, err := p.Get(p.Epoch(), id, [][]byte{key(in, 1)}, kv.ReadOptions{Stmt: 1, Lock: kv.LockShared}); err != nil {
			t.Fatal(err)
		}
	}
	if err := write(in, c, 1, 1, "c", 50*time.Millisecond); kv.KindOf(err) != kv.KindLockTimeout {
		t.Fatalf("a writer must wait for shared holders, got %v", err)
	}
	commit(in, a)
	commit(in, b)
	if err := write(in, c, 2, 1, "c", 0); err != nil {
		t.Fatal(err)
	}
}

func testLockingScanReadsLatest(t *testing.T, in Instance) {
	p := in.Owner()
	for i := 0; i < 5; i++ {
		mustPut(t, in, i, "0")
	}
	a := begin(t, in)
	p.Scan(p.Epoch(), a, key(in, 0), key(in, 10), 0, kv.ReadOptions{Stmt: 1}) // takes the snapshot
	mustPut(t, in, 2, "1")
	rows, _, err := p.Scan(p.Epoch(), a, key(in, 0), key(in, 10), 0, kv.ReadOptions{Stmt: 2, Lock: kv.LockExclusive})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 5 || string(rows[2].Value) != "1" {
		t.Fatalf("locking scan must see the latest versions: %v", rows)
	}
	b := begin(t, in)
	if err := write(in, b, 1, 4, "b", 30*time.Millisecond); kv.KindOf(err) != kv.KindLockTimeout {
		t.Fatalf("rows read FOR UPDATE are locked, got %v", err)
	}
}

// Many transactions move money between accounts with locking reads; the
// total never changes.
func testMoneyTransfers(t *testing.T, in Instance) {
	p := in.Owner()
	const accounts, start = 10, 1000
	for i := 0; i < accounts; i++ {
		mustPut(t, in, i, string(encoding.ValueInt(start)))
	}
	var wg sync.WaitGroup
	var committed, aborted atomic.Int64
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				from, to := (w+i)%accounts, (w*3+i*7+1)%accounts
				if from == to {
					continue
				}
				id := begin(t, in)
				vs, err := p.Get(p.Epoch(), id, [][]byte{key(in, from), key(in, to)}, kv.ReadOptions{Stmt: 1, Lock: kv.LockExclusive})
				if err != nil {
					aborted.Add(1)
					p.Rollback(p.Epoch(), id)
					continue
				}
				a, _ := encoding.ParseInt(vs[0])
				b, _ := encoding.ParseInt(vs[1])
				err = p.Mutate(p.Epoch(), id, 2, []kv.Mutation{
					{Op: kv.OpPut, Key: key(in, from), Value: encoding.ValueInt(a - 1)},
					{Op: kv.OpPut, Key: key(in, to), Value: encoding.ValueInt(b + 1)},
				}, 0)
				if err == nil {
					_, err = commit(in, id)
				}
				if err != nil {
					aborted.Add(1)
					p.Rollback(p.Epoch(), id)
					continue
				}
				committed.Add(1)
			}
		}(w)
	}
	wg.Wait()
	id := begin(t, in)
	rows, _, _ := p.Scan(p.Epoch(), id, key(in, 0), key(in, accounts), 0, kv.ReadOptions{Stmt: 1})
	var total int64
	for _, r := range rows {
		n, _ := encoding.ParseInt(r.Value)
		total += n
	}
	if total != accounts*start {
		t.Fatalf("total changed: %d", total)
	}
	if committed.Load() == 0 {
		t.Fatal("nothing committed")
	}
	t.Logf("committed %d, aborted %d", committed.Load(), aborted.Load())
}
