package kv

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/salesstair-01/001/internal/encoding"
	"github.com/salesstair-01/001/internal/objstore"
)

func begin(t *testing.T, p *Partition) TxnID {
	t.Helper()
	id := txnID()
	if err := p.Begin(p.epoch, id, BeginOptions{}); err != nil {
		t.Fatal(err)
	}
	return id
}

func write(p *Partition, id TxnID, stmt uint64, k int, v string, wait time.Duration) error {
	return p.Mutate(p.epoch, id, stmt, []Mutation{{Op: OpPut, Key: key(k), Value: []byte(v)}}, wait)
}

func TestWriteWaitsForLock(t *testing.T) {
	p := open(t, objstore.NewMem(), "a")
	put(p, 1, "0")
	a, b := begin(t, p), begin(t, p)
	if err := write(p, a, 1, 1, "a", 0); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- write(p, b, 1, 1, "b", 0) }()
	select {
	case err := <-done:
		t.Fatalf("b must wait for a's lock, got %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	if _, err := p.Commit(p.epoch, a); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatalf("b after a committed: %v", err)
	}
	if _, err := p.Commit(p.epoch, b); err != nil {
		t.Fatal(err)
	}
	if get(t, p, 1) != "b" {
		t.Fatal("last writer wins")
	}
}

func TestLockWaitTimeout(t *testing.T) {
	p := open(t, objstore.NewMem(), "a")
	a, b := begin(t, p), begin(t, p)
	write(p, a, 1, 1, "a", 0)
	start := time.Now()
	err := write(p, b, 1, 1, "b", 100*time.Millisecond)
	if KindOf(err) != KindLockTimeout {
		t.Fatalf("want LockTimeout, got %v", err)
	}
	if time.Since(start) < 100*time.Millisecond {
		t.Fatal("returned before the timeout")
	}
	// The transaction survives a lock timeout; only the statement failed.
	if err := write(p, b, 2, 2, "b2", 0); err != nil {
		t.Fatal(err)
	}
	p.Commit(p.epoch, a)
	if _, err := p.Commit(p.epoch, b); err != nil {
		t.Fatal(err)
	}
}

func TestDeadlockYoungestIsVictim(t *testing.T) {
	p := open(t, objstore.NewMem(), "a")
	old := begin(t, p)
	time.Sleep(time.Millisecond)
	young := begin(t, p)
	write(p, old, 1, 1, "old", 0)
	write(p, young, 1, 2, "young", 0)
	oldDone := make(chan error, 1)
	go func() { oldDone <- write(p, old, 2, 2, "old", 0) }() // old waits for young
	time.Sleep(20 * time.Millisecond)
	err := write(p, young, 2, 1, "young", 0) // closes the cycle
	if KindOf(err) != KindDeadlock {
		t.Fatalf("the youngest transaction must be the victim, got %v", err)
	}
	if err := <-oldDone; err != nil {
		t.Fatalf("the survivor should get the lock: %v", err)
	}
	if _, err := p.Commit(p.epoch, young); KindOf(err) != KindTxnNotFound {
		t.Fatalf("the victim is rolled back, got %v", err)
	}
	if _, err := p.Commit(p.epoch, old); err != nil {
		t.Fatal(err)
	}
	if get(t, p, 1) != "old" || get(t, p, 2) != "old" {
		t.Fatal("survivor's writes")
	}
}

func TestDeadlockOlderWaiterVictimized(t *testing.T) {
	// The cycle is closed by the older transaction, so the younger one,
	// already waiting, is the victim and is woken with Deadlock.
	p := open(t, objstore.NewMem(), "a")
	old := begin(t, p)
	time.Sleep(time.Millisecond)
	young := begin(t, p)
	write(p, old, 1, 1, "old", 0)
	write(p, young, 1, 2, "young", 0)
	youngDone := make(chan error, 1)
	go func() { youngDone <- write(p, young, 2, 1, "young", 0) }()
	time.Sleep(20 * time.Millisecond)
	if err := write(p, old, 2, 2, "old", 0); err != nil {
		t.Fatalf("older transaction should win: %v", err)
	}
	if err := <-youngDone; KindOf(err) != KindDeadlock {
		t.Fatalf("waiting younger transaction must be the victim, got %v", err)
	}
}

func TestSharedLocks(t *testing.T) {
	p := open(t, objstore.NewMem(), "a")
	put(p, 1, "v")
	a, b, c := begin(t, p), begin(t, p), begin(t, p)
	for _, id := range []TxnID{a, b} {
		if _, err := p.Get(p.epoch, id, [][]byte{key(1)}, ReadOptions{Stmt: 1, Lock: LockShared}); err != nil {
			t.Fatal(err)
		}
	}
	if err := write(p, c, 1, 1, "c", 50*time.Millisecond); KindOf(err) != KindLockTimeout {
		t.Fatalf("a writer must wait for shared holders, got %v", err)
	}
	p.Commit(p.epoch, a)
	p.Commit(p.epoch, b)
	if err := write(p, c, 2, 1, "c", 0); err != nil {
		t.Fatal(err)
	}
}

func TestLockingScanReadsLatest(t *testing.T) {
	p := open(t, objstore.NewMem(), "a")
	for i := 0; i < 5; i++ {
		put(p, i, "0")
	}
	a := begin(t, p)
	p.Scan(p.epoch, a, key(0), key(10), 0, ReadOptions{Stmt: 1}) // takes the snapshot
	put(p, 2, "1")
	rows, _, err := p.Scan(p.epoch, a, key(0), key(10), 0, ReadOptions{Stmt: 2, Lock: LockExclusive})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 5 || string(rows[2].Value) != "1" {
		t.Fatalf("locking scan must see the latest versions: %v", rows)
	}
	b := begin(t, p)
	if err := write(p, b, 1, 4, "b", 30*time.Millisecond); KindOf(err) != KindLockTimeout {
		t.Fatalf("rows read FOR UPDATE are locked, got %v", err)
	}
}

func TestLeaseExpiryFreesLocks(t *testing.T) {
	cfg := testConfig(t, objstore.NewMem(), "a")
	cfg.TxnLease = 200 * time.Millisecond
	p, err := Takeover(t.Context(), cfg, pid)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	dead := begin(t, p)
	write(p, dead, 1, 1, "dead", 0) // its query engine dies here
	b := begin(t, p)
	start := time.Now()
	stop := make(chan struct{})
	go func() { // b's own query engine keeps heartbeating
		for {
			select {
			case <-stop:
				return
			case <-time.After(50 * time.Millisecond):
				p.Heartbeat(p.epoch, []TxnID{b})
			}
		}
	}()
	defer close(stop)
	if err := write(p, b, 1, 1, "b", 5*time.Second); err != nil {
		t.Fatalf("b should get the lock once the lease expires: %v", err)
	}
	if time.Since(start) > 3*time.Second {
		t.Fatal("lease expiry too slow")
	}
}

// Slice 4 concurrency test: many transactions move money between accounts
// with locking reads; the total never changes.
func TestMoneyTransfers(t *testing.T) {
	p := open(t, objstore.NewMem(), "a")
	const accounts, start = 10, 1000
	for i := 0; i < accounts; i++ {
		put(p, i, string(encoding.ValueInt(start)))
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
				id := begin(t, p)
				vs, err := p.Get(p.epoch, id, [][]byte{key(from), key(to)}, ReadOptions{Stmt: 1, Lock: LockExclusive})
				if err != nil {
					aborted.Add(1)
					p.Rollback(p.epoch, id)
					continue
				}
				a, _ := encoding.ParseInt(vs[0])
				b, _ := encoding.ParseInt(vs[1])
				err = p.Mutate(p.epoch, id, 2, []Mutation{
					{Op: OpPut, Key: key(from), Value: encoding.ValueInt(a - 1)},
					{Op: OpPut, Key: key(to), Value: encoding.ValueInt(b + 1)},
				}, 0)
				if err == nil {
					_, err = p.Commit(p.epoch, id)
				}
				if err != nil {
					aborted.Add(1)
					p.Rollback(p.epoch, id)
					continue
				}
				committed.Add(1)
			}
		}(w)
	}
	wg.Wait()
	id := begin(t, p)
	rows, _, _ := p.Scan(p.epoch, id, key(0), key(accounts), 0, ReadOptions{Stmt: 1})
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
