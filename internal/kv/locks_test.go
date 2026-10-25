package kv

import (
	"testing"
	"time"

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

