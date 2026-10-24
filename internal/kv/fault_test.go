package kv

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/salesstair-01/001/internal/objstore"
)

var faultRuns = flag.Int("fault-runs", 100, "number of seeded fault-injection runs (the slice-2 bar is 1000)")
var faultSeed = flag.Int64("fault-seed", -1, "replay a single seed")

// TestFaultInjection runs a write load against an owner whose object store
// delays, fails and drops responses, takes the partition over mid-load with a
// second owner, and checks that no acknowledged commit is lost.
func TestFaultInjection(t *testing.T) {
	seeds := make([]int64, 0, *faultRuns)
	if *faultSeed >= 0 {
		seeds = append(seeds, *faultSeed)
	} else {
		for i := 0; i < *faultRuns; i++ {
			seeds = append(seeds, int64(i))
		}
	}
	for _, seed := range seeds {
		if err := faultRun(t, seed, false); err != nil {
			t.Fatalf("seed %d: %v (replay with -fault-seed=%d)", seed, err, seed)
		}
	}
}

// TestFaultInjectionRemoteSSTs is the same load with SSTs on the object store
// (and small memtables, so faults also hit flushes and compactions).
func TestFaultInjectionRemoteSSTs(t *testing.T) {
	runs := *faultRuns / 4
	for seed := int64(0); seed < int64(runs); seed++ {
		if *faultSeed >= 0 && seed != *faultSeed {
			continue
		}
		if err := faultRun(t, seed, true); err != nil {
			t.Fatalf("seed %d: %v (replay with -run RemoteSSTs -fault-seed=%d)", seed, err, seed)
		}
	}
}

func takeoverRetry(t *testing.T, s objstore.Store, owner string, remote bool) (*Partition, error) {
	cfg := testConfig(t, s, owner)
	if remote {
		cfg.RemoteSSTs = true
		cfg.CacheMaxBytes = -1
		cfg.MemTableBytes = 16 << 10
	}
	cfg.CommitRetryTimeout = 500 * time.Millisecond
	cfg.Logger = discardLogger
	var err error
	for i := 0; i < 50; i++ {
		var p *Partition
		if p, err = Takeover(context.Background(), cfg, pid); err == nil {
			return p, nil
		}
	}
	return nil, fmt.Errorf("takeover %s: %w", owner, err)
}

func faultRun(t *testing.T, seed int64, remote bool) error {
	rng := rand.New(rand.NewSource(seed))
	mem := objstore.NewMem()
	f := objstore.NewFaulty(mem, objstore.FaultConfig{
		Seed: seed, FailProb: 0.1, DropResponseProb: 0.1, DelayProb: 0.5, MaxDelay: time.Millisecond,
	})
	a, err := takeoverRetry(t, f, "a", remote)
	if err != nil {
		return err
	}
	defer a.Close()

	var mu sync.Mutex
	acked := map[int]string{}
	write := func(p *Partition, k int) bool {
		v := fmt.Sprintf("v%d-%d", k, p.epoch)
		if remote {
			v += strings.Repeat("x", 1000) // fill memtables
		}
		if _, err := put(p, k, v); err != nil {
			return false
		}
		mu.Lock()
		acked[k] = v
		mu.Unlock()
		return true
	}

	var wg sync.WaitGroup
	for w := 0; w < 3; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				if !write(a, w*1000+i) && a.Stopped() != nil {
					return
				}
			}
		}(w)
	}
	pauses := []time.Duration{time.Duration(rng.Intn(3)) * time.Millisecond, time.Duration(rng.Intn(3)) * time.Millisecond}
	wg.Add(1)
	go func() { // occasional checkpoints on the old owner
		defer wg.Done()
		for i := 0; i < 2 && a.Stopped() == nil; i++ {
			time.Sleep(pauses[i])
			_ = a.Checkpoint(context.Background())
		}
	}()
	time.Sleep(time.Duration(rng.Intn(4)) * time.Millisecond)
	b, err := takeoverRetry(t, f, "b", remote) // the second owner starts mid-load
	if err != nil {
		return err
	}
	defer b.Close()
	wg.Wait()
	for i := 0; i < 10; i++ {
		write(b, 9000+i)
	}
	if rng.Intn(2) == 0 {
		_ = b.Checkpoint(context.Background())
	}
	a.Close()
	b.Close()

	// Verify from a clean store view.
	c, err := takeoverRetry(t, mem, "verify", remote)
	if err != nil {
		return err
	}
	defer c.Close()
	for k, v := range acked {
		if got := get(t, c, k); got != v {
			return fmt.Errorf("acknowledged key %d = %q, recovered %q", k, v, got)
		}
	}
	return nil
}
