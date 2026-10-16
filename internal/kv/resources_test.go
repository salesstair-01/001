package kv

import (
	"context"
	"testing"
	"time"

	"github.com/salesstair-01/001/internal/objstore"
)

// A partition over its PUT quota slows down; one sharing the node's PUT
// slots but under its quota does not.
func TestPutQuota(t *testing.T) {
	s := objstore.NewMem()
	slow := testConfig(t, s, "kv-a")
	slow.PutRate, slow.PutBurst = 20, 1
	slow.res = newNodeResources(&Config{PutConcurrency: 4})
	defer slow.res.close()
	fast := slow
	fast.DataDir = t.TempDir()
	fast.PutRate = -1

	ps, err := Takeover(context.Background(), slow, pid)
	if err != nil {
		t.Fatal(err)
	}
	defer ps.Close()
	pf, err := Takeover(context.Background(), fast, pid+1)
	if err != nil {
		t.Fatal(err)
	}
	defer pf.Close()

	time.Sleep(100 * time.Millisecond) // let the takeover's PUTs drain the bucket
	start := time.Now()
	for i := 0; i < 6; i++ {
		if _, err := put(ps, i, "v"); err != nil {
			t.Fatal(err)
		}
	}
	if d := time.Since(start); d < 200*time.Millisecond {
		t.Fatalf("6 commits at 20 PUT/s took %v", d)
	}
	start = time.Now()
	for i := 0; i < 6; i++ {
		if _, err := put(pf, i, "v"); err != nil {
			t.Fatal(err)
		}
	}
	if d := time.Since(start); d > 200*time.Millisecond {
		t.Fatalf("unlimited partition: 6 commits took %v", d)
	}
	if ps.Puts() < 6 || pf.Puts() < 6 {
		t.Fatalf("puts: %d %d", ps.Puts(), pf.Puts())
	}
}

func TestTokenBucket(t *testing.T) {
	b := newTokenBucket(100, 5)
	start := time.Now()
	for i := 0; i < 15; i++ {
		if err := b.wait(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	// 5 from the burst, 10 more at 100/s.
	if d := time.Since(start); d < 80*time.Millisecond || d > 400*time.Millisecond {
		t.Fatalf("15 tokens took %v", d)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	b2 := newTokenBucket(0.001, 1)
	_ = b2.wait(ctx)
	if err := b2.wait(ctx); err == nil {
		t.Fatal("wait on a cancelled context returned nil")
	}
}
