package objstore

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"
)

// TestS3Compat checks what the owner relies on from an S3-compatible store,
// beyond what testStore covers. Point MINIO_ENDPOINT (plus MINIO_ACCESS_KEY,
// MINIO_SECRET_KEY, MINIO_BUCKET, MINIO_REGION as needed) at any store:
//
//	MINIO_ENDPOINT=127.0.0.1:8333 go test ./internal/objstore -run S3Compat -v
//
// Each subtest names one property; a store that fails any of them cannot
// hold a partition (put-if-absent is what fences owners, D4).
func TestS3Compat(t *testing.T) {
	ctx := context.Background()
	s := minioForTest(t)
	run := func(name string, f func(t *testing.T)) { t.Run(name, f) }

	run("put-if-absent refuses an existing key and keeps the first content", func(t *testing.T) {
		if err := s.PutIfAbsent(ctx, "pia/k", []byte("first")); err != nil {
			t.Fatal(err)
		}
		if err := s.PutIfAbsent(ctx, "pia/k", []byte("second")); !errors.Is(err, ErrPreconditionFailed) {
			t.Fatalf("second put: want 412, got %v", err)
		}
		if b, err := s.Get(ctx, "pia/k"); err != nil || string(b) != "first" {
			t.Fatalf("content after refused put: %q %v", b, err)
		}
	})

	run("put-if-absent is atomic under concurrent writers", func(t *testing.T) {
		// 16 writers race on each of 20 keys with different bodies: exactly
		// one may win, and the stored body must be the winner's.
		for k := 0; k < 20; k++ {
			key := fmt.Sprintf("race/%d", k)
			var mu sync.Mutex
			var winners []string
			var other []error
			var wg sync.WaitGroup
			for w := 0; w < 16; w++ {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					body := fmt.Sprintf("writer-%d", w)
					err := s.PutIfAbsent(ctx, key, []byte(body))
					mu.Lock()
					defer mu.Unlock()
					switch {
					case err == nil:
						winners = append(winners, body)
					case !errors.Is(err, ErrPreconditionFailed):
						other = append(other, err)
					}
				}(w)
			}
			wg.Wait()
			if len(other) > 0 {
				t.Fatalf("%s: unexpected errors: %v", key, other)
			}
			if len(winners) != 1 {
				t.Fatalf("%s: %d writers succeeded (%v), want exactly 1", key, len(winners), winners)
			}
			if b, err := s.Get(ctx, key); err != nil || string(b) != winners[0] {
				t.Fatalf("%s: stored %q, winner wrote %q (%v)", key, b, winners[0], err)
			}
		}
	})

	run("a deleted key can be created again", func(t *testing.T) {
		if err := s.PutIfAbsent(ctx, "recreate/k", []byte("a")); err != nil {
			t.Fatal(err)
		}
		if err := s.Delete(ctx, "recreate/k"); err != nil {
			t.Fatal(err)
		}
		if err := s.PutIfAbsent(ctx, "recreate/k", []byte("b")); err != nil {
			t.Fatalf("put after delete: %v", err)
		}
		if b, _ := s.Get(ctx, "recreate/k"); string(b) != "b" {
			t.Fatalf("got %q", b)
		}
	})

	run("missing keys report not found; deleting one is not an error", func(t *testing.T) {
		if _, err := s.Get(ctx, "missing/k"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("get: %v", err)
		}
		if _, err := s.Size(ctx, "missing/k"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("size: %v", err)
		}
		if _, err := s.GetRange(ctx, "missing/k", 0, 1); !errors.Is(err, ErrNotFound) {
			t.Fatalf("range: %v", err)
		}
		if err := s.Delete(ctx, "missing/k"); err != nil {
			t.Fatalf("delete: %v", err)
		}
	})

	run("ranged reads and sizes", func(t *testing.T) {
		data := make([]byte, 1<<20)
		rand.Read(data)
		if err := s.PutIfAbsent(ctx, "range/k", data); err != nil {
			t.Fatal(err)
		}
		if n, err := s.Size(ctx, "range/k"); err != nil || n != int64(len(data)) {
			t.Fatalf("size %d %v", n, err)
		}
		for _, r := range [][2]int64{{0, 1}, {4096, 32768}, {int64(len(data)) - 1, 1}, {0, int64(len(data))}} {
			b, err := s.GetRange(ctx, "range/k", r[0], r[1])
			if err != nil || !bytes.Equal(b, data[r[0]:r[0]+r[1]]) {
				t.Fatalf("range %v: %d bytes, %v", r, len(b), err)
			}
		}
		if _, err := s.GetRange(ctx, "range/k", int64(len(data))-10, 20); err == nil {
			t.Fatal("range past the end succeeded")
		}
	})

	run("a 16 MiB object in one PUT", func(t *testing.T) {
		data := make([]byte, 16<<20)
		rand.Read(data)
		if err := s.PutIfAbsent(ctx, "big/k", data); err != nil {
			t.Fatal(err)
		}
		if b, err := s.Get(ctx, "big/k"); err != nil || !bytes.Equal(b, data) {
			t.Fatalf("get: %d bytes, %v", len(b), err)
		}
	})

	run("list is ordered, paginates past 1000 keys, and honours start-after", func(t *testing.T) {
		var want []string
		var wg sync.WaitGroup
		errs := make(chan error, 1500)
		sem := make(chan struct{}, 32)
		for i := 0; i < 1500; i++ {
			k := fmt.Sprintf("list/%06d", i)
			want = append(want, k)
			wg.Add(1)
			sem <- struct{}{}
			go func() {
				defer wg.Done()
				defer func() { <-sem }()
				if err := s.PutIfAbsent(ctx, k, []byte("x")); err != nil {
					errs <- err
				}
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Fatal(err)
		}
		got, err := s.List(ctx, "list/", "")
		if err != nil {
			t.Fatal(err)
		}
		if !sort.StringsAreSorted(got) || fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("list: %d keys (sorted %v), want %d", len(got), sort.StringsAreSorted(got), len(want))
		}
		got, err = s.List(ctx, "list/", "list/001200")
		if err != nil || len(got) != 299 || got[0] != "list/001201" {
			t.Fatalf("start-after: %d keys, first %v, %v", len(got), got[:min(1, len(got))], err)
		}
	})

	run("reads and lists see a write at once", func(t *testing.T) {
		for i := 0; i < 50; i++ {
			k := fmt.Sprintf("raw/%d", i)
			if err := s.PutIfAbsent(ctx, k, []byte(k)); err != nil {
				t.Fatal(err)
			}
			if b, err := s.Get(ctx, k); err != nil || string(b) != k {
				t.Fatalf("read after write %s: %q %v", k, b, err)
			}
			keys, err := s.List(ctx, "raw/", "")
			if err != nil || len(keys) != i+1 {
				t.Fatalf("list after write %s: %d keys, %v", k, len(keys), err)
			}
		}
		if err := s.Delete(ctx, "raw/0"); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Get(ctx, "raw/0"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("read after delete: %v", err)
		}
	})

	run("latency of a small put-if-absent and get", func(t *testing.T) {
		var put, get time.Duration
		for i := 0; i < 50; i++ {
			k := fmt.Sprintf("lat/%d", i)
			t0 := time.Now()
			if err := s.PutIfAbsent(ctx, k, make([]byte, 4096)); err != nil {
				t.Fatal(err)
			}
			put += time.Since(t0)
			t0 = time.Now()
			if _, err := s.Get(ctx, k); err != nil {
				t.Fatal(err)
			}
			get += time.Since(t0)
		}
		t.Logf("mean put-if-absent %v, get %v (4 KiB, loopback)", put/50, get/50)
	})
}
