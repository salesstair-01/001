package objstore

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"
)

// MinIOForTest connects to the MinIO named by MINIO_ENDPOINT, or skips.
func minioForTest(t *testing.T) *MinIO {
	t.Helper()
	ep := os.Getenv("MINIO_ENDPOINT")
	if ep == "" {
		t.Skip("MINIO_ENDPOINT not set")
	}
	user, pass := os.Getenv("MINIO_ACCESS_KEY"), os.Getenv("MINIO_SECRET_KEY")
	if user == "" {
		user, pass = "minioadmin", "minioadmin"
	}
	m, err := NewMinIO(context.Background(), MinIOConfig{
		Endpoint: ep, AccessKey: user, SecretKey: pass, Bucket: "s2z-test",
		Prefix: fmt.Sprintf("objstore-test-%d/", time.Now().UnixNano()),
	})
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func testStore(t *testing.T, s Store) {
	ctx := context.Background()
	if err := s.PutIfAbsent(ctx, "a/1", []byte("one")); err != nil {
		t.Fatal(err)
	}
	// Verify-before-coding item: a second put-if-absent on the same key is a 412.
	if err := s.PutIfAbsent(ctx, "a/1", []byte("other")); err != ErrPreconditionFailed {
		t.Fatalf("want 412, got %v", err)
	}
	// Compare-on-412: GET returns the first writer's content.
	b, err := s.Get(ctx, "a/1")
	if err != nil || string(b) != "one" {
		t.Fatalf("get: %q %v", b, err)
	}
	if _, err := s.Get(ctx, "a/missing"); err != ErrNotFound {
		t.Fatalf("want not found, got %v", err)
	}
	for _, k := range []string{"a/3", "a/2", "b/1"} {
		if err := s.PutIfAbsent(ctx, k, []byte(k)); err != nil {
			t.Fatal(err)
		}
	}
	keys, err := s.List(ctx, "a/", "a/1")
	if err != nil || fmt.Sprint(keys) != "[a/2 a/3]" {
		t.Fatalf("list: %v %v", keys, err)
	}
	if err := s.Delete(ctx, "a/2"); err != nil {
		t.Fatal(err)
	}
	keys, _ = s.List(ctx, "a/", "")
	if fmt.Sprint(keys) != "[a/1 a/3]" {
		t.Fatalf("list after delete: %v", keys)
	}
}

func TestMem(t *testing.T)   { testStore(t, NewMem()) }
func TestMinIO(t *testing.T) { testStore(t, minioForTest(t)) }

func TestFaultyDropResponse(t *testing.T) {
	inner := NewMem()
	f := NewFaulty(inner, FaultConfig{Seed: 1, DropResponseProb: 1})
	ctx := context.Background()
	if err := f.PutIfAbsent(ctx, "k", []byte("v")); err != ErrInjected {
		t.Fatalf("want injected, got %v", err)
	}
	if b, err := inner.Get(ctx, "k"); err != nil || string(b) != "v" {
		t.Fatal("write should have landed")
	}
}
