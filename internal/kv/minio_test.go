package kv

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/salesstair-01/001/internal/objstore"
)

// TestMinIOTakeoverAndFencing runs the durability protocol against a real
// MinIO (set MINIO_ENDPOINT): commit, takeover by a second owner, fencing of
// the first, checkpoint, and recovery through a third owner.
func TestMinIOTakeoverAndFencing(t *testing.T) {
	ep := os.Getenv("MINIO_ENDPOINT")
	if ep == "" {
		t.Skip("MINIO_ENDPOINT not set")
	}
	s, err := objstore.NewMinIO(context.Background(), objstore.MinIOConfig{
		Endpoint: ep, AccessKey: "minioadmin", SecretKey: "minioadmin", Bucket: "s2z-test",
		Prefix: fmt.Sprintf("kv-test-%d/", time.Now().UnixNano()),
	})
	if err != nil {
		t.Fatal(err)
	}
	a := open(t, s, "a")
	for i := 0; i < 20; i++ {
		if _, err := put(a, i, "a"); err != nil {
			t.Fatal(err)
		}
	}
	b := open(t, s, "b")
	if _, err := put(a, 100, "zombie"); KindOf(err) != KindEpochChanged {
		t.Fatalf("zombie owner must be fenced: %v", err)
	}
	for i := 10; i < 30; i++ {
		if _, err := put(b, i, "b"); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Checkpoint(context.Background()); err != nil {
		t.Fatal(err)
	}
	put(b, 30, "after-checkpoint")
	b.Close()
	c := open(t, s, "c")
	for i := 0; i <= 30; i++ {
		want := "a"
		switch {
		case i == 30:
			want = "after-checkpoint"
		case i >= 10:
			want = "b"
		}
		if got := get(t, c, i); got != want {
			t.Fatalf("key %d: %q, want %q", i, got, want)
		}
	}
	if get(t, c, 100) != "" {
		t.Fatal("the fenced owner's write must not exist")
	}
}

// TestMinIORemoteSSTs runs remote SSTs against a real MinIO: SSTs written by
// flushes and compactions, a wake that reads them with ranged GETs, and a
// fenced owner flushing under its own epoch.
func TestMinIORemoteSSTs(t *testing.T) {
	ep := os.Getenv("MINIO_ENDPOINT")
	if ep == "" {
		t.Skip("MINIO_ENDPOINT not set")
	}
	ctx := context.Background()
	s, err := objstore.NewMinIO(ctx, objstore.MinIOConfig{
		Endpoint: ep, AccessKey: "minioadmin", SecretKey: "minioadmin", Bucket: "s2z-test",
		Prefix: fmt.Sprintf("kv-remote-test-%d/", time.Now().UnixNano()),
	})
	if err != nil {
		t.Fatal(err)
	}
	a, err := Takeover(ctx, remoteTestConfig(t, s, "a"), pid)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	fill(t, a, 0, 1000, "a")
	checkpoint(t, a)
	if len(a.manifest.Remote) == 0 {
		t.Fatal("no remote SSTs")
	}
	b, err := Takeover(ctx, remoteTestConfig(t, s, "b"), pid)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	for _, i := range []int{0, 500, 999} {
		if got := get(t, b, i); got != val("a", i) {
			t.Fatalf("key %d = %.20q", i, got)
		}
	}
	fill(t, b, 0, 1000, "b")
	checkpoint(t, b)
	if err := a.Checkpoint(ctx); err == nil {
		t.Fatal("fenced owner wrote a manifest")
	}
	for _, r := range b.manifest.Remote {
		if n, err := s.Size(ctx, r.Object); err != nil || n != r.Size {
			t.Fatalf("%s: size %d (want %d), %v", r.Object, n, r.Size, err)
		}
	}
	if got := get(t, b, 999); got != val("b", 999) {
		t.Fatalf("key 999 = %.20q", got)
	}
}
