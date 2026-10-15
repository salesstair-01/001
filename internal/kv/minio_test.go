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
