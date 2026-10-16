// Command s2zctl is the operator tool. Its commands work on object storage
// directly:
//
//	s2zctl restore-points -partition 42
//	s2zctl restore -src 42 -dst 1042 -at 2026-10-16T09:30:00Z
//
// A restore writes a new partition; point the tenant at it (the proxy's
// tenants file) once it is checked.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/salesstair-01/001/internal/kv"
	"github.com/salesstair-01/001/internal/objstore"
)

func env(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage: s2zctl <command> [flags]

commands:
  restore-points -partition N        list the times partition N can be restored to
  restore -src N -dst M -at TIME     write partition M as partition N was at TIME (RFC 3339)

run "s2zctl <command> -h" for flags`)
	os.Exit(2)
}

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	cmd := os.Args[1]
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	endpoint := fs.String("minio-endpoint", env("MINIO_ENDPOINT", "127.0.0.1:9000"), "MinIO host:port")
	access := fs.String("minio-access-key", env("MINIO_ACCESS_KEY", "minioadmin"), "MinIO access key")
	secret := fs.String("minio-secret-key", env("MINIO_SECRET_KEY", "minioadmin"), "MinIO secret key")
	bucket := fs.String("bucket", env("MINIO_BUCKET", "s2z"), "MinIO bucket")
	useSSL := fs.Bool("minio-ssl", false, "use TLS to MinIO")
	var partition, src, dst *uint64
	var at, scratch *string
	switch cmd {
	case "restore-points":
		partition = fs.Uint64("partition", 0, "partition id")
	case "restore":
		src = fs.Uint64("src", 0, "partition to restore from")
		dst = fs.Uint64("dst", 0, "new partition id to write")
		at = fs.String("at", "", "time to restore to, RFC 3339 (e.g. 2026-10-16T09:30:00Z)")
		scratch = fs.String("scratch", os.TempDir(), "directory for scratch files")
	default:
		usage()
	}
	_ = fs.Parse(os.Args[2:])

	ctx := context.Background()
	store, err := objstore.NewMinIO(ctx, objstore.MinIOConfig{
		Endpoint: *endpoint, AccessKey: *access, SecretKey: *secret, Bucket: *bucket, UseSSL: *useSSL,
	})
	if err != nil {
		fail(err)
	}
	switch cmd {
	case "restore-points":
		if *partition == 0 {
			fail(fmt.Errorf("-partition is required"))
		}
		points, err := kv.RestorePoints(ctx, store, *partition)
		if err != nil {
			fail(err)
		}
		if len(points) == 0 {
			fmt.Println("no restore points")
			return
		}
		fmt.Printf("partition %d can be restored to any time from %s on\n", *partition, points[0].CreatedAt.UTC().Format(time.RFC3339Nano))
		for _, p := range points {
			fmt.Printf("  manifest %d  %s  files=%d  wal-after=%d\n", p.N, p.CreatedAt.UTC().Format(time.RFC3339Nano), p.Files, p.WALAfter)
		}
	case "restore":
		if *src == 0 || *dst == 0 || *at == "" {
			fail(fmt.Errorf("-src, -dst and -at are required"))
		}
		t, err := time.Parse(time.RFC3339Nano, *at)
		if err != nil {
			fail(fmt.Errorf("-at: %w", err))
		}
		start := time.Now()
		res, err := kv.Restore(ctx, store, *src, *dst, t, *scratch)
		if err != nil {
			fail(err)
		}
		last := "none"
		if !res.LastCommit.IsZero() {
			last = res.LastCommit.UTC().Format(time.RFC3339Nano)
		}
		fmt.Printf("restored partition %d at %s into partition %d in %v\n  from manifest %d (%s), %d WAL records replayed, last commit %s\n",
			*src, t.UTC().Format(time.RFC3339Nano), *dst, time.Since(start).Round(time.Millisecond),
			res.From.N, res.From.CreatedAt.UTC().Format(time.RFC3339Nano), res.Replayed, last)
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "s2zctl:", err)
	os.Exit(1)
}
