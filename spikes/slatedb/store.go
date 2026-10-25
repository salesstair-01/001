package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	slatedb "slatedb.io/slatedb-go/uniffi"
)

// s3 is the S3-compatible store the spike runs against (Ceph RGW: mini.sh
// locally, MicroCeph in CI), configured like the owner's tests.
type s3 struct {
	endpoint, access, secret, bucket string
}

func s3FromEnv() s3 {
	env := func(k, d string) string {
		if v := os.Getenv(k); v != "" {
			return v
		}
		return d
	}
	return s3{
		endpoint: env("MINIO_ENDPOINT", "127.0.0.1:8000"),
		access:   env("MINIO_ACCESS_KEY", "s2zkey"),
		secret:   env("MINIO_SECRET_KEY", "s2zsecret"),
		bucket:   env("SLATE_BUCKET", "slatedb-spike"),
	}
}

// ensureBucket creates the bucket with a plain S3 client; SlateDB does not.
func (c s3) ensureBucket(ctx context.Context) error {
	cl, err := minio.New(c.endpoint, &minio.Options{Creds: credentials.NewStaticV4(c.access, c.secret, "")})
	if err != nil {
		return err
	}
	ok, err := cl.BucketExists(ctx, c.bucket)
	if err != nil || ok {
		return err
	}
	return cl.MakeBucket(ctx, c.bucket, minio.MakeBucketOptions{})
}

// client returns a plain S3 client, for counting and deleting objects.
func (c s3) client() (*minio.Client, error) {
	return minio.New(c.endpoint, &minio.Options{Creds: credentials.NewStaticV4(c.access, c.secret, "")})
}

// objectStore builds SlateDB's S3 store. endpoint overrides c.endpoint
// (the latency proxy listens elsewhere).
func (c s3) objectStore(endpoint string) (*slatedb.ObjectStore, error) {
	if endpoint == "" {
		endpoint = c.endpoint
	}
	b := slatedb.NewObjectStoreBuilder(slatedb.ObjectStoreTypeS3)
	defer b.Destroy()
	for k, v := range map[string]string{
		"aws_endpoint":          "http://" + endpoint,
		"aws_access_key_id":     c.access,
		"aws_secret_access_key": c.secret,
		"aws_bucket":            c.bucket,
		"aws_region":            "us-east-1",
		"allow_http":            "true",
	} {
		if err := b.WithConfig(k, v); err != nil {
			return nil, fmt.Errorf("config %s: %w", k, err)
		}
	}
	return b.Build()
}

// openDB opens a writable SlateDB at path with the spike's settings.
func openDB(store *slatedb.ObjectStore, path string, settings map[string]string) (*slatedb.Db, error) {
	b := slatedb.NewDbBuilder(path, store)
	defer b.Destroy()
	if len(settings) > 0 {
		s := slatedb.SettingsDefault()
		defer s.Destroy()
		for k, v := range settings {
			if err := s.Set(k, v); err != nil {
				return nil, fmt.Errorf("setting %s: %w", k, err)
			}
		}
		if err := b.WithSettings(s); err != nil {
			return nil, err
		}
	}
	return b.Build()
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func since(t time.Time) time.Duration { return time.Since(t).Round(time.Microsecond) }
