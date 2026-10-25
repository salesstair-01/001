// Command s3bucket creates the object-store bucket kvd uses, if it does not
// exist, from MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY,
// MINIO_BUCKET (default s2z) and MINIO_REGION:
//
//	make ceph-up && eval "$(scripts/ceph/microceph.sh env)" && go run ./cmd/s3bucket
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/salesstair-01/001/internal/objstore"
)

func env(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

func main() {
	cfg := objstore.MinIOConfig{
		Endpoint:  env("MINIO_ENDPOINT", "127.0.0.1:9000"),
		AccessKey: env("MINIO_ACCESS_KEY", "minioadmin"),
		SecretKey: env("MINIO_SECRET_KEY", "minioadmin"),
		Bucket:    env("MINIO_BUCKET", "s2z"),
		Region:    os.Getenv("MINIO_REGION"),
	}
	if _, err := objstore.NewMinIO(context.Background(), cfg); err != nil {
		fmt.Fprintln(os.Stderr, "s3bucket:", err)
		os.Exit(1)
	}
	fmt.Printf("bucket %s ready on %s\n", cfg.Bucket, cfg.Endpoint)
}
