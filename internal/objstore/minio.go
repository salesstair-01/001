package objstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// MinIOConfig configures a MinIO bucket.
type MinIOConfig struct {
	Endpoint  string // host:port
	AccessKey string
	SecretKey string
	Bucket    string
	UseSSL    bool
	// Prefix is prepended to every key, so several deployments can share a bucket.
	Prefix string
}

// MinIO is a Store backed by a MinIO bucket.
type MinIO struct {
	c      *minio.Client
	bucket string
	prefix string
}

// NewMinIO connects to MinIO and creates the bucket if it does not exist.
func NewMinIO(ctx context.Context, cfg MinIOConfig) (*MinIO, error) {
	c, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: cfg.UseSSL,
	})
	if err != nil {
		return nil, err
	}
	ok, err := c.BucketExists(ctx, cfg.Bucket)
	if err != nil {
		return nil, err
	}
	if !ok {
		if err := c.MakeBucket(ctx, cfg.Bucket, minio.MakeBucketOptions{}); err != nil {
			// Another process may have created it concurrently.
			if exists, err2 := c.BucketExists(ctx, cfg.Bucket); err2 != nil || !exists {
				return nil, err
			}
		}
	}
	return &MinIO{c: c, bucket: cfg.Bucket, prefix: cfg.Prefix}, nil
}

func (m *MinIO) PutIfAbsent(ctx context.Context, key string, data []byte) error {
	opts := minio.PutObjectOptions{
		ContentType: "application/octet-stream",
		// A single PUT keeps the precondition atomic; multipart would split it.
		DisableMultipart: true,
		SendContentMd5:   true,
	}
	opts.SetMatchETagExcept("*")
	_, err := m.c.PutObject(ctx, m.bucket, m.prefix+key, bytes.NewReader(data), int64(len(data)), opts)
	return translate(err)
}

func (m *MinIO) Get(ctx context.Context, key string) ([]byte, error) {
	obj, err := m.c.GetObject(ctx, m.bucket, m.prefix+key, minio.GetObjectOptions{})
	if err != nil {
		return nil, translate(err)
	}
	defer obj.Close()
	b, err := io.ReadAll(obj)
	if err != nil {
		return nil, translate(err)
	}
	return b, nil
}

func (m *MinIO) List(ctx context.Context, prefix, startAfter string) ([]string, error) {
	opts := minio.ListObjectsOptions{Prefix: m.prefix + prefix, Recursive: true}
	if startAfter != "" {
		opts.StartAfter = m.prefix + startAfter
	}
	var keys []string
	for obj := range m.c.ListObjects(ctx, m.bucket, opts) {
		if obj.Err != nil {
			return nil, translate(obj.Err)
		}
		keys = append(keys, obj.Key[len(m.prefix):])
	}
	return keys, nil
}

func (m *MinIO) Delete(ctx context.Context, key string) error {
	return translate(m.c.RemoveObject(ctx, m.bucket, m.prefix+key, minio.RemoveObjectOptions{}))
}

func (m *MinIO) GetRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	if n == 0 {
		return []byte{}, nil
	}
	opts := minio.GetObjectOptions{}
	if err := opts.SetRange(off, off+n-1); err != nil {
		return nil, err
	}
	obj, err := m.c.GetObject(ctx, m.bucket, m.prefix+key, opts)
	if err != nil {
		return nil, translate(err)
	}
	defer obj.Close()
	b := make([]byte, n)
	if _, err := io.ReadFull(obj, b); err != nil {
		return nil, translate(err)
	}
	return b, nil
}

func (m *MinIO) Size(ctx context.Context, key string) (int64, error) {
	info, err := m.c.StatObject(ctx, m.bucket, m.prefix+key, minio.StatObjectOptions{})
	if err != nil {
		return 0, translate(err)
	}
	return info.Size, nil
}

func translate(err error) error {
	if err == nil {
		return nil
	}
	var resp minio.ErrorResponse
	if errors.As(err, &resp) {
		switch {
		case resp.StatusCode == http.StatusPreconditionFailed || resp.Code == "PreconditionFailed":
			return ErrPreconditionFailed
		case resp.StatusCode == http.StatusNotFound || resp.Code == "NoSuchKey":
			return ErrNotFound
		}
	}
	return err
}
