// Package objstore is the object-store client used by the KV owner. Object
// storage is the only durable state (D4). v1 targets MinIO only; every write
// that matters for fencing uses put-if-absent (If-None-Match: *).
package objstore

import (
	"context"
	"errors"
)

var (
	// ErrPreconditionFailed is returned when a put-if-absent finds the key already exists (HTTP 412).
	ErrPreconditionFailed = errors.New("objstore: precondition failed (412)")
	// ErrNotFound is returned by Get for a missing key.
	ErrNotFound = errors.New("objstore: not found")
)

// Store is the minimal object-store surface the durability layer needs.
type Store interface {
	// PutIfAbsent writes data at key only if nothing exists there yet.
	PutIfAbsent(ctx context.Context, key string, data []byte) error
	// Get returns the whole object.
	Get(ctx context.Context, key string) ([]byte, error)
	// List returns every key under prefix that sorts after startAfter, ascending.
	List(ctx context.Context, prefix, startAfter string) ([]string, error)
	// Delete removes key; deleting a missing key is not an error.
	Delete(ctx context.Context, key string) error
	// GetRange returns n bytes of the object starting at off. A range past
	// the end of the object is an error.
	GetRange(ctx context.Context, key string, off, n int64) ([]byte, error)
	// Size returns the object's length.
	Size(ctx context.Context, key string) (int64, error)
}
