package kv

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/salesstair-01/001/internal/objstore"
)

// Object layout per partition (D4):
//
//	p/<partition>/manifest/<N>     numbered manifests, put-if-absent
//	p/<partition>/wal/<seq>        WAL segments, put-if-absent
//	p/<partition>/sst/<uuid>.sst   immutable SSTs
//	p/<partition>/meta/<uuid>/...  Pebble MANIFEST/OPTIONS files of one checkpoint
const seqDigits = 20

func partitionRoot(pid uint64) string     { return fmt.Sprintf("p/%d/", pid) }
func manifestPrefix(pid uint64) string    { return partitionRoot(pid) + "manifest/" }
func walPrefix(pid uint64) string         { return partitionRoot(pid) + "wal/" }
func manifestKey(pid, n uint64) string    { return manifestPrefix(pid) + pad(n) }
func walKey(pid, seq uint64) string       { return walPrefix(pid) + pad(seq) }
func sstKey(pid uint64, id string) string { return partitionRoot(pid) + "sst/" + id + ".sst" }
func metaKey(pid uint64, ckpt, name string) string {
	return partitionRoot(pid) + "meta/" + ckpt + "/" + name
}

func pad(n uint64) string { return fmt.Sprintf("%0*d", seqDigits, n) }

func parseNumbered(key, prefix string) (uint64, bool) {
	if !strings.HasPrefix(key, prefix) {
		return 0, false
	}
	n, err := strconv.ParseUint(key[len(prefix):], 10, 64)
	return n, err == nil
}

// FileRef is one file of a Pebble checkpoint and the object holding it.
type FileRef struct {
	Name   string `json:"name"`   // local file name inside the Pebble directory
	Object string `json:"object"` // object key
	Size   int64  `json:"size"`
}

// Manifest is the durable root of a partition.
type Manifest struct {
	Epoch             uint64    `json:"epoch"`
	Owner             string    `json:"owner"`
	Files             []FileRef `json:"files"` // live SSTs plus Pebble metadata files
	ReplayAfterWALSeq uint64    `json:"replay_after_wal_seq"`
	MaxCommitTS       uint64    `json:"max_commit_ts"`
	CheckpointID      string    `json:"checkpoint_id,omitempty"`
	// CreatedAt is when the manifest was written (unix µs), so point-in-time
	// restore can pick the newest state before a given time.
	CreatedAt int64 `json:"created_at,omitempty"`
}

func (m *Manifest) encode() []byte {
	b, _ := json.Marshal(m)
	return b
}

// latestManifest lists the manifests and returns the highest, or N=0 and an
// empty manifest for a partition that has never been owned.
func latestManifest(ctx context.Context, s objstore.Store, pid uint64) (uint64, *Manifest, error) {
	keys, err := s.List(ctx, manifestPrefix(pid), "")
	if err != nil {
		return 0, nil, err
	}
	var best uint64
	found := false
	for _, k := range keys {
		if n, ok := parseNumbered(k, manifestPrefix(pid)); ok && (!found || n > best) {
			best, found = n, true
		}
	}
	if !found {
		return 0, &Manifest{}, nil
	}
	b, err := s.Get(ctx, manifestKey(pid, best))
	if err != nil {
		return 0, nil, err
	}
	var m Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		return 0, nil, fmt.Errorf("manifest %d: %w", best, err)
	}
	return best, &m, nil
}
