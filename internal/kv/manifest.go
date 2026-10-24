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
func remotePrefix(pid uint64) string      { return partitionRoot(pid) + "rsst/" }
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

// manifestFormat is the format of the engine files a manifest lists. It
// changes whenever a new build cannot open files written by an older one.
//
//	0 (absent): Pebble v1, comparer s2z.mvcc.v1 (byte-wise; issue #3)
//	2:          Pebble v2 at FormatValueSeparation, comparer s2z.mvcc.v2
const manifestFormat = 2

// Manifest is the durable root of a partition.
type Manifest struct {
	Format int       `json:"format,omitempty"`
	Epoch  uint64    `json:"epoch"`
	Owner  string    `json:"owner"`
	Files  []FileRef `json:"files"` // live SSTs plus Pebble metadata files
	// Remote lists the SSTs Pebble keeps on the object store (RemoteSSTs);
	// they are read in place, never downloaded.
	Remote            []RemoteRef `json:"remote,omitempty"`
	ReplayAfterWALSeq uint64      `json:"replay_after_wal_seq"`
	MaxCommitTS       uint64      `json:"max_commit_ts"`
	CheckpointID      string      `json:"checkpoint_id,omitempty"`
	// CreatedAt is when the manifest was written (unix µs), so point-in-time
	// restore can pick the newest state before a given time.
	CreatedAt int64 `json:"created_at,omitempty"`
}

// checkFormat refuses a manifest whose files this build cannot open. A
// manifest without files (a partition that only has WAL) has nothing to
// open, so any format passes: the WAL format has not changed.
func (m *Manifest) checkFormat() error {
	if m.Format > manifestFormat {
		return fmt.Errorf("manifest format %d is newer than this build's %d", m.Format, manifestFormat)
	}
	if m.Format < manifestFormat && len(m.Files) > 0 {
		return fmt.Errorf("manifest format %d lists engine files this build cannot open (it writes format %d); "+
			"dump the database with the old build and load it into a new one", m.Format, manifestFormat)
	}
	return nil
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
