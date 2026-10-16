package kv

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/pebble"

	"github.com/salesstair-01/001/internal/encoding"
	"github.com/salesstair-01/001/internal/objstore"
	"github.com/salesstair-01/001/internal/wal"
)

// Point-in-time restore (slice 9) rebuilds a partition as of a wall-clock
// time into a new partition id, which a tenant can then be pointed at. It
// starts from the newest manifest written at or before the time and replays
// WAL records committed up to it. Older manifests and covered WAL segments are
// deleted GCGrace after the next checkpoint, so the restore window is about
// GCGrace (1 h by default).
//
// Every key starts with its 8-byte partition id (D2), so the restored data is
// rewritten under the new id; values never hold a partition id.

// RestorePoint is one manifest a restore can start from.
type RestorePoint struct {
	N         uint64
	CreatedAt time.Time
	WALAfter  uint64 // replay starts at the next segment
	Files     int
}

// RestorePoints lists the manifests of partition pid that carry a creation
// time, oldest first.
func RestorePoints(ctx context.Context, s objstore.Store, pid uint64) ([]RestorePoint, error) {
	keys, err := s.List(ctx, manifestPrefix(pid), "")
	if err != nil {
		return nil, err
	}
	var out []RestorePoint
	for _, k := range keys {
		n, ok := parseNumbered(k, manifestPrefix(pid))
		if !ok {
			continue
		}
		b, err := s.Get(ctx, k)
		if errors.Is(err, objstore.ErrNotFound) {
			continue // collected meanwhile
		}
		if err != nil {
			return nil, err
		}
		var m Manifest
		if err := json.Unmarshal(b, &m); err != nil {
			return nil, fmt.Errorf("manifest %d: %w", n, err)
		}
		if m.CreatedAt == 0 {
			continue
		}
		out = append(out, RestorePoint{N: n, CreatedAt: time.UnixMicro(m.CreatedAt), WALAfter: m.ReplayAfterWALSeq, Files: len(m.Files)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].N < out[j].N })
	return out, nil
}

// RestoreResult describes a finished restore.
type RestoreResult struct {
	From        RestorePoint
	Replayed    int       // WAL records applied
	LastCommit  time.Time // wall time of the last record applied (zero = none)
	MaxCommitTS uint64
}

// Restore writes partition dst as partition src was at time at. dst must
// never have been used. dataDir holds scratch files.
func Restore(ctx context.Context, s objstore.Store, src, dst uint64, at time.Time, dataDir string) (*RestoreResult, error) {
	if src == dst {
		return nil, errors.New("restore: the target partition must differ from the source")
	}
	if keys, err := s.List(ctx, manifestPrefix(dst), ""); err != nil {
		return nil, err
	} else if len(keys) > 0 {
		return nil, fmt.Errorf("restore: partition %d already exists", dst)
	}
	points, err := RestorePoints(ctx, s, src)
	if err != nil {
		return nil, err
	}
	var from *RestorePoint
	for i := range points {
		if !points[i].CreatedAt.After(at) {
			from = &points[i]
		}
	}
	if from == nil {
		if len(points) == 0 {
			return nil, fmt.Errorf("restore: partition %d has no restore points", src)
		}
		return nil, fmt.Errorf("restore: %s is before the oldest restore point of partition %d (%s)",
			at.UTC().Format(time.RFC3339), src, points[0].CreatedAt.UTC().Format(time.RFC3339))
	}
	b, err := s.Get(ctx, manifestKey(src, from.N))
	if err != nil {
		return nil, fmt.Errorf("restore: read manifest %d: %w", from.N, err)
	}
	var m Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}

	work, err := os.MkdirTemp(dataDir, fmt.Sprintf("restore-%d-", dst))
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(work)

	// 1. The source checkpoint, plus WAL records up to at.
	srcDir := filepath.Join(work, "src")
	if err := os.MkdirAll(srcDir, 0o755); err != nil {
		return nil, err
	}
	for _, f := range m.Files {
		data, err := s.Get(ctx, f.Object)
		if err != nil {
			return nil, fmt.Errorf("restore: read %s (outside the retention window?): %w", f.Object, err)
		}
		if err := os.WriteFile(filepath.Join(srcDir, f.Name), data, 0o644); err != nil {
			return nil, err
		}
	}
	opts := func() *pebble.Options {
		return &pebble.Options{Comparer: pebbleComparer, DisableWAL: true, Logger: quietLogger{}}
	}
	sdb, err := pebble.Open(srcDir, opts())
	if err != nil {
		return nil, fmt.Errorf("restore: open checkpoint: %w", err)
	}
	defer sdb.Close()
	res := &RestoreResult{From: *from, MaxCommitTS: m.MaxCommitTS}
	seq := m.ReplayAfterWALSeq + 1
	for done := false; !done; seq++ {
		data, err := s.Get(ctx, walKey(src, seq))
		if errors.Is(err, objstore.ErrNotFound) {
			if err := checkWALEnd(ctx, s, src, seq); err != nil {
				return nil, err
			}
			break
		}
		if err != nil {
			return nil, err
		}
		segm, err := wal.Decode(data)
		if err != nil {
			return nil, fmt.Errorf("restore: wal %d: %w", seq, err)
		}
		batch := sdb.NewBatch()
		for _, r := range segm.Records {
			if time.UnixMicro(r.WallMicros).After(at) {
				done = true
				break
			}
			for _, w := range r.Writes {
				if err := batch.Set(encoding.MVCCKey(w.Key, r.CommitTS), w.Value, nil); err != nil {
					return nil, err
				}
			}
			res.Replayed++
			res.LastCommit = time.UnixMicro(r.WallMicros)
			if r.CommitTS > res.MaxCommitTS {
				res.MaxCommitTS = r.CommitTS
			}
		}
		if err := batch.Commit(pebble.NoSync); err != nil {
			return nil, err
		}
	}

	// 2. Copy every version under the new partition id.
	dstDir := filepath.Join(work, "dst")
	ddb, err := pebble.Open(dstDir, opts())
	if err != nil {
		return nil, err
	}
	defer ddb.Close()
	oldP, newP := encoding.PartitionPrefix(src), encoding.PartitionPrefix(dst)
	it, err := sdb.NewIter(nil)
	if err != nil {
		return nil, err
	}
	batch := ddb.NewBatch()
	for ok := it.First(); ok; ok = it.Next() {
		k := it.Key()
		if !bytes.HasPrefix(k, oldP) {
			it.Close()
			return nil, fmt.Errorf("restore: key %x is not in partition %d", k, src)
		}
		nk := append(append([]byte(nil), newP...), k[len(oldP):]...)
		if err := batch.Set(nk, it.Value(), nil); err != nil {
			it.Close()
			return nil, err
		}
		if batch.Len() > 4<<20 {
			if err := batch.Commit(pebble.NoSync); err != nil {
				it.Close()
				return nil, err
			}
			batch = ddb.NewBatch()
		}
	}
	if err := it.Close(); err != nil {
		return nil, err
	}
	if err := batch.Commit(pebble.NoSync); err != nil {
		return nil, err
	}
	if err := ddb.Flush(); err != nil {
		return nil, err
	}

	// 3. Upload a checkpoint of it as the new partition's first manifest.
	ckptID := newID()
	ckpt := filepath.Join(work, "ckpt")
	if err := ddb.Checkpoint(ckpt); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(ckpt)
	if err != nil {
		return nil, err
	}
	var files []FileRef
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		data, err := os.ReadFile(filepath.Join(ckpt, name))
		if err != nil {
			return nil, err
		}
		obj := metaKey(dst, ckptID, name)
		if strings.HasSuffix(name, ".sst") {
			obj = sstKey(dst, newID())
		}
		if err := putWithRetry(ctx, s, obj, data, time.Minute); err != nil {
			return nil, fmt.Errorf("restore: upload %s: %w", name, err)
		}
		files = append(files, FileRef{Name: name, Object: obj, Size: int64(len(data))})
	}
	nm := Manifest{Owner: "restore", Files: files, MaxCommitTS: res.MaxCommitTS, CheckpointID: ckptID, CreatedAt: time.Now().UnixMicro()}
	if err := s.PutIfAbsent(ctx, manifestKey(dst, 1), nm.encode()); err != nil {
		return nil, fmt.Errorf("restore: write manifest of partition %d: %w", dst, err)
	}
	return res, nil
}

// checkWALEnd fails if WAL segments exist past a missing one: the records in
// between are gone, so the restore could not be exact.
func checkWALEnd(ctx context.Context, s objstore.Store, pid, missing uint64) error {
	keys, err := s.List(ctx, walPrefix(pid), "")
	if err != nil {
		return err
	}
	for _, k := range keys {
		if n, ok := parseNumbered(k, walPrefix(pid)); ok && n > missing {
			return fmt.Errorf("restore: WAL segment %d of partition %d is gone (outside the retention window)", missing, pid)
		}
	}
	return nil
}
