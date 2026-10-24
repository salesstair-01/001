package kv

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/v2/objstorage/objstorageprovider/remoteobjcat"
	"github.com/cockroachdb/pebble/v2/objstorage/remote"
	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/salesstair-01/001/internal/objstore"
)

// Remote SSTs (spike): Pebble v2 writes the SSTs of flushes and compactions
// straight to the object store and reads them back lazily, through a local
// cache, so a wake does not download them. Three things keep this inside
// the durability rules of D4:
//
//   - Fencing. Every owner creates objects under its own epoch,
//     p/<id>/rsst/e<epoch>/..., carried as the Pebble locator stored with
//     each object; epochs are unique because manifests are put-if-absent.
//     A fenced owner can keep writing, but only under its own epoch, and
//     uploads are put-if-absent too.
//   - Deletion. Pebble deletes a remote object as soon as it leaves its own
//     LSM, which can be before any manifest stops listing it. The adapter
//     never deletes: it reports the object as obsolete and the owner's
//     manifest-driven GC removes it after the grace period. Pebble's
//     reference-marker objects are not written at all.
//   - Listing. A manifest lists the live remote objects next to the local
//     files, so restore and GC know about them.

// remoteConfig puts an engine's SSTs on the object store.
type remoteConfig struct {
	store     objstore.Store
	prefix    string // p/<id>/rsst/
	creatorID uint64 // non-zero, fixed for the partition
	// epoch names the locator of the objects this engine creates; 0 creates
	// nothing remotely (restore reads remote objects but writes locally).
	epoch      uint64
	cacheBytes int64 // local cache of remote blocks; 0 = none
	lowerOnly  bool  // only L5 and L6 remote; L0–L4 stay local (uploaded at checkpoints)
	sizes      map[string]int64
	onCreate   func(RemoteRef)
	onObsolete func(object string)
	timeout    time.Duration
}

// RemoteRef is one remote SST of a manifest.
type RemoteRef struct {
	FileNum uint64 `json:"file_num"`
	Object  string `json:"object"`
	Size    int64  `json:"size"`
}

func remoteLocator(epoch uint64) remote.Locator { return remote.Locator(fmt.Sprintf("e%d", epoch)) }

func (c *remoteConfig) CreateStorage(locator remote.Locator) (remote.Storage, error) {
	if !strings.HasPrefix(string(locator), "e") {
		return nil, fmt.Errorf("remote storage: unknown locator %q", locator)
	}
	return &remoteStorage{cfg: c, keyPrefix: c.prefix + string(locator) + "/"}, nil
}

type remoteStorage struct {
	cfg       *remoteConfig
	keyPrefix string
	mu        sync.Mutex
}

func (s *remoteStorage) ctx() (context.Context, context.CancelFunc) {
	t := s.cfg.timeout
	if t == 0 {
		t = time.Minute
	}
	return context.WithTimeout(context.Background(), t)
}

// isRef reports a Pebble reference-marker name (<object>.ref.<creator>.<filenum>).
func isRef(name string) bool { return strings.Contains(name, ".ref.") }

func (s *remoteStorage) ReadObject(ctx context.Context, name string) (remote.ObjectReader, int64, error) {
	key := s.keyPrefix + name
	size, err := s.size(ctx, key)
	if err != nil {
		return nil, 0, err
	}
	return &remoteReader{store: s.cfg.store, key: key}, size, nil
}

func (s *remoteStorage) size(ctx context.Context, key string) (int64, error) {
	s.mu.Lock()
	n, ok := s.cfg.sizes[key]
	s.mu.Unlock()
	if ok {
		return n, nil
	}
	n, err := s.cfg.store.Size(ctx, key)
	if err != nil {
		return 0, err
	}
	s.mu.Lock()
	s.cfg.sizes[key] = n
	s.mu.Unlock()
	return n, nil
}

func (s *remoteStorage) CreateObject(name string) (io.WriteCloser, error) {
	if isRef(name) {
		return discard{}, nil
	}
	return &remoteWriter{s: s, name: name}, nil
}

func (s *remoteStorage) List(prefix, delimiter string) ([]string, error) {
	if isRef(prefix) {
		return nil, nil // reference markers are never written
	}
	ctx, cancel := s.ctx()
	defer cancel()
	keys, err := s.cfg.store.List(ctx, s.keyPrefix+prefix, "")
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	var out []string
	for _, k := range keys {
		r := strings.TrimPrefix(k, s.keyPrefix+prefix)
		if delimiter != "" {
			if i := strings.Index(r, delimiter); i >= 0 {
				r = r[:i]
			}
		}
		if !seen[r] {
			seen[r] = true
			out = append(out, r)
		}
	}
	return out, nil
}

// Delete never deletes; see the package comment above.
func (s *remoteStorage) Delete(name string) error {
	if !isRef(name) && s.cfg.onObsolete != nil {
		s.cfg.onObsolete(s.keyPrefix + name)
	}
	return nil
}

func (s *remoteStorage) Size(name string) (int64, error) {
	if isRef(name) {
		// Markers are never written; to Pebble every object stays
		// referenced until our GC removes it. (Pebble checks markers on
		// open in invariant builds, -race included.)
		return 0, nil
	}
	ctx, cancel := s.ctx()
	defer cancel()
	return s.size(ctx, s.keyPrefix+name)
}

func (s *remoteStorage) IsNotExistError(err error) bool { return errors.Is(err, objstore.ErrNotFound) }

func (s *remoteStorage) Close() error { return nil }

// remoteWriter buffers an SST and uploads it put-if-absent on Close.
type remoteWriter struct {
	s    *remoteStorage
	name string
	buf  bytes.Buffer
	done bool
}

func (w *remoteWriter) Write(p []byte) (int, error) { return w.buf.Write(p) }

func (w *remoteWriter) Close() error {
	if w.done {
		return nil
	}
	w.done = true
	key := w.s.keyPrefix + w.name
	ctx, cancel := w.s.ctx()
	defer cancel()
	data := w.buf.Bytes()
	if err := putWithRetry(ctx, w.s.cfg.store, key, data, w.s.cfg.timeout); err != nil {
		if !errors.Is(err, objstore.ErrPreconditionFailed) {
			return err
		}
		// A retry whose first attempt landed: the same bytes are fine,
		// anything else means another writer used our epoch's name.
		got, gerr := w.s.cfg.store.Get(ctx, key)
		if gerr != nil || !bytes.Equal(got, data) {
			return fmt.Errorf("remote storage: %s exists with other content", key)
		}
	}
	w.s.mu.Lock()
	w.s.cfg.sizes[key] = int64(len(data))
	w.s.mu.Unlock()
	if w.s.cfg.onCreate != nil {
		if fn, ok := remoteFileNum(w.name); ok {
			w.s.cfg.onCreate(RemoteRef{FileNum: fn, Object: key, Size: int64(len(data))})
		}
	}
	return nil
}

// remoteFileNum parses the file number from Pebble's <hash>-<creator>-<filenum>.sst.
func remoteFileNum(name string) (uint64, bool) {
	base := name
	if i := strings.IndexByte(base, '.'); i >= 0 {
		base = base[:i]
	}
	parts := strings.Split(base, "-")
	if len(parts) != 3 {
		return 0, false
	}
	n, err := strconv.ParseUint(parts[2], 10, 64)
	return n, err == nil
}

type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }
func (discard) Close() error                { return nil }

type remoteReader struct {
	store objstore.Store
	key   string
}

func (r *remoteReader) ReadAt(ctx context.Context, p []byte, off int64) error {
	b, err := r.store.GetRange(ctx, r.key, off, int64(len(p)))
	if err != nil {
		return err
	}
	copy(p, b)
	return nil
}

func (r *remoteReader) Close() error { return nil }

// fixCatalogMarker works around a Pebble v2.1.7 bug in reopening a
// checkpoint that has remote objects. Catalog.Checkpoint copies the catalog
// file under its live name (REMOTE-OBJ-CATALOG-000002) but starts the
// checkpoint's marker at iteration 1. The first catalog rewrite after open
// then names its new file after iteration 2, creates it over the current
// file, and removes the "previous" file, which is the same one, leaving the
// marker pointing at nothing. Renaming the file to match the marker's
// iteration before open avoids it.
func fixCatalogMarker(dir string) error {
	const markerPrefix = "marker.remote-obj-catalog."
	ents, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range ents {
		name := e.Name()
		if !strings.HasPrefix(name, markerPrefix) {
			continue
		}
		iter, file, ok := strings.Cut(strings.TrimPrefix(name, markerPrefix), ".")
		if !ok {
			return fmt.Errorf("remote catalog marker %q", name)
		}
		want := "REMOTE-OBJ-CATALOG-" + iter
		if file == want {
			return nil
		}
		if err := os.Rename(filepath.Join(dir, file), filepath.Join(dir, want)); err != nil {
			return err
		}
		return os.Rename(filepath.Join(dir, name), filepath.Join(dir, markerPrefix+iter+"."+want))
	}
	return nil
}

// checkpointRemoteFiles returns the file numbers of the remote objects a
// Pebble checkpoint directory references.
func checkpointRemoteFiles(dir string) ([]uint64, error) {
	cat, contents, err := remoteobjcat.Open(vfs.Default, dir)
	if err != nil {
		return nil, err
	}
	defer cat.Close()
	out := make([]uint64, 0, len(contents.Objects))
	for _, o := range contents.Objects {
		out = append(out, uint64(o.FileNum))
	}
	return out, nil
}
