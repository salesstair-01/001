package kv

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/salesstair-01/001/internal/objstore"
)

// fileCache keeps checkpoint files on local disk across restarts on the same
// host (slice 6), keyed by object name. Objects are immutable once written
// (SSTs have uuid names, metadata lives under a per-checkpoint prefix), so a
// cached copy never goes stale. Eviction is least recently used, by mtime.
type fileCache struct {
	dir string
	max int64
}

func newFileCache(dir string, max int64) *fileCache {
	if dir == "" || max < 0 {
		return nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil
	}
	return &fileCache{dir: dir, max: max}
}

func (c *fileCache) path(object string) string {
	sum := sha256.Sum256([]byte(object))
	return filepath.Join(c.dir, hex.EncodeToString(sum[:16]))
}

// install puts object at dst: from the cache when it has it, otherwise
// downloaded and added to the cache. SSTs are hard-linked (Pebble never
// changes them); other files, such as the MANIFEST Pebble appends to, are copied.
func (c *fileCache) install(ctx context.Context, s objstore.Store, object, dst string) (hit bool, err error) {
	sst := strings.HasSuffix(dst, ".sst")
	if c != nil {
		src := c.path(object)
		if _, err := os.Stat(src); err == nil {
			now := time.Now()
			_ = os.Chtimes(src, now, now)
			if sst && os.Link(src, dst) == nil {
				return true, nil
			}
			if copyFile(src, dst) == nil {
				return true, nil
			}
			// Another process sharing the cache evicted it meanwhile: download.
			_ = os.Remove(dst)
		}
	}
	b, err := s.Get(ctx, object)
	if err != nil {
		return false, err
	}
	if err := os.WriteFile(dst, b, 0o644); err != nil {
		return false, err
	}
	c.add(object, dst)
	return false, nil
}

// add copies (or for SSTs, links) a local file into the cache.
func (c *fileCache) add(object, src string) {
	if c == nil {
		return
	}
	dst := c.path(object)
	if _, err := os.Stat(dst); err == nil {
		return
	}
	tmp := dst + "." + newID() + ".tmp" // processes on one host may share the cache
	if !strings.HasSuffix(src, ".sst") || os.Link(src, tmp) != nil {
		if copyFile(src, tmp) != nil {
			_ = os.Remove(tmp)
			return
		}
	}
	_ = os.Rename(tmp, dst)
}

// evict deletes the least recently used files until the cache fits.
func (c *fileCache) evict() {
	if c == nil {
		return
	}
	entries, err := os.ReadDir(c.dir)
	if err != nil {
		return
	}
	type file struct {
		path  string
		size  int64
		mtime time.Time
	}
	var files []file
	var total int64
	for _, e := range entries {
		info, err := e.Info()
		if err != nil || e.IsDir() || strings.HasSuffix(e.Name(), ".tmp") {
			continue
		}
		files = append(files, file{filepath.Join(c.dir, e.Name()), info.Size(), info.ModTime()})
		total += info.Size()
	}
	sort.Slice(files, func(i, j int) bool { return files[i].mtime.Before(files[j].mtime) })
	for _, f := range files {
		if total <= c.max {
			return
		}
		if os.Remove(f.path) == nil {
			total -= f.size
		}
	}
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}
