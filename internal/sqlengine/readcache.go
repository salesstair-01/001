package sqlengine

import (
	"encoding/binary"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/salesstair-01/001/internal/kv"
)

// A statement's non-locking reads see one snapshot and exclude the
// statement's own writes, so reading the same spans twice in a statement
// returns the same rows. go-mysql-server re-runs the inner side of a nested
// join for every outer row; each run would be KV round trips. The read cache
// keeps the rows of completed span reads for the rest of the statement.
const (
	readCacheStmtBytes  = 64 << 20 // per statement
	readCacheEntryBytes = 8 << 20  // larger reads are not kept
)

type readCache struct {
	mu    sync.Mutex
	stmt  uint64
	bytes int
	m     map[string][][]byte
}

// cacheKey names a read: table, index and spans.
func cacheKey(t *Table, index *secIndex, spans []keySpan) string {
	b := binary.BigEndian.AppendUint32(nil, t.info.ID)
	if index != nil {
		b = binary.BigEndian.AppendUint32(b, index.info.ID)
	} else {
		b = binary.BigEndian.AppendUint32(b, 0)
	}
	for _, s := range spans {
		if s.point {
			b = append(b, 'p')
		} else {
			b = append(b, 'r')
		}
		b = binary.BigEndian.AppendUint32(b, uint32(len(s.start)))
		b = append(b, s.start...)
		b = binary.BigEndian.AppendUint32(b, uint32(len(s.end)))
		b = append(b, s.end...)
	}
	return string(b)
}

func (c *readCache) get(stmt uint64, key string) ([][]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stmt != stmt {
		return nil, false
	}
	v, ok := c.m[key]
	return v, ok
}

func (c *readCache) put(stmt uint64, key string, rows [][]byte, size int) {
	if size > readCacheEntryBytes {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stmt != stmt {
		c.stmt, c.bytes, c.m = stmt, 0, map[string][][]byte{}
	}
	if c.bytes+size > readCacheStmtBytes {
		return
	}
	c.m[key] = rows
	c.bytes += size
}

// cacheable says whether a read may use the statement's read cache, and
// returns the session's cache and statement.
func cacheable(ctx *sql.Context, it *rowIter) (*readCache, uint64, bool) {
	if it.opts.Lock != kv.LockNone {
		return nil, 0, false // a locking read must lock and read the latest version
	}
	s, ok := ctx.Session.(*Session)
	if !ok {
		return nil, 0, false
	}
	return &s.reads, s.Stmt(), true
}
