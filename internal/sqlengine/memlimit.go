package sqlengine

import (
	"runtime/metrics"
	"sync/atomic"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/mysql"

	s2zmetrics "github.com/salesstair-01/001/internal/metrics"
)

// A query engine's memory budget (issue #18). go-mysql-server's sorts,
// GROUP BY, DISTINCT, hash joins and cached subqueries buffer their rows in
// the memory manager's caches, which refuse a row once the reporter's used
// memory passes its maximum; the statement then fails (1041, as MySQL when
// out of memory) instead of growing the process until the kernel kills it,
// with every tenant's sessions. The maximum is SetMemoryLimit's (sqld
// -max-memory); 0, the default, is none. The stock reporter reads
// runtime.MemStats, which stops the world, on every row; this one samples
// the heap every 20 ms, and again whenever the last sample was over the
// limit.

var memLimit atomic.Uint64

// SetMemoryLimit sets the heap size past which statements buffering rows
// fail; 0 for none.
func SetMemoryLimit(bytes uint64) { memLimit.Store(bytes) }

type memReporter struct {
	at   atomic.Int64 // last sample, unix nanoseconds
	used atomic.Uint64
}

var heapReporter = &memReporter{}

func (r *memReporter) MaxMemory() uint64 { return memLimit.Load() }

func (r *memReporter) UsedMemory() uint64 {
	now := time.Now().UnixNano()
	used := r.used.Load()
	if now-r.at.Load() < int64(20*time.Millisecond) && used < memLimit.Load() {
		return used
	}
	s := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
	metrics.Read(s)
	used = s[0].Value.Uint64()
	r.used.Store(used)
	r.at.Store(now)
	return used
}

var (
	errOutOfMemory = mysql.NewSQLError(1041, "HY000", "Out of memory; the statement needs more than the query engine's memory budget")

	memRejected = s2zmetrics.Default.NewCounter("s2z_sqld_memory_rejections_total",
		"Statements that failed because the query engine's heap passed -max-memory.")
)

func init() {
	s2zmetrics.Default.NewGaugeFunc("s2z_sqld_heap_bytes", "Heap in use, as the memory budget measures it.",
		func() []s2zmetrics.Sample {
			return []s2zmetrics.Sample{{Value: float64(heapReporter.UsedMemory())}}
		})
	s2zmetrics.Default.NewGaugeFunc("s2z_sqld_memory_limit_bytes", "The memory budget (-max-memory); 0 for none.",
		func() []s2zmetrics.Sample { return []s2zmetrics.Sample{{Value: float64(memLimit.Load())}} })
}

func newMemoryManager() *sql.MemoryManager { return sql.NewMemoryManager(heapReporter) }
