package sqlengine

import (
	"context"
	"regexp"
	"strconv"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

// max_execution_time (issue #18): a SELECT runs at most @@max_execution_time
// milliseconds, or the MAX_EXECUTION_TIME(n) optimizer hint's, as in MySQL;
// go-mysql-server ignores both. The engine builds each statement's
// iterators with rootBuilder, which puts the deadline on the statement's
// context; its iterators then stop with the context's error, reported as
// MySQL's 3024 (errcodes.go). The handler gives each query a context of
// its own, so the timer ends with the query.

type rootBuilder struct {
	sql.NodeExecBuilder
}

func (b rootBuilder) Build(ctx *sql.Context, n sql.Node, row sql.Row) (sql.RowIter, error) {
	if _, ok := ctx.Deadline(); !ok && plan.NodeRepresentsSelect(n) {
		if d := executionTime(ctx); d > 0 {
			var cancel context.CancelFunc
			ctx.Context, cancel = context.WithTimeout(ctx.Context, d)
			context.AfterFunc(ctx.Context, cancel)
		}
	}
	if err := checkFuncWrites(n); err != nil {
		return nil, err // storedfunc.go
	}
	return b.NodeExecBuilder.Build(ctx, n, row)
}

var reExecHint = regexp.MustCompile(`(?i)/\*\+[^*]*\bMAX_EXECUTION_TIME\s*\(\s*(\d+)\s*\)`)

// executionTime is the statement's limit; 0 for none.
func executionTime(ctx *sql.Context) time.Duration {
	if m := reExecHint.FindStringSubmatch(ctx.Query()); m != nil {
		ms, _ := strconv.ParseInt(m[1], 10, 64)
		return time.Duration(ms) * time.Millisecond
	}
	v, err := ctx.GetSessionVariable(ctx, "max_execution_time")
	if err != nil {
		return 0
	}
	ms, err := fnInt64(ctx, v)
	if err != nil || ms <= 0 {
		return 0
	}
	return time.Duration(ms) * time.Millisecond
}

// pollCancel returns the context's error every 256th call: most of
// go-mysql-server's iterators never look at the context, so the ones that
// produce rows (KV scans, recursive CTEs) and each node build check it, to
// end a statement past its max_execution_time or killed (KILL QUERY).
func pollCancel(ctx *sql.Context, n *uint32) error {
	if *n++; *n&255 != 0 {
		return nil
	}
	return ctx.Err()
}
