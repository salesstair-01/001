package sqlengine

import (
	"io"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

// A stored program's CONTINUE handler resumes after the statement that
// raised the condition. go-mysql-server's block instead calls Next on the
// statement's iterator again after the handler runs, and SIGNAL's iterator
// raises again, so CALL looped forever (MySQL's sp-error.test, issue #19).
// The builder wraps a statement's iterator so that after an error it is
// finished, as a statement is once it has failed.

var stickyBuilding sync.Map // nodes being built under a stickyIter

func isStatement(n sql.Node) bool {
	switch n.(type) {
	case *plan.Signal:
		return true
	}
	return false
}

// buildSticky builds n wrapped in a stickyIter; nil when n is not a
// statement, or is being built by buildSticky now.
func (o *exactUpdates) buildSticky(ctx *sql.Context, n sql.Node, row sql.Row) (sql.RowIter, bool, error) {
	if !isStatement(n) {
		return nil, false, nil
	}
	if _, inner := stickyBuilding.LoadAndDelete(n); inner {
		return nil, false, nil
	}
	stickyBuilding.Store(n, struct{}{})
	it, err := o.base.Build(ctx, n, row)
	stickyBuilding.Delete(n)
	if err != nil {
		return nil, true, err
	}
	return &stickyIter{RowIter: it}, true, nil
}

type stickyIter struct {
	sql.RowIter
	failed bool
}

func (it *stickyIter) Next(ctx *sql.Context) (sql.Row, error) {
	if it.failed {
		return nil, io.EOF
	}
	r, err := it.RowIter.Next(ctx)
	if err != nil && err != io.EOF {
		it.failed = true
	}
	return r, err
}
