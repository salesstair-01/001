// s2z patch: CUME_DIST() and NTH_VALUE(expr, n), which go-mysql-server
// 0.20 lacks (issue #26).

package aggregation

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

// CumeDist is the share of the partition's rows up to and including the
// current row's peer group.
type CumeDist struct {
	partitionStart, partitionEnd int
	orderBy                      []sql.Expression
}

var _ sql.WindowFunction = (*CumeDist)(nil)

func NewCumeDist(orderBy []sql.Expression) *CumeDist {
	return &CumeDist{partitionStart: -1, partitionEnd: -1, orderBy: orderBy}
}

func (a *CumeDist) WithWindow(w *sql.WindowDefinition) (sql.WindowFunction, error) {
	na := *a
	na.orderBy = w.OrderBy.ToExpressions()
	return &na, nil
}

func (a *CumeDist) Dispose() {}

func (a *CumeDist) DefaultFramer() sql.WindowFramer { return NewPeerGroupFramer(a.orderBy) }

func (a *CumeDist) StartPartition(ctx *sql.Context, interval sql.WindowInterval, buffer sql.WindowBuffer) error {
	a.partitionStart, a.partitionEnd = interval.Start, interval.End
	return nil
}

func (a *CumeDist) NewSlidingFrameInterval(added, dropped sql.WindowInterval) {
	panic("sliding window interface not implemented yet")
}

// Compute: interval is the current row's peer group.
func (a *CumeDist) Compute(ctx *sql.Context, interval sql.WindowInterval, buf sql.WindowBuffer) interface{} {
	if interval.End-interval.Start < 1 || a.partitionEnd <= a.partitionStart {
		return nil
	}
	return float64(interval.End-a.partitionStart) / float64(a.partitionEnd-a.partitionStart)
}

// NthAgg is the value of expr in the frame's nth row, NULL if the frame has
// fewer rows. Without a frame clause the frame is MySQL's default: the
// partition's rows up to and including the current row's peers (all of it
// without ORDER BY).
type NthAgg struct {
	partitionStart, partitionEnd int
	expr                         sql.Expression
	n                            int64
	framer                       sql.WindowFramer
	orderBy                      []sql.Expression
}

var _ sql.WindowFunction = (*NthAgg)(nil)

func NewNthAgg(e sql.Expression, n int64) *NthAgg {
	return &NthAgg{expr: e, n: n}
}

func (a *NthAgg) WithWindow(w *sql.WindowDefinition) (sql.WindowFunction, error) {
	na := *a
	na.orderBy = w.OrderBy.ToExpressions()
	if w.Frame != nil {
		framer, err := w.Frame.NewFramer(w)
		if err != nil {
			return nil, err
		}
		na.framer = framer
	}
	return &na, nil
}

func (a *NthAgg) Dispose() { expression.Dispose(a.expr) }

func (a *NthAgg) DefaultFramer() sql.WindowFramer {
	if a.framer != nil {
		return a.framer
	}
	return NewPeerGroupFramer(a.orderBy)
}

func (a *NthAgg) StartPartition(ctx *sql.Context, interval sql.WindowInterval, buffer sql.WindowBuffer) error {
	a.Dispose()
	a.partitionStart, a.partitionEnd = interval.Start, interval.End
	return nil
}

func (a *NthAgg) NewSlidingFrameInterval(added, dropped sql.WindowInterval) {
	panic("sliding window interface not implemented yet")
}

func (a *NthAgg) Compute(ctx *sql.Context, interval sql.WindowInterval, buffer sql.WindowBuffer) interface{} {
	start := interval.Start
	if a.framer == nil {
		start = a.partitionStart // the peer group's interval ends the default frame
	}
	idx := start + int(a.n) - 1
	if interval.End-interval.Start < 1 || idx >= interval.End || idx < start {
		return nil
	}
	v, err := a.expr.Eval(ctx, buffer[idx])
	if err != nil {
		return err
	}
	return v
}
