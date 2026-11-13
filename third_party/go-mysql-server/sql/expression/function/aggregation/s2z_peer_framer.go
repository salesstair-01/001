// s2z patch: MySQL's default window frame.

package aggregation

import (
	"io"

	"github.com/dolthub/go-mysql-server/sql"
)

// PeersFramer is MySQL's frame for a window with ORDER BY and no frame
// clause, RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW: the
// partition's rows up to the last peer of the current row (equal on every
// ORDER BY expression, NULLs equal to each other), in the partition's
// sorted order whatever its direction.
type PeersFramer struct {
	orderBy                      []sql.Expression
	idx                          int
	partitionStart, partitionEnd int
	frameEnd                     int
	partitionSet                 bool
}

var _ sql.WindowFramer = (*PeersFramer)(nil)

func NewPeersFramer(orderBy []sql.Expression) *PeersFramer {
	return &PeersFramer{orderBy: orderBy}
}

func (f *PeersFramer) NewFramer(interval sql.WindowInterval) (sql.WindowFramer, error) {
	return &PeersFramer{
		orderBy:        f.orderBy,
		idx:            interval.Start,
		partitionStart: interval.Start,
		partitionEnd:   interval.End,
		frameEnd:       interval.Start,
		partitionSet:   true,
	}, nil
}

func (f *PeersFramer) Next(ctx *sql.Context, buf sql.WindowBuffer) (sql.WindowInterval, error) {
	if !f.partitionSet || f.idx >= f.partitionEnd {
		return sql.WindowInterval{}, io.EOF
	}
	if f.frameEnd <= f.idx {
		end := f.idx + 1
		for end < f.partitionEnd {
			peer, err := f.peers(ctx, buf[f.idx], buf[end])
			if err != nil {
				return sql.WindowInterval{}, err
			}
			if !peer {
				break
			}
			end++
		}
		f.frameEnd = end
	}
	f.idx++
	return f.Interval()
}

func (f *PeersFramer) peers(ctx *sql.Context, a, b sql.Row) (bool, error) {
	for _, e := range f.orderBy {
		va, err := e.Eval(ctx, a)
		if err != nil {
			return false, err
		}
		vb, err := e.Eval(ctx, b)
		if err != nil {
			return false, err
		}
		if va == nil || vb == nil {
			if va != nil || vb != nil {
				return false, nil
			}
			continue
		}
		c, err := e.Type().Compare(ctx, va, vb)
		if err != nil {
			return false, err
		}
		if c != 0 {
			return false, nil
		}
	}
	return true, nil
}

func (f *PeersFramer) FirstIdx() int { return f.partitionStart }
func (f *PeersFramer) LastIdx() int  { return f.frameEnd }

func (f *PeersFramer) Interval() (sql.WindowInterval, error) {
	if !f.partitionSet {
		return sql.WindowInterval{}, ErrPartitionNotSet
	}
	return sql.WindowInterval{Start: f.partitionStart, End: f.frameEnd}, nil
}
