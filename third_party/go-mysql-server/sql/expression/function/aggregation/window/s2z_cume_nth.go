// s2z patch: CUME_DIST() and NTH_VALUE(expr, n) (issue #26).

package window

import (
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function/aggregation"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/mysql"
)

type CumeDist struct {
	window *sql.WindowDefinition
	id     sql.ColumnId
}

var _ sql.FunctionExpression = (*CumeDist)(nil)
var _ sql.WindowAggregation = (*CumeDist)(nil)
var _ sql.WindowAdaptableExpression = (*CumeDist)(nil)

func NewCumeDist() sql.Expression { return &CumeDist{} }

func (c *CumeDist) Id() sql.ColumnId { return c.id }
func (c *CumeDist) WithId(id sql.ColumnId) sql.IdExpression {
	ret := *c
	ret.id = id
	return &ret
}
func (c *CumeDist) Description() string           { return "returns the cumulative distribution value." }
func (c *CumeDist) Window() *sql.WindowDefinition { return c.window }
func (c *CumeDist) Resolved() bool                { return windowResolved(c.window) }
func (c *CumeDist) FunctionName() string          { return "CUME_DIST" }
func (c *CumeDist) Type() sql.Type                { return types.Float64 }
func (c *CumeDist) IsNullable() bool              { return false }
func (c *CumeDist) Children() []sql.Expression    { return c.window.ToExpressions() }
func (c *CumeDist) DebugString() string           { return c.String() }
func (c *CumeDist) String() string {
	sb := strings.Builder{}
	sb.WriteString("cume_dist()")
	if c.window != nil {
		sb.WriteString(" ")
		sb.WriteString(c.window.String())
	}
	return sb.String()
}
func (c *CumeDist) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return nil, sql.ErrWindowUnsupported.New(c.FunctionName())
}
func (c *CumeDist) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	window, err := c.window.FromExpressions(children)
	if err != nil {
		return nil, err
	}
	return c.WithWindow(window), nil
}
func (c *CumeDist) WithWindow(window *sql.WindowDefinition) sql.WindowAdaptableExpression {
	nc := *c
	nc.window = window
	return &nc
}
func (c *CumeDist) NewWindowFunction() (sql.WindowFunction, error) {
	return aggregation.NewCumeDist(c.window.OrderBy.ToExpressions()), nil
}

type NthValue struct {
	window *sql.WindowDefinition
	expr   sql.Expression
	n      sql.Expression
	id     sql.ColumnId
}

var _ sql.FunctionExpression = (*NthValue)(nil)
var _ sql.WindowAggregation = (*NthValue)(nil)
var _ sql.WindowAdaptableExpression = (*NthValue)(nil)

func NewNthValue(e, n sql.Expression) sql.Expression { return &NthValue{expr: e, n: n} }

func (f *NthValue) Id() sql.ColumnId { return f.id }
func (f *NthValue) WithId(id sql.ColumnId) sql.IdExpression {
	ret := *f
	ret.id = id
	return &ret
}
func (f *NthValue) Description() string {
	return "returns value of argument from the n-th row of the window frame."
}
func (f *NthValue) Window() *sql.WindowDefinition { return f.window }
func (f *NthValue) Resolved() bool {
	return windowResolved(f.window) && f.expr.Resolved() && f.n.Resolved()
}
func (f *NthValue) FunctionName() string { return "NTH_VALUE" }
func (f *NthValue) Type() sql.Type       { return f.expr.Type() }
func (f *NthValue) IsNullable() bool     { return true }
func (f *NthValue) DebugString() string  { return f.String() }
func (f *NthValue) String() string {
	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf("nth_value(%s, %s)", f.expr, f.n))
	if f.window != nil {
		sb.WriteString(" ")
		sb.WriteString(f.window.String())
	}
	return sb.String()
}
func (f *NthValue) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return nil, sql.ErrWindowUnsupported.New(f.FunctionName())
}
func (f *NthValue) Children() []sql.Expression {
	return append(f.window.ToExpressions(), f.expr, f.n)
}
func (f *NthValue) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) < 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(f, len(children), 2)
	}
	nf := *f
	window, err := f.window.FromExpressions(children[:len(children)-2])
	if err != nil {
		return nil, err
	}
	nf.window = window
	nf.expr, nf.n = children[len(children)-2], children[len(children)-1]
	return &nf, nil
}
func (f *NthValue) WithWindow(window *sql.WindowDefinition) sql.WindowAdaptableExpression {
	nf := *f
	nf.window = window
	return &nf
}
func (f *NthValue) NewWindowFunction() (sql.WindowFunction, error) {
	v, err := f.n.Eval(nil, nil)
	if err != nil {
		return nil, err
	}
	n, ok := toPositiveInt(v)
	if !ok {
		return nil, mysql.NewSQLError(1210, "HY000", "Incorrect arguments to nth_value")
	}
	c, err := transform.Clone(f.expr)
	if err != nil {
		return nil, err
	}
	return aggregation.NewNthAgg(c, n).WithWindow(f.window)
}

func toPositiveInt(v interface{}) (int64, bool) {
	var n int64
	switch x := v.(type) {
	case int8:
		n = int64(x)
	case int16:
		n = int64(x)
	case int32:
		n = int64(x)
	case int64:
		n = x
	case uint8:
		n = int64(x)
	case uint16:
		n = int64(x)
	case uint32:
		n = int64(x)
	case uint64:
		n = int64(x)
	default:
		return 0, false
	}
	return n, n > 0
}

var _ = expression.Dispose
