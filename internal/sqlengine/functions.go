package sqlengine

import (
	"math"
	"strconv"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression/function"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// Built-in functions that replace go-mysql-server's, where its version
// panics on input MySQL accepts (found by MySQL's test suite, issue #17).
// The catalog asks the provider before its built-ins.
var overrides = map[string]sql.Function{
	"inet6_aton": sql.Function1{Name: "inet6_aton", Fn: func(e sql.Expression) sql.Expression {
		return &inet6Aton{function.NewInet6Aton(e)}
	}},
	"format": sql.FunctionN{Name: "format", Fn: func(args ...sql.Expression) (sql.Expression, error) {
		f, err := function.NewFormat(args...)
		if err != nil {
			return nil, err
		}
		return &format{f}, nil
	}},
}

var _ sql.FunctionProvider = (*Provider)(nil)

func (p *Provider) Function(_ *sql.Context, name string) (sql.Function, bool) {
	f, ok := overrides[strings.ToLower(name)]
	return f, ok
}

// inet6Aton is NULL for an argument that is not a string, as in MySQL
// (go-mysql-server asserted a string).
type inet6Aton struct{ sql.Expression }

func (f *inet6Aton) Eval(ctx *sql.Context, row sql.Row) (any, error) {
	v, err := f.Children()[0].Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	switch v.(type) {
	case string, []byte:
		return f.Expression.Eval(ctx, row)
	}
	return nil, nil
}

func (f *inet6Aton) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	e, err := f.Expression.WithChildren(children...)
	if err != nil {
		return nil, err
	}
	return &inet6Aton{e}, nil
}

// format formats NaN and infinities ('nan', 'inf' converted from strings)
// as 0, and doubles too large for go-mysql-server's decimals itself, with at
// most 30 decimals, as MySQL does (it panicked on both).
type format struct{ sql.Expression }

func (f *format) Eval(ctx *sql.Context, row sql.Row) (any, error) {
	args := f.Children()
	if len(args) != 2 && len(args) != 3 {
		return f.Expression.Eval(ctx, row)
	}
	x, err := args[0].Eval(ctx, row)
	if err != nil || x == nil {
		return nil, err
	}
	fx, _, err := types.Float64.Convert(ctx, x)
	if err != nil {
		return f.Expression.Eval(ctx, row)
	}
	v := fx.(float64)
	if !math.IsNaN(v) && !math.IsInf(v, 0) && math.Abs(v) < 1e60 {
		return f.Expression.Eval(ctx, row)
	}
	d, err := args[1].Eval(ctx, row)
	if err != nil || d == nil {
		return nil, err
	}
	fd, _, err := types.Int64.Convert(ctx, d)
	if err != nil {
		return nil, err
	}
	if math.IsNaN(v) || math.IsInf(v, 0) {
		v = 0
	}
	return formatNumber(v, int(min(max(fd.(int64), 0), 30))), nil
}

func (f *format) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	e, err := f.Expression.WithChildren(children...)
	if err != nil {
		return nil, err
	}
	return &format{e}, nil
}

// formatNumber is v with decimals digits after the point and commas between
// groups of three digits before it.
func formatNumber(v float64, decimals int) string {
	s := strconv.FormatFloat(math.Abs(v), 'f', decimals, 64)
	intPart, frac := s, ""
	if i := strings.IndexByte(s, '.'); i >= 0 {
		intPart, frac = s[:i], s[i:]
	}
	var b strings.Builder
	if v < 0 {
		b.WriteByte('-')
	}
	for i, c := range intPart {
		if i > 0 && (len(intPart)-i)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteRune(c)
	}
	b.WriteString(frac)
	return b.String()
}
