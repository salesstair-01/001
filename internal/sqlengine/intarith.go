package sqlengine

import (
	"math"
	"math/big"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/vt/sqlparser"
)

// go-mysql-server adds, subtracts and multiplies integers in Go's fixed-width
// arithmetic: results wrap around (9223372036854775808 + 1 is
// -9223372036854775808), and an unsigned operand with a signed one gives a
// signed result. MySQL computes an unsigned result when either operand is
// unsigned, and fails with 1690 when the result is out of range (issue #16).
// A post-analyze rule replaces integer +, - and * with an expression that
// does so, CAST(... AS SIGNED / UNSIGNED) where go-mysql-server saturates at
// 2^63, and comparisons of a signed and an unsigned integer, which
// go-mysql-server makes in float64 (18446744073709551615 = 18446744073709551614
// was true).

const intArithID analyzer.RuleId = 10005

func intArith(ctx *sql.Context, a *analyzer.Analyzer, n sql.Node, scope *plan.Scope, sel analyzer.RuleSelector, qf *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	return transform.NodeExprsWithOpaque(n, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
		switch e := e.(type) {
		case *expression.Arithmetic:
			if mysqlIntArith(e) {
				return &checkedArith{e}, transform.NewTree, nil
			}
		case *expression.Equals, *expression.NullSafeEquals, *expression.GreaterThan, *expression.LessThan,
			*expression.GreaterThanOrEqual, *expression.LessThanOrEqual:
			if c := e.(expression.Comparer); mixedIntCompare(c) {
				return &exactCmp{c}, transform.NewTree, nil
			}
		case *expression.Convert:
			if to, _ := convertTarget(e); to == expression.ConvertToSigned || to == expression.ConvertToUnsigned {
				return &intCast{Convert: e, signed: to == expression.ConvertToSigned}, transform.NewTree, nil
			}
		case *plan.Subquery:
			q, same, err := intArith(ctx, a, e.Query, scope, sel, qf)
			if err != nil || same {
				return e, transform.SameTree, err
			}
			return e.WithQuery(q), transform.NewTree, nil
		}
		return e, transform.SameTree, nil
	})
}

func mysqlIntArith(e *expression.Arithmetic) bool {
	switch strings.ToLower(e.Op) {
	case sqlparser.PlusStr, sqlparser.MinusStr, sqlparser.MultStr:
	default:
		return false
	}
	return isInt(e.LeftChild.Type()) && isInt(e.RightChild.Type())
}

func isInt(t sql.Type) bool {
	return types.IsInteger(t) && !types.IsYear(t)
}

// checkedArith is an integer +, - or * with MySQL's result type and range.
type checkedArith struct{ *expression.Arithmetic }

func (c *checkedArith) unsigned() bool {
	return unsignedOperand(c.LeftChild) || unsignedOperand(c.RightChild)
}

// unsignedOperand reports whether e is unsigned in MySQL's arithmetic. An
// integer literal is signed unless it is above 2^63-1 (go-mysql-server
// types 4294967295 as an unsigned 32-bit literal).
func unsignedOperand(e sql.Expression) bool {
	if l, ok := e.(*expression.Literal); ok {
		v, isU := l.Value().(uint64)
		return isU && v > math.MaxInt64
	}
	return types.IsUnsigned(e.Type())
}

func (c *checkedArith) Type() sql.Type {
	if c.unsigned() {
		return types.Uint64
	}
	return types.Int64
}

var (
	minInt64  = big.NewInt(math.MinInt64)
	maxInt64  = big.NewInt(math.MaxInt64)
	maxUint64 = new(big.Int).SetUint64(math.MaxUint64)
)

func (c *checkedArith) Eval(ctx *sql.Context, row sql.Row) (any, error) {
	l, err := c.LeftChild.Eval(ctx, row)
	if err != nil || l == nil {
		return nil, err
	}
	r, err := c.RightChild.Eval(ctx, row)
	if err != nil || r == nil {
		return nil, err
	}
	x, okL := bigInt(l)
	y, okR := bigInt(r)
	if !okL || !okR {
		return c.Arithmetic.Eval(ctx, row)
	}
	switch strings.ToLower(c.Op) {
	case sqlparser.PlusStr:
		x.Add(x, y)
	case sqlparser.MinusStr:
		x.Sub(x, y)
	default:
		x.Mul(x, y)
	}
	if c.unsigned() {
		if x.Sign() < 0 || x.Cmp(maxUint64) > 0 {
			return nil, outOfRange("BIGINT UNSIGNED", c)
		}
		return x.Uint64(), nil
	}
	if x.Cmp(minInt64) < 0 || x.Cmp(maxInt64) > 0 {
		return nil, outOfRange("BIGINT", c)
	}
	return x.Int64(), nil
}

func (c *checkedArith) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	e, err := c.Arithmetic.WithChildren(children...)
	if err != nil {
		return nil, err
	}
	if a, ok := e.(*expression.Arithmetic); ok && mysqlIntArith(a) {
		return &checkedArith{a}, nil
	}
	return e, nil
}

func outOfRange(typ string, e sql.Expression) error {
	return mysql.NewSQLError(1690, "22003", "%s value is out of range in '%s'", typ, e.String())
}

func bigInt(v any) (*big.Int, bool) {
	switch v := v.(type) {
	case int8:
		return big.NewInt(int64(v)), true
	case int16:
		return big.NewInt(int64(v)), true
	case int32:
		return big.NewInt(int64(v)), true
	case int64:
		return big.NewInt(v), true
	case int:
		return big.NewInt(int64(v)), true
	case uint8:
		return new(big.Int).SetUint64(uint64(v)), true
	case uint16:
		return new(big.Int).SetUint64(uint64(v)), true
	case uint32:
		return new(big.Int).SetUint64(uint64(v)), true
	case uint64:
		return new(big.Int).SetUint64(v), true
	case uint:
		return new(big.Int).SetUint64(uint64(v)), true
	}
	return nil, false
}

// intCast is CAST(x AS SIGNED / UNSIGNED) with MySQL's 64-bit results where
// go-mysql-server's differ: an unsigned value above 2^63-1 cast to SIGNED
// wraps around, and a string converts as MySQL converts it (stringToInt).
type intCast struct {
	*expression.Convert
	signed bool
}

func (c *intCast) Eval(ctx *sql.Context, row sql.Row) (any, error) {
	v, err := c.Child.Eval(ctx, row)
	if err != nil || v == nil {
		return nil, err
	}
	switch x := v.(type) {
	case uint64:
		if c.signed {
			return int64(x), nil
		}
	case string:
		return stringToInt(ctx, x, c.signed), nil
	case []byte:
		return stringToInt(ctx, string(x), c.signed), nil
	}
	return c.Convert.Eval(ctx, row)
}

func (c *intCast) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	e, err := c.Convert.WithChildren(children...)
	if err != nil {
		return nil, err
	}
	if cv, ok := e.(*expression.Convert); ok {
		return &intCast{Convert: cv, signed: c.signed}, nil
	}
	return e, nil
}

// stringToInt converts a string as MySQL's CAST to an integer does: leading
// whitespace and a sign, then the integer prefix; anything after it but
// whitespace, or no digits at all, warns 1292. Above 2^64-1 it saturates (as
// SIGNED, 2^64-1 reads as -1), below -2^63 too; a negative value cast to
// UNSIGNED is its two's complement, with warning 1105.
func stringToInt(ctx *sql.Context, s string, signed bool) any {
	truncated := func() { ctx.Warn(1292, "Truncated incorrect INTEGER value: '%s'", s) }
	rest := strings.TrimLeft(s, " \t\n\r")
	neg := false
	if rest != "" && (rest[0] == '-' || rest[0] == '+') {
		neg = rest[0] == '-'
		rest = rest[1:]
	}
	end := 0
	for end < len(rest) && rest[end] >= '0' && rest[end] <= '9' {
		end++
	}
	n, overflow := new(big.Int), false
	if end > 0 {
		n.SetString(rest[:end], 10)
	}
	if neg {
		n.Neg(n)
	}
	switch {
	case n.Cmp(maxUint64) > 0:
		n.Set(maxUint64)
		overflow = true
	case n.Cmp(minInt64) < 0:
		n.Set(minInt64)
		overflow = true
	}
	var out any
	if n.Sign() < 0 {
		if signed {
			out = n.Int64()
		} else {
			ctx.Warn(1105, "Cast to unsigned converted negative integer to its positive complement")
			out = uint64(n.Int64())
		}
	} else if signed {
		out = int64(n.Uint64())
	} else {
		out = n.Uint64()
	}
	if overflow || end == 0 || strings.TrimRight(rest[end:], " \t\n\r") != "" {
		truncated()
	}
	return out
}

// mixedIntCompare reports whether a comparison is one go-mysql-server makes
// in float64 that MySQL makes exactly, with a literal on one side (so as not
// to change join conditions the planner chose): a signed integer with an
// unsigned one, or an integer with a numeric string literal (MySQL folds
// colA < '18446744073709551616' to an exact comparison).
func mixedIntCompare(c expression.Comparer) bool {
	l, r := c.Left(), c.Right()
	litL, okL := l.(*expression.Literal)
	litR, okR := r.(*expression.Literal)
	switch {
	case isInt(l.Type()) && isInt(r.Type()):
		return types.IsUnsigned(l.Type()) != types.IsUnsigned(r.Type()) && (okL || okR)
	case isInt(l.Type()) && okR:
		return numericString(litR.Value())
	case isInt(r.Type()) && okL:
		return numericString(litL.Value())
	}
	return false
}

func numericString(v any) bool {
	s, ok := v.(string)
	if !ok {
		return false
	}
	_, ok = new(big.Rat).SetString(strings.TrimSpace(s))
	return ok
}

// exactCmp is a comparison of integers, or of an integer with a numeric
// string, made exactly.
type exactCmp struct{ expression.Comparer }

func (c *exactCmp) Compare(ctx *sql.Context, row sql.Row) (int, error) {
	l, r, ok, err := c.operands(ctx, row)
	if err != nil || !ok {
		if err == nil {
			return c.Comparer.Compare(ctx, row)
		}
		return 0, err
	}
	return l.Cmp(r), nil
}

func (c *exactCmp) operands(ctx *sql.Context, row sql.Row) (*big.Rat, *big.Rat, bool, error) {
	lv, err := c.Left().Eval(ctx, row)
	if err != nil {
		return nil, nil, false, err
	}
	rv, err := c.Right().Eval(ctx, row)
	if err != nil {
		return nil, nil, false, err
	}
	l, okL := exactNumber(lv)
	r, okR := exactNumber(rv)
	return l, r, okL && okR, nil
}

func exactNumber(v any) (*big.Rat, bool) {
	if s, ok := v.(string); ok {
		return new(big.Rat).SetString(strings.TrimSpace(s))
	}
	i, ok := bigInt(v)
	if !ok {
		return nil, false
	}
	return new(big.Rat).SetInt(i), true
}

func (c *exactCmp) Eval(ctx *sql.Context, row sql.Row) (any, error) {
	l, r, ok, err := c.operands(ctx, row)
	if err != nil || !ok {
		if err != nil {
			return nil, err
		}
		return c.Comparer.Eval(ctx, row) // NULLs, or values that aren't integers
	}
	cmp := l.Cmp(r)
	switch c.Comparer.(type) {
	case *expression.Equals, *expression.NullSafeEquals:
		return cmp == 0, nil
	case *expression.GreaterThan:
		return cmp > 0, nil
	case *expression.LessThan:
		return cmp < 0, nil
	case *expression.GreaterThanOrEqual:
		return cmp >= 0, nil
	default:
		return cmp <= 0, nil
	}
}

func (c *exactCmp) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	e, err := c.Comparer.WithChildren(children...)
	if err != nil {
		return nil, err
	}
	if cc, ok := e.(expression.Comparer); ok && mixedIntCompare(cc) {
		return &exactCmp{cc}, nil
	}
	return e, nil
}
