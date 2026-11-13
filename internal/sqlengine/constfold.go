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
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/shopspring/decimal"
)

// An integer column compared with a constant that is out of the column's
// range, has a fraction, or is a string, gave wrong results: go-mysql-server
// compares in float64, and converts the constant to the column's type to
// build an index range, which saturates or fails (colA < '18446744073709551616'
// on a BIGINT UNSIGNED missed 18446744073709551615, or failed with 1366 on
// an index). MySQL folds such comparisons (MySQL's const_folding.test, see
// mtr/): out of range they are always true or false (NULL for a NULL
// column, except <=>), a fraction rounds the bound (a > 1.5 is a >= 2), and
// otherwise the constant becomes an integer of the column's type. A
// pre-analyze rule does the same, before indexes are chosen.
//
// The rule also gives INSERT ... VALUES an exact integer for a constant
// that is a whole number but not an integer literal ('254.0', 0.0E0,
// '18446744073709551615.0') going into an integer column: go-mysql-server
// converted it through float64, took 0.0 for a negative number in an
// unsigned column, and refused 2^64-1 as out of range.

const constFoldID analyzer.RuleId = 10007

func constFold(ctx *sql.Context, a *analyzer.Analyzer, n sql.Node, scope *plan.Scope, sel analyzer.RuleSelector, qf *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	n, same1, err := exactIntInserts(ctx, n)
	if err != nil {
		return nil, transform.SameTree, err
	}
	n, same2, err := transform.NodeExprsWithOpaque(n, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
		switch e := e.(type) {
		case *expression.Equals, *expression.NullSafeEquals, *expression.GreaterThan, *expression.LessThan,
			*expression.GreaterThanOrEqual, *expression.LessThanOrEqual:
			if f := foldYearCompare(ctx, e.(expression.Comparer)); f != nil {
				return f, transform.NewTree, nil
			}
			if f := foldIntCompare(ctx, e.(expression.Comparer)); f != nil {
				return f, transform.NewTree, nil
			}
			if f := foldDecimalCompare(ctx, e.(expression.Comparer)); f != nil {
				return f, transform.NewTree, nil
			}
		case *expression.InTuple:
			if f := foldYearIn(ctx, e); f != nil {
				return f, transform.NewTree, nil
			}
		case *expression.Between:
			if types.IsYear(e.Val.Type()) {
				lo, okLo := yearOperand(ctx, e.Lower)
				hi, okHi := yearOperand(ctx, e.Upper)
				if okLo || okHi {
					return expression.NewBetween(e.Val, lo, hi), transform.NewTree, nil
				}
			}
		case *plan.Subquery:
			q, same, err := constFold(ctx, a, e.Query, scope, sel, qf)
			if err != nil || same {
				return e, transform.SameTree, err
			}
			return e.WithQuery(q), transform.NewTree, nil
		}
		return e, transform.SameTree, nil
	})
	if !same1 {
		return n, transform.NewTree, err
	}
	return n, same2, err
}

type cmpOp int

const (
	opEq cmpOp = iota
	opNullSafeEq
	opGt
	opGe
	opLt
	opLe
)

func opOf(c expression.Comparer) cmpOp {
	switch c.(type) {
	case *expression.Equals:
		return opEq
	case *expression.NullSafeEquals:
		return opNullSafeEq
	case *expression.GreaterThan:
		return opGt
	case *expression.GreaterThanOrEqual:
		return opGe
	case *expression.LessThan:
		return opLt
	default:
		return opLe
	}
}

// mirror is op with its operands swapped: 5 < a is a > 5.
func (o cmpOp) mirror() cmpOp {
	switch o {
	case opGt:
		return opLt
	case opGe:
		return opLe
	case opLt:
		return opGt
	case opLe:
		return opGe
	}
	return o
}

func (o cmpOp) build(l, r sql.Expression) sql.Expression {
	switch o {
	case opEq:
		return expression.NewEquals(l, r)
	case opNullSafeEq:
		return expression.NewNullSafeEquals(l, r)
	case opGt:
		return expression.NewGreaterThan(l, r)
	case opGe:
		return expression.NewGreaterThanOrEqual(l, r)
	case opLt:
		return expression.NewLessThan(l, r)
	default:
		return expression.NewLessThanOrEqual(l, r)
	}
}

// foldIntCompare is MySQL's folding of col OP constant for an integer
// column, or nil if the comparison is left as it is.
func foldIntCompare(ctx *sql.Context, c expression.Comparer) sql.Expression {
	op := opOf(c)
	col, ok := c.Left().(*expression.GetField)
	lit, okLit := c.Right().(*expression.Literal)
	if !ok || !okLit {
		col, ok = c.Right().(*expression.GetField)
		lit, okLit = c.Left().(*expression.Literal)
		if !ok || !okLit {
			return nil
		}
		op = op.mirror()
	}
	lo, hi, isInt := intRange(col.Type())
	if !isInt {
		return nil
	}
	v, ok := constValue(lit.Value())
	if !ok {
		return nil
	}
	switch {
	case v.Cmp(hi) > 0:
		return alwaysBool(col, op, op == opLt || op == opLe)
	case v.Cmp(lo) < 0:
		return alwaysBool(col, op, op == opGt || op == opGe)
	}
	if !v.IsInt() {
		floor := new(big.Int).Quo(v.Num(), v.Denom()) // toward zero
		if v.Sign() < 0 {
			floor.Sub(floor, big.NewInt(1))
		}
		ceil := new(big.Int).Add(floor, big.NewInt(1))
		switch op {
		case opEq, opNullSafeEq:
			return alwaysBool(col, op, false)
		case opGt, opGe:
			return opGe.build(col, intLiteral(ctx, ceil, col.Type()))
		default:
			return opLe.build(col, intLiteral(ctx, floor, col.Type()))
		}
	}
	if isIntLiteral(lit) {
		return nil // already exact; mixed signedness is handled by exactCmp
	}
	return op.build(col, intLiteral(ctx, v.Num(), col.Type()))
}

// intRange is an integer type's range.
func intRange(t sql.Type) (lo, hi *big.Rat, ok bool) {
	r := func(lo int64, hi uint64) (*big.Rat, *big.Rat, bool) {
		return new(big.Rat).SetInt64(lo), new(big.Rat).SetInt(new(big.Int).SetUint64(hi)), true
	}
	switch t.Type() {
	case sqltypes.Int8:
		return r(math.MinInt8, math.MaxInt8)
	case sqltypes.Uint8:
		return r(0, math.MaxUint8)
	case sqltypes.Int16:
		return r(math.MinInt16, math.MaxInt16)
	case sqltypes.Uint16:
		return r(0, math.MaxUint16)
	case sqltypes.Int24:
		return r(-1<<23, 1<<23-1)
	case sqltypes.Uint24:
		return r(0, 1<<24-1)
	case sqltypes.Int32:
		return r(math.MinInt32, math.MaxInt32)
	case sqltypes.Uint32:
		return r(0, math.MaxUint32)
	case sqltypes.Int64:
		return r(math.MinInt64, math.MaxInt64)
	case sqltypes.Uint64:
		return r(0, math.MaxUint64)
	}
	return nil, nil, false
}

// constValue is a numeric constant's exact value: an integer, a float, a
// decimal, or a string that is entirely a number.
func constValue(v any) (*big.Rat, bool) {
	switch v := v.(type) {
	case float32:
		return finiteRat(float64(v))
	case float64:
		return finiteRat(v)
	case decimal.Decimal:
		return new(big.Rat).SetString(v.String())
	case string:
		return new(big.Rat).SetString(strings.TrimSpace(v))
	}
	if i, ok := bigInt(v); ok {
		return new(big.Rat).SetInt(i), true
	}
	return nil, false
}

func finiteRat(f float64) (*big.Rat, bool) {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return nil, false
	}
	return new(big.Rat).SetFloat64(f), true
}

func isIntLiteral(l *expression.Literal) bool {
	_, ok := bigInt(l.Value())
	return ok
}

// intLiteral is n as a literal of the integer type t (n is in its range).
func intLiteral(ctx *sql.Context, n *big.Int, t sql.Type) sql.Expression {
	var v any
	if n.Sign() < 0 || n.IsInt64() {
		v = n.Int64()
	} else {
		v = n.Uint64()
	}
	if cv, _, err := t.Convert(ctx, v); err == nil {
		return expression.NewLiteral(cv, t)
	}
	return expression.NewLiteral(v, t)
}

// alwaysBool is a comparison of col with an out-of-range constant: val, or
// NULL when col is NULL (false for <=>).
func alwaysBool(col sql.Expression, op cmpOp, val bool) sql.Expression {
	return &constCompare{col: col, val: val, nullSafe: op == opNullSafeEq}
}

type constCompare struct {
	col      sql.Expression
	val      bool
	nullSafe bool
}

func (c *constCompare) Resolved() bool             { return c.col.Resolved() }
func (c *constCompare) Type() sql.Type             { return types.Boolean }
func (c *constCompare) IsNullable() bool           { return !c.nullSafe && c.col.IsNullable() }
func (c *constCompare) Children() []sql.Expression { return []sql.Expression{c.col} }

func (c *constCompare) String() string {
	if c.val {
		return "(" + c.col.String() + " IS NOT NULL OR NULL)"
	}
	return "(" + c.col.String() + " IS NULL AND NULL)"
}

func (c *constCompare) Eval(ctx *sql.Context, row sql.Row) (any, error) {
	v, err := c.col.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if v == nil {
		if c.nullSafe {
			return false, nil
		}
		return nil, nil
	}
	return c.val, nil
}

func (c *constCompare) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(children), 1)
	}
	return &constCompare{col: children[0], val: c.val, nullSafe: c.nullSafe}, nil
}

// exactIntInserts replaces a whole-number constant that is not an integer
// literal in INSERT ... VALUES for an integer column, within the column's
// range, with the integer.
func exactIntInserts(ctx *sql.Context, n sql.Node) (sql.Node, bool, error) {
	same := true
	out, _, err := transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		ii, ok := n.(*plan.InsertInto)
		if !ok {
			return n, transform.SameTree, nil
		}
		vals, ok := ii.Source.(*plan.Values)
		if !ok || ii.Destination == nil {
			return n, transform.SameTree, nil
		}
		sch := ii.Destination.Schema()
		cols := make([]*sql.Column, 0, len(sch))
		if len(ii.ColumnNames) == 0 {
			cols = append(cols, sch...)
		} else {
			for _, name := range ii.ColumnNames {
				i := sch.IndexOfColName(name)
				if i < 0 {
					return n, transform.SameTree, nil
				}
				cols = append(cols, sch[i])
			}
		}
		changed := false
		tuples := make([][]sql.Expression, len(vals.ExpressionTuples))
		for r, tuple := range vals.ExpressionTuples {
			tuples[r] = tuple
			for i, e := range tuple {
				lit, ok := e.(*expression.Literal)
				if !ok || i >= len(cols) || isIntLiteral(lit) {
					continue
				}
				lo, hi, isInt := intRange(cols[i].Type)
				if !isInt {
					continue
				}
				if v, ok := constValue(lit.Value()); ok && v.IsInt() && v.Cmp(lo) >= 0 && v.Cmp(hi) <= 0 {
					if !changed || &tuples[r][0] == &tuple[0] {
						tuples[r] = append([]sql.Expression(nil), tuple...)
					}
					changed = true
					tuples[r][i] = intLiteral(ctx, v.Num(), cols[i].Type)
				}
			}
		}
		if !changed {
			return n, transform.SameTree, nil
		}
		same = false
		nv := plan.NewValuesWithAlias(vals.AliasName, vals.ColumnNames, tuples)
		return ii.WithSource(nv), transform.NewTree, nil
	})
	return out, same, err
}

// ---- YEAR ----------------------------------------------------------------------
//
// MySQL compares a YEAR with a constant as YEARs: the constant becomes the
// year it stands for (1 and '1' are 2001, 70 is 1970, '0' is 2000, 0 and
// '0000' are 0000, 1.5 rounds to 2002), when it stands for one; otherwise
// (100, -1, 2156) the two compare as numbers. So does BETWEEN; IN does not.

// foldYearCompare is c with its constant operand as a YEAR, or nil.
func foldYearCompare(ctx *sql.Context, c expression.Comparer) sql.Expression {
	l, r := c.Left(), c.Right()
	switch {
	case types.IsYear(l.Type()) && !types.IsYear(r.Type()):
		if y, ok := yearOperand(ctx, r); ok {
			return opOf(c).build(l, y)
		}
	case types.IsYear(r.Type()) && !types.IsYear(l.Type()):
		if y, ok := yearOperand(ctx, l); ok {
			return opOf(c).build(y, r)
		}
	}
	return nil
}

// foldYearIn is MySQL's YEAR IN (...): with one value it is =; with more,
// strings stand for years and the YEAR compares with the list as a number
// (y IN (1, 70) does not find 2001). Else nil.
func foldYearIn(ctx *sql.Context, in *expression.InTuple) sql.Expression {
	if !types.IsYear(in.Left().Type()) {
		return nil
	}
	tup, ok := in.Right().(expression.Tuple)
	if !ok {
		return nil
	}
	if len(tup) == 1 {
		if y, ok := yearOperand(ctx, tup[0]); ok {
			return expression.NewEquals(in.Left(), y)
		}
		return nil
	}
	vals := make([]sql.Expression, len(tup))
	for i, v := range tup {
		if !isConstant(v) {
			return nil
		}
		vals[i] = v
		if types.IsText(v.Type()) {
			vals[i], _ = yearOperand(ctx, v)
		}
	}
	return expression.NewInTuple(expression.NewConvert(in.Left(), expression.ConvertToSigned), expression.NewTuple(vals...))
}

// yearOperand is e as a YEAR literal, when e is a constant that stands for
// a year; else e and false.
func yearOperand(ctx *sql.Context, e sql.Expression) (sql.Expression, bool) {
	if !isConstant(e) {
		return e, false
	}
	v, err := e.Eval(ctx, nil)
	if err != nil || v == nil {
		return e, false
	}
	if y, ok := yearOf(v); ok {
		return expression.NewLiteral(y, types.Int64), true
	}
	return e, false
}

// isConstant is whether e is made of literals and arithmetic.
func isConstant(e sql.Expression) bool {
	constant := true
	transform.InspectExpr(e, func(x sql.Expression) bool {
		switch x.(type) {
		case *expression.Literal:
		case *expression.Arithmetic, *expression.UnaryMinus, *expression.Div, *expression.IntDiv, *expression.Mod:
		default:
			constant = false
		}
		return !constant
	})
	return constant
}

// yearOf is the YEAR a constant stands for, as MySQL converts it.
func yearOf(v any) (int64, bool) {
	var r *big.Rat
	var ok bool
	if s, isStr := v.(string); isStr {
		t := strings.TrimSpace(s)
		if r, ok = new(big.Rat).SetString(t); !ok {
			return 0, false
		}
		if r.Sign() == 0 && len(t) < 4 {
			return 2000, true // '0' and '00' are 2000; '0000' is 0000
		}
	} else if r, ok = constValue(v); !ok {
		return 0, false
	}
	// round half away from zero
	f, _ := r.Float64()
	n := int64(math.Round(f))
	switch {
	case n == 0:
		return 0, true
	case n >= 1 && n <= 69:
		return 2000 + n, true
	case n >= 70 && n <= 99:
		return 1900 + n, true
	case n >= 1901 && n <= 2155:
		return n, true
	}
	return 0, false
}

// ---- DECIMAL -------------------------------------------------------------------
//
// A DECIMAL(p,s) column compared with a constant that has more than s
// decimals, or is out of the column's range: go-mysql-server converted the
// constant to the column's type, rounding it (d = 10.31 found 10.3 in a
// DECIMAL(3,1)). MySQL compares exactly, as foldIntCompare does for
// integers, in steps of 10^-s.

func foldDecimalCompare(ctx *sql.Context, c expression.Comparer) sql.Expression {
	op := opOf(c)
	col, ok := c.Left().(*expression.GetField)
	v, okLit := exactConst(c.Right())
	if !ok || !okLit {
		col, ok = c.Right().(*expression.GetField)
		v, okLit = exactConst(c.Left())
		if !ok || !okLit {
			return nil
		}
		op = op.mirror()
	}
	dt, isDec := col.Type().(sql.DecimalType)
	if !isDec {
		return nil
	}
	scale := new(big.Rat).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(dt.Scale())), nil))
	units := new(big.Rat).Mul(v, scale) // v in steps of 10^-s
	maxUnits := new(big.Rat).SetInt(new(big.Int).Sub(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(dt.Precision())), nil), big.NewInt(1)))
	switch {
	case units.Cmp(maxUnits) > 0:
		return alwaysBool(col, op, op == opLt || op == opLe)
	case units.Cmp(new(big.Rat).Neg(maxUnits)) < 0:
		return alwaysBool(col, op, op == opGt || op == opGe)
	case units.IsInt():
		return nil // exact in the column's type
	}
	floor := new(big.Int).Quo(units.Num(), units.Denom()) // toward zero
	if units.Sign() < 0 {
		floor.Sub(floor, big.NewInt(1))
	}
	ceil := new(big.Int).Add(floor, big.NewInt(1))
	bound := func(n *big.Int) sql.Expression {
		d := decimal.NewFromBigInt(n, -int32(dt.Scale()))
		return expression.NewLiteral(d, col.Type())
	}
	switch op {
	case opEq, opNullSafeEq:
		return alwaysBool(col, op, false)
	case opGt, opGe:
		return opGe.build(col, bound(ceil))
	default:
		return opLe.build(col, bound(floor))
	}
}

// exactConst is an integer or DECIMAL constant's value, also negated (-10.31
// is a minus on 10.31). Floats compare with a DECIMAL as doubles, and
// strings are left as they are.
func exactConst(e sql.Expression) (*big.Rat, bool) {
	neg := false
	if m, ok := e.(*expression.UnaryMinus); ok {
		neg, e = true, m.Child
	}
	lit, ok := e.(*expression.Literal)
	if !ok {
		return nil, false
	}
	switch lit.Value().(type) {
	case string, float32, float64, nil:
		return nil, false
	}
	v, ok := constValue(lit.Value())
	if !ok {
		return nil, false
	}
	if neg {
		v = new(big.Rat).Neg(v)
	}
	return v, true
}
