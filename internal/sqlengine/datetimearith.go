package sqlengine

import (
	"strings"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/shopspring/decimal"
)

// go-mysql-server types x + INTERVAL n unit (and -) as DATETIME(0), so a
// DATETIME(6) operand lost its fraction and a DATE + INTERVAL 1 DAY printed
// a time. MySQL's type is the operand's: a DATE stays a DATE for units of a
// day or more, a DATETIME keeps its precision, raised to 6 by a
// microsecond unit or to a decimal value's scale; a string gives a string
// (issue #25). intervalArith retypes the expression accordingly.

// intervalArith's one child is the arithmetic, so go-mysql-server's check
// that an interval is only used under + and - still sees it.
type intervalArith struct {
	arith *expression.Arithmetic
	typ   sql.Type
	text  bool // a string operand: format as MySQL does
	date  bool // ... and it has no time, nor the unit
	span  bool // a TIME operand: the result is a TIME
}

var _ sql.Expression = (*intervalArith)(nil)

func (e *intervalArith) Type() sql.Type             { return e.typ }
func (e *intervalArith) Resolved() bool             { return e.arith.Resolved() }
func (e *intervalArith) IsNullable() bool           { return e.arith.IsNullable() }
func (e *intervalArith) String() string             { return e.arith.String() }
func (e *intervalArith) Children() []sql.Expression { return []sql.Expression{e.arith} }

func (e *intervalArith) WithChildren(c ...sql.Expression) (sql.Expression, error) {
	if len(c) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(e, len(c), 1)
	}
	a, ok := c[0].(*expression.Arithmetic)
	if !ok {
		return c[0], nil
	}
	n := *e
	n.arith = a
	return &n, nil
}

// Eval adds the interval at full precision (go-mysql-server's Eval first
// converts the operand to its DATETIME(0)).
func (e *intervalArith) Eval(ctx *sql.Context, row sql.Row) (any, error) {
	a := e.arith
	iv, other := a.RightChild, a.LeftChild
	if _, ok := iv.(*expression.Interval); !ok {
		iv, other = a.LeftChild, a.RightChild
	}
	v, err := other.Eval(ctx, row)
	if err != nil || v == nil {
		return nil, err
	}
	d, err := intervalDelta(ctx, iv.(*expression.Interval), row)
	if err != nil || d == nil {
		return nil, err
	}
	if e.span {
		t, ok := toTemporal(ctx, other, v)
		if !ok || t.isDT || d.Years != 0 || d.Months != 0 {
			return nil, nil
		}
		us := (((d.Days*24+d.Hours)*60+d.Minutes)*60+d.Seconds)*usPerSec + d.Microseconds
		if a.Op == "-" {
			us = -us
		}
		us = clampTime(t.us + us)
		return formatTime(us, resultScale(us, t)), nil
	}
	tv, _, err := types.DatetimeMaxPrecision.Convert(ctx, v)
	if err != nil || tv == nil {
		ctx.Warn(1292, "Incorrect datetime value: '%v'", v)
		return nil, nil
	}
	var t time.Time
	if a.Op == "-" {
		t = d.Sub(tv.(time.Time))
	} else {
		t = d.Add(tv.(time.Time))
	}
	if !validDatetime(t) {
		return nil, nil
	}
	if !e.text {
		return t, nil
	}
	if e.date {
		return t.Format("2006-01-02"), nil
	}
	s := t.Format("2006-01-02 15:04:05")
	if t.Nanosecond() != 0 {
		s += t.Format(".000000")
	}
	return s, nil
}

// intervalDelta is the interval's delta; n SECOND keeps a decimal n's
// fraction, which go-mysql-server rounds away.
func intervalDelta(ctx *sql.Context, i *expression.Interval, row sql.Row) (*expression.TimeDelta, error) {
	if strings.EqualFold(i.Unit, "SECOND") {
		v, err := i.Child.Eval(ctx, row)
		if err != nil || v == nil {
			return nil, err
		}
		if d, ok := v.(decimal.Decimal); ok && !d.IsInteger() {
			us := d.Mul(decimal.NewFromInt(usPerSec)).Round(0).IntPart()
			return &expression.TimeDelta{Seconds: us / usPerSec, Microseconds: us % usPerSec}, nil
		}
	}
	return i.EvalDelta(ctx, row)
}

var dateUnits = map[string]bool{"YEAR": true, "QUARTER": true, "MONTH": true, "WEEK": true, "DAY": true, "YEAR_MONTH": true}

// intervalScale is the fractional digits an interval adds.
func intervalScale(i *expression.Interval) int {
	if strings.HasSuffix(strings.ToUpper(i.Unit), "MICROSECOND") {
		return 6
	}
	if l, ok := i.Child.(*expression.Literal); ok {
		switch v := l.Value().(type) {
		case decimal.Decimal:
			return min(6, int(max(0, -v.Exponent())))
		case string:
			if strings.Contains(v, ".") {
				return 6
			}
		}
	}
	return 0
}

// retypeIntervalArith wraps a + or - with an interval; a unchanged when
// go-mysql-server's type is right or the operand's type isn't known.
func retypeIntervalArith(a *expression.Arithmetic) sql.Expression {
	var iv *expression.Interval
	var other sql.Expression
	if i, ok := a.RightChild.(*expression.Interval); ok {
		iv, other = i, a.LeftChild
	} else if i, ok := a.LeftChild.(*expression.Interval); ok {
		iv, other = i, a.RightChild
	} else {
		return a
	}
	unit := strings.ToUpper(iv.Unit)
	scale := intervalScale(iv)
	t := other.Type()
	switch {
	case types.IsTime(t) && t.Type() == sqltypes.Date:
		if dateUnits[unit] {
			return &intervalArith{arith: a, typ: types.Date}
		}
		return &intervalArith{arith: a, typ: types.MustCreateDatetimeType(sqltypes.Datetime, scale)}
	case types.IsTime(t):
		p := typeScale(t)
		return &intervalArith{arith: a, typ: types.MustCreateDatetimeType(sqltypes.Datetime, max(p, scale))}
	case t == types.Time:
		return &intervalArith{arith: a, typ: types.LongText, span: true}
	case types.IsText(t):
		date := dateUnits[unit]
		if l, ok := other.(*expression.Literal); ok {
			s, _ := l.Value().(string)
			date = date && !strings.ContainsAny(strings.TrimSpace(s), " T")
		} else {
			date = false
		}
		return &intervalArith{arith: a, typ: types.LongText, text: true, date: date}
	}
	return a
}
