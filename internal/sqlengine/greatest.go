package sqlengine

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression/function"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/shopspring/decimal"
)

// GREATEST and LEAST of TIME values, and of DECIMAL and integer values,
// which go-mysql-server refuses: TIMEs compare as times and give a TIME with
// the arguments' most fractional digits; DECIMALs and integers compare as
// decimals and give a DECIMAL with the most scale. Other arguments are
// go-mysql-server's.

func init() {
	for name, greatest := range map[string]bool{"greatest": true, "least": false} {
		upstream := function.NewLeast
		if greatest {
			upstream = function.NewGreatest
		}
		overrides[name] = sql.FunctionN{Name: name, Fn: func(args ...sql.Expression) (sql.Expression, error) {
			switch extremeKind(args) {
			case "time":
				return fnN(name, 1, -1, timeExtremeType, false, extremeTime(greatest)).NewInstance(args)
			case "decimal":
				return fnN(name, 1, -1, decimalExtremeType, false, extremeDecimal(greatest)).NewInstance(args)
			}
			return upstream(args...)
		}}
	}
}

// extremeKind is "time" when every argument is a TIME, "decimal" when they
// are DECIMALs and integers with at least one DECIMAL, else "".
func extremeKind(args []sql.Expression) string {
	allTime, allExact, anyDecimal := true, true, false
	for _, a := range args {
		if !a.Resolved() {
			return ""
		}
		t := a.Type()
		if t == types.Null {
			continue // the result is NULL, of the others' type
		}
		allTime = allTime && types.IsTimespan(t)
		allExact = allExact && (types.IsDecimal(t) || types.IsInteger(t))
		anyDecimal = anyDecimal || types.IsDecimal(t)
	}
	switch {
	case allTime:
		return "time"
	case allExact && anyDecimal:
		return "decimal"
	}
	return ""
}

// timePrecision is a TIME type's fractional digits, -1 when the type has
// none of its own (a literal: then its value's).
func timePrecision(t sql.Type) int {
	if tt, ok := t.(types.TimespanType_); ok {
		if p, fixed := tt.Precision(); fixed {
			return p
		}
	}
	return -1
}

func timeExtremeType(args []sql.Expression) sql.Type {
	p := 0
	for _, a := range args {
		p = max(p, timePrecision(a.Type()))
	}
	return types.CreateTimeType(p)
}

func extremeTime(greatest bool) func(*sql.Context, []sql.Expression, []any) (any, error) {
	return func(ctx *sql.Context, args []sql.Expression, v []any) (any, error) {
		var best int64
		p := 0
		for i, x := range v {
			if x == nil {
				return nil, nil
			}
			ts, err := types.Time.ConvertToTimespan(x)
			if err != nil {
				return nil, err
			}
			us := ts.AsMicroseconds()
			if ap := timePrecision(args[i].Type()); ap >= 0 {
				p = max(p, ap)
			} else {
				p = max(p, fracDigits(us))
			}
			if i == 0 || (greatest && us > best) || (!greatest && us < best) {
				best = us
			}
		}
		return types.CreateTimeType(p).MicrosecondsToTimespan(best), nil
	}
}

// fracDigits is the fractional digits a time in microseconds needs.
func fracDigits(us int64) int {
	f := us % usPerSec
	if f < 0 {
		f = -f
	}
	n := 6
	for n > 0 && f%10 == 0 {
		f /= 10
		n--
	}
	return n
}

// decimalDigits is a DECIMAL or integer type's digits before the point and
// after it.
func decimalDigits(t sql.Type) (int, int) {
	if dt, ok := t.(sql.DecimalType); ok {
		return int(dt.Precision() - dt.Scale()), int(dt.Scale())
	}
	switch t.Type() {
	case types.Int8.Type(), types.Uint8.Type():
		return 3, 0
	case types.Int16.Type(), types.Uint16.Type():
		return 5, 0
	case types.Int24.Type(), types.Uint24.Type():
		return 8, 0
	case types.Int32.Type(), types.Uint32.Type():
		return 10, 0
	}
	return 19, 0
}

func decimalExtremeType(args []sql.Expression) sql.Type {
	ints, scale := 0, 0
	for _, a := range args {
		i, s := decimalDigits(a.Type())
		ints, scale = max(ints, i), max(scale, s)
	}
	t, err := types.CreateDecimalType(uint8(min(ints+scale, 65)), uint8(min(scale, 30)))
	if err != nil {
		return types.InternalDecimalType
	}
	return t
}

func extremeDecimal(greatest bool) func(*sql.Context, []sql.Expression, []any) (any, error) {
	return func(ctx *sql.Context, args []sql.Expression, v []any) (any, error) {
		var best decimal.Decimal
		scale := int32(0)
		for i, x := range v {
			if x == nil {
				return nil, nil
			}
			c, _, err := types.InternalDecimalType.Convert(ctx, x)
			if err != nil {
				return nil, err
			}
			d := c.(decimal.Decimal)
			_, s := decimalDigits(args[i].Type())
			scale = max(scale, int32(s))
			if i == 0 || (greatest && d.GreaterThan(best)) || (!greatest && d.LessThan(best)) {
				best = d
			}
		}
		return best.Round(scale), nil
	}
}
