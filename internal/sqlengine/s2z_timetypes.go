package sqlengine

import (
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/sqltypes"
)

// MySQL's types for the TIME functions sqld implements: a TIME or DATETIME
// with the fractional digits of the arguments.

// temporalFsp is the fractional digits MySQL gives an argument of a
// temporal function: a temporal type's own, a DECIMAL's scale, none for an
// integer or DATE, a string literal's digits after its point, else 6.
func temporalFsp(e sql.Expression) int {
	t := e.Type()
	switch {
	case types.IsTimespan(t):
		if tt, ok := t.(types.TimespanType_); ok {
			if p, fixed := tt.Precision(); fixed {
				return p
			}
		}
		return 0
	case types.IsDateType(t), types.IsInteger(t), types.IsYear(t):
		return 0
	case types.IsDatetimeType(t) || types.IsTimestampType(t):
		if dt, ok := t.(sql.DatetimeType); ok {
			return dt.Precision()
		}
		return 0
	case types.IsDecimal(t):
		return min(int(t.(sql.DecimalType).Scale()), 6)
	case types.IsText(t):
		if lit, ok := e.(*expression.Literal); ok {
			if s, ok := lit.Value().(string); ok {
				i := strings.LastIndexByte(s, '.')
				if i < 0 {
					return 0
				}
				n := 0
				for _, c := range s[i+1:] {
					if c < '0' || c > '9' {
						break
					}
					n++
				}
				return min(n, 6)
			}
		}
	}
	return 6
}

func maxFsp(args []sql.Expression) int {
	f := 0
	for _, a := range args {
		f = max(f, temporalFsp(a))
	}
	return f
}

// addTimeType is ADDTIME's and SUBTIME's: the first argument's kind (TIME,
// or DATETIME for a DATE or DATETIME), else a string.
func addTimeType(args []sql.Expression) sql.Type {
	t := args[0].Type()
	switch {
	case types.IsTimespan(t):
		return types.CreateTimeType(maxFsp(args))
	case types.IsDateType(t), types.IsDatetimeType(t), types.IsTimestampType(t):
		return types.MustCreateDatetimeType(sqltypes.Datetime, maxFsp(args))
	}
	return types.LongText
}

func timeDiffType(args []sql.Expression) sql.Type {
	return types.CreateTimeType(maxFsp(args))
}

// lastArgTimeType is a TIME with the last argument's digits (SEC_TO_TIME,
// MAKETIME).
func lastArgTimeType(args []sql.Expression) sql.Type {
	a := args[len(args)-1]
	if types.IsText(a.Type()) {
		return types.CreateTimeType(6) // a number from a string: 6 digits
	}
	return types.CreateTimeType(temporalFsp(a))
}

// fspArgTimeType is CURTIME(n) and UTC_TIME(n): TIME(n).
func fspArgTimeType(args []sql.Expression) sql.Type {
	if len(args) == 1 {
		if lit, ok := args[0].(*expression.Literal); ok {
			if n, err := fnInt64(nil, lit.Value()); err == nil && n >= 0 && n <= 6 {
				return types.CreateTimeType(int(n))
			}
		}
	}
	return types.CreateTimeType(0)
}

// typedTime makes a function's text result a value of its type (a TIME or
// DATETIME), so that it is shown and used with the type's digits.
func typedTime(typ func([]sql.Expression) sql.Type, eval func(*sql.Context, []sql.Expression, []any) (any, error)) func(*sql.Context, []sql.Expression, []any) (any, error) {
	return func(ctx *sql.Context, args []sql.Expression, v []any) (any, error) {
		r, err := eval(ctx, args, v)
		if err != nil || r == nil {
			return r, err
		}
		t := typ(args)
		if !types.IsTimespan(t) && !types.IsDatetimeType(t) {
			return r, nil
		}
		if c, _, cerr := t.Convert(ctx, r); cerr == nil {
			return c, nil
		}
		return r, nil
	}
}
