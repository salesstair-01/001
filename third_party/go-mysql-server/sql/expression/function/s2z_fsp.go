// Copyright 2026 s2z. Licensed under the Apache License, Version 2.0.

package function

// s2z patch: the fractional digits MySQL gives a temporal function's
// result from its argument.

import (
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// s2zFsp is the digits of e as a temporal argument: a temporal type's own,
// a DECIMAL's scale, none for an integer or DATE, a string literal's digits
// after its point, else 6.
func s2zFsp(e sql.Expression) int {
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
