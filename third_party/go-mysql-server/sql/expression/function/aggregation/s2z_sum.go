// Copyright 2026 s2z. Licensed under the Apache License, Version 2.0.

package aggregation

// s2z patch: SUM and AVG of an exact number are DECIMAL, as in MySQL.

import (
	"math/big"
	"strconv"
	"strings"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/shopspring/decimal"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// exactDigits is the precision and scale MySQL gives an exact number
// (integer, DECIMAL, BIT, YEAR, date or time), and false for anything else.
func exactDigits(e sql.Expression) (int, int, bool) {
	if lit, ok := e.(*expression.Literal); ok && types.IsInteger(lit.Type()) {
		if v := lit.Value(); v != nil {
			s := strings.TrimPrefix(strconv.FormatInt(toInt64(v), 10), "-")
			return len(s), 0, true
		}
	}
	t := e.Type()
	switch {
	case types.IsDecimal(t):
		dt := t.(sql.DecimalType)
		return int(dt.Precision()), int(dt.Scale()), true
	case types.IsYear(t):
		return 4, 0, true
	case types.IsBit(t):
		if bt, ok := t.(types.BitType); ok {
			return int(bt.NumberOfBits()), 0, true
		}
	case types.IsInteger(t):
		switch t.Type() {
		case sqltypes.Int8, sqltypes.Uint8:
			return 3, 0, true
		case sqltypes.Int16, sqltypes.Uint16:
			return 5, 0, true
		case sqltypes.Int24, sqltypes.Uint24:
			return 8, 0, true
		case sqltypes.Int32, sqltypes.Uint32:
			return 10, 0, true
		case sqltypes.Uint64:
			return 20, 0, true
		}
		return 19, 0, true
	case types.IsDateType(t):
		return 8, 0, true
	case types.IsDatetimeType(t) || types.IsTimestampType(t):
		p := 0
		if dt, ok := t.(sql.DatetimeType); ok {
			p = dt.Precision()
		}
		return 14 + p, p, true
	case types.IsTimespan(t):
		p := 0
		if tt, ok := t.(types.TimespanType_); ok {
			if n, fixed := tt.Precision(); fixed {
				p = n
			}
		}
		return 7 + p, p, true
	}
	return 0, 0, false
}

func toInt64(v interface{}) int64 {
	switch x := v.(type) {
	case int8:
		return int64(x)
	case int16:
		return int64(x)
	case int32:
		return int64(x)
	case int64:
		return x
	case int:
		return int64(x)
	case uint8:
		return int64(x)
	case uint16:
		return int64(x)
	case uint32:
		return int64(x)
	case uint64:
		return int64(x)
	}
	return 0
}

func decimalType(p, s int) sql.Type {
	return types.MustCreateDecimalType(uint8(min(p, types.DecimalTypeMaxPrecision)), uint8(min(s, types.DecimalTypeMaxScale)))
}

// sumType is MySQL's type of SUM(e): DECIMAL(p+22, s) of an exact number,
// else DOUBLE.
func sumType(e sql.Expression) sql.Type {
	if p, s, ok := exactDigits(e); ok {
		return decimalType(p+22, s)
	}
	return types.Float64
}

// avgType is MySQL's type of AVG(e): DECIMAL(p+4, s+4) of an exact
// number, else DOUBLE.
func avgType(e sql.Expression) sql.Type {
	if p, s, ok := exactDigits(e); ok {
		return decimalType(p+4, s+4)
	}
	return types.Float64
}

// exactDecimal is v, a value of type t, as a DECIMAL.
func exactDecimal(ctx *sql.Context, t sql.Type, v interface{}) decimal.Decimal {
	switch x := v.(type) {
	case decimal.Decimal:
		return x
	case uint64:
		return decimal.NewFromBigInt(new(big.Int).SetUint64(x), 0)
	case int8, int16, int32, int64, int, uint8, uint16, uint32:
		return decimal.NewFromInt(toInt64(x))
	case bool:
		if x {
			return decimal.NewFromInt(1)
		}
		return decimal.Zero
	}
	if d, ok := expression.S2zTemporalDecimal(t, v); ok {
		return d
	}
	if d, _, err := types.InternalDecimalType.Convert(ctx, v); err == nil {
		if dd, ok := d.(decimal.Decimal); ok {
			return dd
		}
	}
	return decimal.Zero
}
