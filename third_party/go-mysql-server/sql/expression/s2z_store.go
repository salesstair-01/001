// Copyright 2026 s2z. Licensed under the Apache License, Version 2.0.

package expression

// s2z patch: MySQL's handling of values a column must cut, for INSERT and
// UPDATE.

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/sqltypes"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// S2zStoreValue is v as MySQL takes it for col before converting it: a
// number column takes a string's numeric prefix ('12abc' is 12, warning or
// in strict mode error 1265) and a float past its range is its bound
// (1264); a SET takes its valid members (1265); a BIT its largest value
// (1264). (s2z patch)
func S2zStoreValue(ctx *sql.Context, name string, typ sql.Type, v interface{}, strict bool, rowNum int) (interface{}, error) {
	warn := func(code int, msg string) error {
		if strict {
			state := "01000"
			if code == 1264 {
				state = "22003"
			}
			return mysql.NewSQLError(code, state, msg+" for column '%s' at row %d", name, rowNum)
		}
		ctx.Session.Warn(&sql.Warning{Level: "Warning", Code: code, Message: fmt.Sprintf(msg+" for column '%s' at row %d", name, rowNum)})
		return nil
	}
	str, isStr := v.(string)
	switch {
	case isStr && types.IsNumber(typ) && !types.IsYear(typ) && !types.IsBit(typ):
		p, truncated := types.S2zNumberPrefix(str)
		if types.IsFloat(typ) {
			if f, _ := strconv.ParseFloat(p, 64); math.IsInf(f, 0) || (typ.Type() == sqltypes.Float32 && math.Abs(f) > math.MaxFloat32) {
				if err := warn(1264, "Out of range value"); err != nil {
					return nil, err
				}
				bound := math.MaxFloat64
				if typ.Type() == sqltypes.Float32 {
					bound = math.MaxFloat32
				}
				return math.Copysign(bound, f), nil
			}
		}
		if strings.TrimSpace(str) == "" {
			kind := "integer"
			if types.IsDecimal(typ) {
				kind = "decimal"
			} else if types.IsFloat(typ) {
				kind = "double"
			}
			msg := fmt.Sprintf("Incorrect %s value: '%s' for column '%s' at row %d", kind, str, name, rowNum)
			if strict {
				return nil, mysql.NewSQLError(1366, "HY000", "%s", msg)
			}
			ctx.Session.Warn(&sql.Warning{Level: "Warning", Code: 1366, Message: msg})
			return typ.Zero(), nil
		}
		if truncated && types.S2zHasNumberPrefix(str) {
			if err := warn(1265, "Data truncated"); err != nil {
				return nil, err
			}
			return p, nil
		}
	case isStr && types.IsSet(typ):
		var keep []string
		dropped := false
		for _, m := range strings.Split(str, ",") {
			if m == "" {
				continue
			}
			if _, _, err := typ.Convert(ctx, m); err != nil {
				dropped = true
				continue
			}
			keep = append(keep, m)
		}
		if dropped {
			if err := warn(1265, "Data truncated"); err != nil {
				return nil, err
			}
			return strings.Join(keep, ","), nil
		}
	case types.IsBit(typ) && !strict:
		if bt, ok := typ.(types.BitType); ok {
			if n, err := strconv.ParseUint(fmt.Sprint(v), 10, 64); err == nil && bt.NumberOfBits() < 64 && n >= 1<<bt.NumberOfBits() {
				_ = warn(1264, "Out of range value")
				return uint64(1)<<bt.NumberOfBits() - 1, nil
			}
		}
	}
	return v, nil
}

// S2zStoreError is MySQL's strict-mode error for err, converting a value
// for column name of type typ, or nil to keep err: 1406 for a string or BIT
// too long, 1265 for an ENUM or SET value, 1264 for a YEAR.
func S2zStoreError(name string, typ sql.Type, err error, rowNum int) error {
	switch {
	case types.ErrLengthBeyondLimit.Is(err) && types.IsText(typ), types.IsBit(typ):
		return mysql.NewSQLError(1406, "22001", "Data too long for column '%s' at row %d", name, rowNum)
	case types.ErrConvertingToEnum.Is(err) || sql.ErrInvalidSetValue.Is(err):
		return mysql.NewSQLError(1265, "01000", "Data truncated for column '%s' at row %d", name, rowNum)
	case types.ErrConvertingToYear.Is(err):
		return mysql.NewSQLError(1264, "22003", "Out of range value for column '%s' at row %d", name, rowNum)
	}
	return nil
}
