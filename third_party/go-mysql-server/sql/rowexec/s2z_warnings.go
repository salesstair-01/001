// s2z patch: MySQL's warnings for values INSERT / UPDATE IGNORE adjusts.

package rowexec

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/shopspring/decimal"
)

// ignoreWarning is the warning MySQL gives when IGNORE stores a value
// adjusted for a column: 1264 for a number out of range, 1366 for one that
// is not a number, 1265 for a string cut to fit (or an ENUM, SET or TIME
// value it can't hold), else the error's own code and text.
func ignoreWarning(col *sql.Column, val interface{}, err error, rowNum int) *sql.Warning {
	w := &sql.Warning{Level: "Warning"}
	switch {
	case sql.ErrValueOutOfRange.Is(err):
		w.Code, w.Message = 1264, fmt.Sprintf("Out of range value for column '%s' at row %d", col.Name, rowNum)
	case (types.IsTime(col.Type) || types.IsYear(col.Type)) && !isString(val),
		types.IsTimespan(col.Type) && isNumeric(val),
		types.IsNumber(col.Type) && !types.IsYear(col.Type) && isNumeric(val):
		// a number that is no date or time, or too big for the column
		w.Code, w.Message = 1264, fmt.Sprintf("Out of range value for column '%s' at row %d", col.Name, rowNum)
	case types.ErrLengthBeyondLimit.Is(err), types.IsEnum(col.Type), types.IsSet(col.Type), types.IsTimespan(col.Type), types.IsTime(col.Type):
		w.Code, w.Message = 1265, fmt.Sprintf("Data truncated for column '%s' at row %d", col.Name, rowNum)
	case types.IsNumber(col.Type):
		typ := "integer"
		if types.IsDecimal(col.Type) {
			typ = "decimal"
		} else if types.IsFloat(col.Type) {
			typ = "double"
		}
		w.Code, w.Message = 1366, fmt.Sprintf("Incorrect %s value: '%v' for column '%s' at row %d", typ, val, col.Name, rowNum)
	default:
		sqlerr := sql.CastSQLError(err)
		w.Code, w.Message = sqlerr.Num, sqlerr.Message
	}
	return w
}

// isNumeric is whether v is a number, or a string that is one.
func isNumeric(v interface{}) bool {
	switch x := v.(type) {
	case string:
		_, err := strconv.ParseFloat(strings.TrimSpace(x), 64)
		return err == nil || errors.Is(err, strconv.ErrRange)
	case []byte:
		return isNumeric(string(x))
	case nil:
		return false
	}
	return !isString(v)
}

func isString(v interface{}) bool {
	switch v.(type) {
	case string, []byte:
		return true
	}
	return false
}

// strictMode is whether the session's sql_mode makes a value a column
// can't hold an error; without it, INSERT and UPDATE store the closest
// value it can hold with a warning, as IGNORE does.
func strictMode(ctx *sql.Context) bool {
	m := sql.LoadSqlMode(ctx)
	return m.ModeEnabled("STRICT_TRANS_TABLES") || m.ModeEnabled("STRICT_ALL_TABLES") || m.ModeEnabled("TRADITIONAL")
}

// noteDecimalRounding notes, as MySQL does (1265), a value that a DECIMAL
// column rounds to its scale.
func noteDecimalRounding(ctx *sql.Context, col *sql.Column, orig, converted interface{}, rowNum int) {
	if !types.IsDecimal(col.Type) || ctx == nil || ctx.Session == nil {
		return
	}
	cd, ok := converted.(decimal.Decimal)
	if !ok {
		return
	}
	var od decimal.Decimal
	switch v := orig.(type) {
	case decimal.Decimal:
		od = v
	case float64:
		od = decimal.NewFromFloat(v)
	case float32:
		od = decimal.NewFromFloat32(v)
	case string:
		d, err := decimal.NewFromString(v)
		if err != nil {
			return
		}
		od = d
	default:
		return
	}
	if !od.Equal(cd) {
		ctx.Session.Warn(&sql.Warning{Level: "Note", Code: 1265, Message: fmt.Sprintf("Data truncated for column '%s' at row %d", col.Name, rowNum)})
	}
}
