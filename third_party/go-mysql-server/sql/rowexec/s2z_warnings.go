// s2z patch: MySQL's warnings for values INSERT / UPDATE IGNORE adjusts.

package rowexec

import (
	"fmt"

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
	case types.ErrLengthBeyondLimit.Is(err), types.IsEnum(col.Type), types.IsSet(col.Type), types.IsTimespan(col.Type):
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
