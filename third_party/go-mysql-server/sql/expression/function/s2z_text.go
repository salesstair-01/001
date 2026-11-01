// s2z patch: string functions of temporal and numeric values.

package function

import (
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/sqltypes"
)

// valueText is v as MySQL shows a value of type t (a DATETIME with its
// fractional digits, a TIME as hh:mm:ss), for the string functions that
// take any value: SUBSTRING, LEFT, RIGHT, INSTR.
func valueText(ctx *sql.Context, t sql.Type, v interface{}) ([]rune, error) {
	if dt, ok := t.(sql.DatetimeType); ok && dt.Type() != sqltypes.Date {
		if tv, isTime := v.(time.Time); isTime {
			layout := "2006-01-02 15:04:05"
			if p := dt.Precision(); p > 0 {
				layout += ".000000"[:p+1]
			}
			return []rune(tv.Format(layout)), nil
		}
	}
	if types.IsDecimal(t) || types.IsFloat(t) || types.IsInteger(t) {
		s, _, err := types.LongText.Convert(ctx, v)
		if err != nil {
			return nil, err
		}
		if str, ok := s.(string); ok {
			return []rune(str), nil
		}
	}
	sv, err := t.SQL(ctx, nil, v)
	if err != nil {
		return nil, err
	}
	return []rune(sv.ToString()), nil
}
