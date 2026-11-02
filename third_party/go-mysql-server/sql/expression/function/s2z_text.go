// s2z patch: string functions of temporal and numeric values.

package function

import (
	"fmt"
	"strconv"
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

// numericDate is a number MySQL reads as a date or datetime, YYYYMMDD,
// YYMMDD, YYYYMMDDhhmmss or YYMMDDhhmmss, as text the datetime type reads;
// other values as they are.
func numericDate(v interface{}) interface{} {
	var n int64
	switch x := v.(type) {
	case int64:
		n = x
	case int32:
		n = int64(x)
	case int16:
		n = int64(x)
	case int8:
		n = int64(x)
	case int:
		n = int64(x)
	case uint64:
		n = int64(x)
	case uint32:
		n = int64(x)
	default:
		return v
	}
	if n <= 0 {
		return v
	}
	s := strconv.FormatInt(n, 10)
	year := func(yy string) string {
		if yy < "70" {
			return "20" + yy
		}
		return "19" + yy
	}
	switch len(s) {
	case 5, 6:
		s = fmt.Sprintf("%06d", n)
		return year(s[:2]) + "-" + s[2:4] + "-" + s[4:6]
	case 7, 8:
		s = fmt.Sprintf("%08d", n)
		return s[:4] + "-" + s[4:6] + "-" + s[6:8]
	case 11, 12:
		s = fmt.Sprintf("%012d", n)
		return year(s[:2]) + "-" + s[2:4] + "-" + s[4:6] + " " + s[6:8] + ":" + s[8:10] + ":" + s[10:12]
	case 13, 14:
		s = fmt.Sprintf("%014d", n)
		return s[:4] + "-" + s[4:6] + "-" + s[6:8] + " " + s[8:10] + ":" + s[10:12] + ":" + s[12:14]
	}
	return v
}
