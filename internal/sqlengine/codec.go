package sqlengine

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"math/big"
	"strings"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/shopspring/decimal"

	"github.com/salesstair-01/001/internal/encoding"
)

// colCodec encodes one column's values for the key and the row value.
type colCodec struct {
	info *ColumnInfo
	typ  sql.Type
}

// ---- row values (D3: compact, not sortable) ----------------------------------

func (c *colCodec) encodeValue(ctx context.Context, v any) ([]byte, error) {
	t := c.typ
	switch {
	case types.IsSigned(t), types.IsYear(t):
		n, err := toInt64(ctx, v)
		return encoding.ValueInt(n), err
	case types.IsUnsigned(t), types.IsBit(t), types.IsEnum(t), types.IsSet(t):
		n, err := toUint64(ctx, t, v)
		return encoding.ValueUint(n), err
	case types.IsFloat(t):
		f, err := toFloat64(v)
		return encoding.ValueFloat(f), err
	case types.IsDecimal(t):
		d, err := toDecimal(ctx, t, v)
		if err != nil {
			return nil, err
		}
		return encodeDecimalValue(d), nil
	case types.IsTime(t):
		tm, err := toTime(ctx, t, v)
		return encoding.ValueInt(tm.UnixMicro()), err
	case types.IsTimespan(t):
		ts, err := toTimespan(ctx, t, v)
		return encoding.ValueInt(int64(ts)), err
	case types.IsJSON(t):
		cv, _, err := t.Convert(ctx, v)
		if err != nil {
			return nil, err
		}
		w, ok := cv.(sql.JSONWrapper)
		if !ok {
			return nil, fmt.Errorf("cannot encode %T as JSON", v)
		}
		s, err := types.JsonToMySqlString(w)
		return []byte(s), err
	case types.IsText(t) || types.IsBinaryType(t):
		b, err := toBytes(v)
		return trimChar(t, b), err
	case types.IsGeometry(t):
		return nil, fmt.Errorf("spatial types are not supported in v1")
	}
	return nil, fmt.Errorf("column %s: unsupported type %s", c.info.Name, t)
}

func (c *colCodec) decodeValue(ctx context.Context, b []byte) (any, error) {
	t := c.typ
	var v any
	switch {
	case types.IsSigned(t), types.IsYear(t):
		n, err := encoding.ParseInt(b)
		if err != nil {
			return nil, err
		}
		v = n
	case types.IsUnsigned(t), types.IsBit(t), types.IsEnum(t), types.IsSet(t):
		n, err := encoding.ParseUint(b)
		if err != nil {
			return nil, err
		}
		if types.IsEnum(t) {
			return uint16(n), nil
		}
		v = n
	case types.IsFloat(t):
		f, err := encoding.ParseFloat(b)
		if err != nil {
			return nil, err
		}
		v = f
	case types.IsDecimal(t):
		d, err := decodeDecimalValue(b)
		if err != nil {
			return nil, err
		}
		v = d
	case types.IsTime(t):
		n, err := encoding.ParseInt(b)
		if err != nil {
			return nil, err
		}
		return time.UnixMicro(n).UTC(), nil
	case types.IsTimespan(t):
		n, err := encoding.ParseInt(b)
		if err != nil {
			return nil, err
		}
		return types.Timespan(n), nil
	case types.IsJSON(t):
		v = string(b)
	case types.IsBinaryType(t):
		v = append([]byte(nil), b...)
	case types.IsText(t):
		return string(b), nil
	default:
		return nil, fmt.Errorf("column %s: unsupported type %s", c.info.Name, t)
	}
	out, _, err := t.Convert(ctx, v)
	return out, err
}

// ---- key columns (D2: memcomparable) -----------------------------------------

// appendKey appends the key encoding of v, which must already be of the
// column's type (or convertible to it).
func (c *colCodec) appendKey(ctx context.Context, b []byte, v any) ([]byte, error) {
	if v == nil {
		return encoding.AppendNull(b, false), nil
	}
	t := c.typ
	switch {
	case types.IsSigned(t), types.IsYear(t):
		n, err := toInt64(ctx, v)
		return encoding.AppendInt(b, n, false), err
	case types.IsUnsigned(t), types.IsBit(t), types.IsEnum(t), types.IsSet(t):
		n, err := toUint64(ctx, t, v)
		return encoding.AppendUint(b, n, false), err
	case types.IsFloat(t):
		f, err := toFloat64(v)
		return encoding.AppendFloat(b, f, false), err
	case types.IsDecimal(t):
		d, err := toDecimal(ctx, t, v)
		if err != nil {
			return nil, err
		}
		return appendDecimalKey(b, d), nil
	case types.IsTime(t):
		tm, err := toTime(ctx, t, v)
		return encoding.AppendTime(b, tm.UnixMicro(), false), err
	case types.IsTimespan(t):
		ts, err := toTimespan(ctx, t, v)
		return encoding.AppendTime(b, int64(ts), false), err
	case types.IsText(t) || types.IsBinaryType(t):
		raw, err := toBytes(v)
		if err != nil {
			return nil, err
		}
		w, err := sortKey(t, trimChar(t, raw))
		if err != nil {
			return nil, err
		}
		return encoding.AppendBytes(b, w, false), nil
	}
	return nil, fmt.Errorf("column %s: type %s cannot be part of a key in v1", c.info.Name, t)
}

// trimChar drops the trailing spaces of a CHAR value, as MySQL does when it
// stores one (without PAD_CHAR_TO_FULL_LENGTH): 'a ' reads back as 'a', and
// ' ' and ” are the same key.
func trimChar(t sql.Type, b []byte) []byte {
	if t.Type() == sqltypes.Char {
		return bytes.TrimRight(b, " ")
	}
	return b
}

// sortKey returns the bytes a string sorts by under its collation (D2):
// raw bytes for binary and _bin collations, per-character weights otherwise,
// with trailing spaces stripped first for PAD SPACE collations.
func sortKey(t sql.Type, raw []byte) ([]byte, error) {
	coll := sql.Collation_binary
	if tc, ok := t.(sql.TypeWithCollation); ok {
		coll = tc.Collation()
	}
	if coll == sql.Collation_binary {
		return raw, nil
	}
	s := string(raw)
	if coll.PadAttribute() == "PAD SPACE" {
		s = strings.TrimRight(s, " ")
	}
	if coll == sql.Collation_utf8mb4_bin || coll == sql.Collation_utf8mb4_0900_bin {
		return []byte(s), nil
	}
	sorter := coll.Sorter()
	if sorter == nil {
		return nil, fmt.Errorf("collation %s has no sort weights", coll.Name())
	}
	out := make([]byte, 0, len(s)*4)
	for _, r := range s {
		out = binary.BigEndian.AppendUint32(out, uint32(sorter(r)))
	}
	return out, nil
}

// keyKind says how to skip over this column in a key.
func (c *colCodec) keyKind() encoding.ColumnKind {
	switch {
	case types.IsDecimal(c.typ):
		return encoding.KindDecimal
	case types.IsText(c.typ) || types.IsBinaryType(c.typ):
		return encoding.KindBytes
	}
	return encoding.KindFixed8
}

// truncate applies an index prefix length col(N): N characters for text,
// N bytes for binary strings (MySQL checks prefix uniqueness the same way).
func truncate(t sql.Type, v any, n int) any {
	if n <= 0 || v == nil {
		return v
	}
	switch x := v.(type) {
	case string:
		if types.IsBinaryType(t) {
			if len(x) > n {
				return x[:n]
			}
			return x
		}
		i := 0
		for pos := range x {
			if i == n {
				return x[:pos]
			}
			i++
		}
		return x
	case []byte:
		if len(x) > n {
			return x[:n]
		}
	}
	return v
}

// keyable reports whether a type can be part of a primary key in v1.
func keyable(t sql.Type) bool {
	return !(types.IsJSON(t) || types.IsGeometry(t))
}

// ---- conversions -------------------------------------------------------------

func toInt64(ctx context.Context, v any) (int64, error) {
	switch x := v.(type) {
	case int8:
		return int64(x), nil
	case int16:
		return int64(x), nil
	case int32:
		return int64(x), nil
	case int64:
		return x, nil
	case int:
		return int64(x), nil
	case uint8:
		return int64(x), nil
	case uint16:
		return int64(x), nil
	case uint32:
		return int64(x), nil
	case uint64:
		if x > math.MaxInt64 {
			return math.MaxInt64, nil
		}
		return int64(x), nil
	case bool:
		if x {
			return 1, nil
		}
		return 0, nil
	}
	n, _, err := types.Int64.Convert(ctx, v)
	if err != nil {
		return 0, err
	}
	return n.(int64), nil
}

func toUint64(ctx context.Context, t sql.Type, v any) (uint64, error) {
	switch x := v.(type) {
	case uint8:
		return uint64(x), nil
	case uint16:
		return uint64(x), nil
	case uint32:
		return uint64(x), nil
	case uint64:
		return x, nil
	case int64:
		if x < 0 {
			return 0, nil
		}
		return uint64(x), nil
	case int32:
		if x < 0 {
			return 0, nil
		}
		return uint64(x), nil
	case int:
		if x < 0 {
			return 0, nil
		}
		return uint64(x), nil
	}
	cv, _, err := t.Convert(ctx, v)
	if err != nil {
		return 0, err
	}
	if cv == nil {
		return 0, nil
	}
	if _, ok := cv.(string); ok {
		return 0, fmt.Errorf("cannot encode %T as an unsigned key", v)
	}
	return toUint64(ctx, types.Uint64, cv)
}

func toFloat64(v any) (float64, error) {
	switch x := v.(type) {
	case float32:
		return float64(x), nil
	case float64:
		return x, nil
	case int64:
		return float64(x), nil
	case int32:
		return float64(x), nil
	case int:
		return float64(x), nil
	case uint64:
		return float64(x), nil
	case decimal.Decimal:
		f, _ := x.Float64()
		return f, nil
	}
	return 0, fmt.Errorf("cannot encode %T as a float", v)
}

func toDecimal(ctx context.Context, t sql.Type, v any) (decimal.Decimal, error) {
	if d, ok := v.(decimal.Decimal); ok {
		return d, nil
	}
	cv, _, err := t.Convert(ctx, v)
	if err != nil {
		return decimal.Decimal{}, err
	}
	switch d := cv.(type) {
	case decimal.Decimal:
		return d, nil
	case decimal.NullDecimal:
		return d.Decimal, nil
	}
	return decimal.Decimal{}, fmt.Errorf("cannot encode %T as a decimal", v)
}

func toTime(ctx context.Context, t sql.Type, v any) (time.Time, error) {
	if tm, ok := v.(time.Time); ok {
		return tm.UTC(), nil
	}
	cv, _, err := t.Convert(ctx, v)
	if err != nil {
		return time.Time{}, err
	}
	tm, ok := cv.(time.Time)
	if !ok {
		return time.Time{}, fmt.Errorf("cannot encode %T as a time", v)
	}
	return tm.UTC(), nil
}

func toTimespan(ctx context.Context, t sql.Type, v any) (types.Timespan, error) {
	if ts, ok := v.(types.Timespan); ok {
		return ts, nil
	}
	cv, _, err := t.Convert(ctx, v)
	if err != nil {
		return 0, err
	}
	ts, ok := cv.(types.Timespan)
	if !ok {
		return 0, fmt.Errorf("cannot encode %T as a time", v)
	}
	return ts, nil
}

func toBytes(v any) ([]byte, error) {
	switch x := v.(type) {
	case string:
		return []byte(x), nil
	case []byte:
		return x, nil
	case fmt.Stringer:
		return []byte(x.String()), nil
	}
	return nil, fmt.Errorf("cannot encode %T as a string", v)
}

// ---- DECIMAL -----------------------------------------------------------------

// encodeDecimalValue is the D3 value form: scale (u8), sign, magnitude.
func encodeDecimalValue(d decimal.Decimal) []byte {
	exp := d.Exponent()
	coef := new(big.Int).Set(d.Coefficient())
	if exp > 0 {
		coef.Mul(coef, new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(exp)), nil))
		exp = 0
	}
	sign := byte(0)
	if coef.Sign() < 0 {
		sign = 1
		coef.Neg(coef)
	}
	return append([]byte{uint8(-exp), sign}, coef.Bytes()...)
}

func decodeDecimalValue(b []byte) (decimal.Decimal, error) {
	if len(b) < 2 {
		return decimal.Decimal{}, encoding.ErrBadRow
	}
	coef := new(big.Int).SetBytes(b[2:])
	if b[1] == 1 {
		coef.Neg(coef)
	}
	return decimal.NewFromBigInt(coef, -int32(b[0])), nil
}

// appendDecimalKey writes the memcomparable decimal encoding (D2).
func appendDecimalKey(b []byte, d decimal.Decimal) []byte {
	if d.Sign() == 0 {
		return encoding.AppendDecimal(b, 0, 0, "", false)
	}
	digits := new(big.Int).Abs(d.Coefficient()).String()
	return encoding.AppendDecimal(b, d.Sign(), d.Exponent()+int32(len(digits)), digits, false)
}
