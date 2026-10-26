package sqlengine

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/shopspring/decimal"
)

// MySQL functions go-mysql-server 0.20 lacks (MySQL's test suite, issue
// #26), registered as overrides in functions.go. Each is a scalarFn: its
// arguments are evaluated (NULL in, NULL out unless nullOK) and passed to
// eval.

type scalarFn struct {
	name   string
	args   []sql.Expression
	typ    func(args []sql.Expression) sql.Type
	nullOK bool // eval sees NULL arguments
	eval   func(ctx *sql.Context, args []sql.Expression, vals []any) (any, error)
}

var _ sql.FunctionExpression = (*scalarFn)(nil)

func (f *scalarFn) FunctionName() string { return f.name }
func (f *scalarFn) Description() string  { return f.name }
func (f *scalarFn) Resolved() bool {
	for _, a := range f.args {
		if !a.Resolved() {
			return false
		}
	}
	return true
}
func (f *scalarFn) Type() sql.Type             { return f.typ(f.args) }
func (f *scalarFn) IsNullable() bool           { return true }
func (f *scalarFn) Children() []sql.Expression { return f.args }
func (f *scalarFn) IsNonDeterministic() bool   { return f.name == "uuid_short" }
func (f *scalarFn) WithChildren(c ...sql.Expression) (sql.Expression, error) {
	if len(c) != len(f.args) {
		return nil, sql.ErrInvalidChildrenNumber.New(f, len(c), len(f.args))
	}
	n := *f
	n.args = c
	return &n, nil
}

func (f *scalarFn) String() string {
	parts := make([]string, len(f.args))
	for i, a := range f.args {
		parts[i] = a.String()
	}
	return f.name + "(" + strings.Join(parts, ",") + ")"
}

func (f *scalarFn) Eval(ctx *sql.Context, row sql.Row) (any, error) {
	vals := make([]any, len(f.args))
	for i, a := range f.args {
		v, err := a.Eval(ctx, row)
		if err != nil {
			return nil, err
		}
		if v == nil && !f.nullOK {
			return nil, nil
		}
		vals[i] = v
	}
	return f.eval(ctx, f.args, vals)
}

// fnN registers a function taking min to max arguments.
func fnN(name string, min, max int, typ func([]sql.Expression) sql.Type, nullOK bool, eval func(*sql.Context, []sql.Expression, []any) (any, error)) sql.Function {
	return sql.FunctionN{Name: name, Fn: func(args ...sql.Expression) (sql.Expression, error) {
		if len(args) < min || (max >= 0 && len(args) > max) {
			return nil, mysql.NewSQLError(1582, "42000", "Incorrect parameter count in the call to native function '%s'", name)
		}
		return &scalarFn{name: name, args: args, typ: typ, nullOK: nullOK, eval: eval}, nil
	}}
}

func fixedType(t sql.Type) func([]sql.Expression) sql.Type {
	return func([]sql.Expression) sql.Type { return t }
}

func fnString(ctx *sql.Context, v any) (string, error) {
	switch x := v.(type) {
	case string:
		return x, nil
	case []byte:
		return string(x), nil
	}
	s, _, err := types.LongText.Convert(ctx, v)
	if err != nil {
		return "", err
	}
	return fmt.Sprint(s), nil
}

func fnInt64(ctx *sql.Context, v any) (int64, error) {
	if u, ok := v.(uint64); ok {
		return int64(u), nil
	}
	i, _, err := types.Int64.Convert(ctx, v)
	if err != nil {
		return 0, err
	}
	return i.(int64), nil
}

func toUint64Bits(ctx *sql.Context, v any) (uint64, error) {
	if u, ok := v.(uint64); ok {
		return u, nil
	}
	i, err := fnInt64(ctx, v)
	return uint64(i), err
}

var extraFunctions = map[string]sql.Function{
	"truncate":    fnN("truncate", 2, 2, truncateType, false, evalTruncate),
	"insert":      fnN("insert", 4, 4, fixedType(types.LongText), false, evalInsert),
	"export_set":  fnN("export_set", 3, 5, fixedType(types.LongText), false, evalExportSet),
	"make_set":    fnN("make_set", 2, -1, fixedType(types.LongText), true, evalMakeSet),
	"aes_encrypt": fnN("aes_encrypt", 2, 3, fixedType(types.LongBlob), false, func(ctx *sql.Context, _ []sql.Expression, v []any) (any, error) { return aesCrypt(ctx, v, true) }),
	"aes_decrypt": fnN("aes_decrypt", 2, 3, fixedType(types.LongBlob), false, func(ctx *sql.Context, _ []sql.Expression, v []any) (any, error) { return aesCrypt(ctx, v, false) }),
	"uuid_short":  fnN("uuid_short", 0, 0, fixedType(types.Uint64), false, evalUUIDShort),
	"benchmark":   fnN("benchmark", 2, 2, fixedType(types.Int64), true, evalBenchmark),
}

func init() {
	for name, f := range extraFunctions {
		overrides[name] = f
	}
}

// ---- TRUNCATE(X, D) -------------------------------------------------------------

// truncateType is X's type: an integer stays an integer, a float a double,
// anything else (decimals, strings as doubles) as MySQL types it.
func truncateType(args []sql.Expression) sql.Type {
	t := args[0].Type()
	switch {
	case types.IsInteger(t):
		if types.IsUnsigned(t) {
			return types.Uint64
		}
		return types.Int64
	case types.IsDecimal(t):
		return t
	}
	return types.Float64
}

func evalTruncate(ctx *sql.Context, args []sql.Expression, v []any) (any, error) {
	d, err := fnInt64(ctx, v[1])
	if err != nil {
		return nil, err
	}
	t := truncateType(args)
	var x decimal.Decimal
	switch n := v[0].(type) {
	case decimal.Decimal:
		x = n
	case float64:
		x, err = decimal.NewFromString(strconv.FormatFloat(n, 'g', -1, 64))
	case float32:
		x, err = decimal.NewFromString(strconv.FormatFloat(float64(n), 'g', -1, 32))
	default:
		var c any
		c, _, err = types.InternalDecimalType.Convert(ctx, v[0])
		if err == nil {
			x = c.(decimal.Decimal)
		}
	}
	if err != nil {
		return nil, err
	}
	if d >= 0 {
		x = x.Truncate(int32(min(d, 30)))
	} else {
		p := decimal.New(1, int32(min(-d, 70)))
		x = x.Div(p).Truncate(0).Mul(p)
	}
	switch {
	case types.IsInteger(t) && types.IsUnsigned(t):
		u, _ := strconv.ParseUint(x.String(), 10, 64)
		return u, nil
	case types.IsInteger(t):
		return x.IntPart(), nil
	case types.IsDecimal(t):
		return x, nil
	}
	f, _ := x.Float64()
	return f, nil
}

// ---- INSERT(str, pos, len, newstr) ------------------------------------------------

func evalInsert(ctx *sql.Context, _ []sql.Expression, v []any) (any, error) {
	s, err := fnString(ctx, v[0])
	if err != nil {
		return nil, err
	}
	pos, err := fnInt64(ctx, v[1])
	if err != nil {
		return nil, err
	}
	n, err := fnInt64(ctx, v[2])
	if err != nil {
		return nil, err
	}
	ns, err := fnString(ctx, v[3])
	if err != nil {
		return nil, err
	}
	r := []rune(s)
	if pos < 1 || pos > int64(len(r)) {
		return s, nil
	}
	start := pos - 1
	end := int64(len(r))
	if n >= 0 && start+n < end {
		end = start + n
	}
	return string(r[:start]) + ns + string(r[end:]), nil
}

// ---- EXPORT_SET and MAKE_SET ---------------------------------------------------------

func evalExportSet(ctx *sql.Context, _ []sql.Expression, v []any) (any, error) {
	bits, err := toUint64Bits(ctx, v[0])
	if err != nil {
		return nil, err
	}
	on, err := fnString(ctx, v[1])
	if err != nil {
		return nil, err
	}
	off, err := fnString(ctx, v[2])
	if err != nil {
		return nil, err
	}
	sep, n := ",", int64(64)
	if len(v) > 3 {
		if sep, err = fnString(ctx, v[3]); err != nil {
			return nil, err
		}
	}
	if len(v) > 4 {
		if n, err = fnInt64(ctx, v[4]); err != nil {
			return nil, err
		}
		if n < 0 || n > 64 {
			n = 64
		}
	}
	parts := make([]string, n)
	for i := range parts {
		if bits&(1<<uint(i)) != 0 {
			parts[i] = on
		} else {
			parts[i] = off
		}
	}
	return strings.Join(parts, sep), nil
}

func evalMakeSet(ctx *sql.Context, _ []sql.Expression, v []any) (any, error) {
	if v[0] == nil {
		return nil, nil
	}
	bits, err := toUint64Bits(ctx, v[0])
	if err != nil {
		return nil, err
	}
	var parts []string
	for i, s := range v[1:] {
		if i >= 64 || bits&(1<<uint(i)) == 0 || s == nil {
			continue
		}
		str, err := fnString(ctx, s)
		if err != nil {
			return nil, err
		}
		parts = append(parts, str)
	}
	return strings.Join(parts, ","), nil
}

// ---- AES_ENCRYPT / AES_DECRYPT ----------------------------------------------------------

// aesCrypt is MySQL's AES_ENCRYPT / AES_DECRYPT in @@block_encryption_mode
// (aes-{128,192,256}-{ecb,cbc,cfb128,ofb}): the key is folded into the key
// length by XOR, ECB and CBC pad with PKCS#7, and the other modes need the
// 16-byte initialization vector as a third argument. A wrong key or
// padding decrypts to NULL.
func aesCrypt(ctx *sql.Context, v []any, encrypt bool) (any, error) {
	name := "aes_decrypt"
	if encrypt {
		name = "aes_encrypt"
	}
	mode := "aes-128-ecb"
	if m, err := ctx.GetSessionVariable(ctx, "block_encryption_mode"); err == nil && m != nil {
		mode = strings.ToLower(fmt.Sprint(m))
	}
	var bitsN int
	var chain string
	if _, err := fmt.Sscanf(mode, "aes-%d-%s", &bitsN, &chain); err != nil || (bitsN != 128 && bitsN != 192 && bitsN != 256) {
		return nil, fmt.Errorf("unsupported block_encryption_mode %q", mode)
	}
	needIV := chain != "ecb"
	if needIV && len(v) < 3 {
		return nil, mysql.NewSQLError(1582, "42000", "Incorrect parameter count in the call to native function '%s'", name)
	}
	data, err := fnString(ctx, v[0])
	if err != nil {
		return nil, err
	}
	keyStr, err := fnString(ctx, v[1])
	if err != nil {
		return nil, err
	}
	key := make([]byte, bitsN/8)
	for i, b := range []byte(keyStr) {
		key[i%len(key)] ^= b
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	var iv []byte
	if needIV {
		ivStr, err := fnString(ctx, v[2])
		if err != nil {
			return nil, err
		}
		if len(ivStr) < aes.BlockSize {
			return nil, mysql.NewSQLError(1876, "HY000", "The initialization vector supplied to %s is too short. Must be at least %d bytes long", name, aes.BlockSize)
		}
		iv = []byte(ivStr[:aes.BlockSize])
	}
	in := []byte(data)
	switch chain {
	case "ecb", "cbc":
		if encrypt {
			pad := aes.BlockSize - len(in)%aes.BlockSize
			in = append(append([]byte(nil), in...), bytes.Repeat([]byte{byte(pad)}, pad)...)
		} else if len(in) == 0 || len(in)%aes.BlockSize != 0 {
			return nil, nil
		}
		out := make([]byte, len(in))
		for i := 0; i < len(in); i += aes.BlockSize {
			switch {
			case chain == "ecb" && encrypt:
				block.Encrypt(out[i:], in[i:])
			case chain == "ecb":
				block.Decrypt(out[i:], in[i:])
			}
		}
		if chain == "cbc" {
			if encrypt {
				cipher.NewCBCEncrypter(block, iv).CryptBlocks(out, in)
			} else {
				cipher.NewCBCDecrypter(block, iv).CryptBlocks(out, in)
			}
		}
		if encrypt {
			return out, nil
		}
		pad := int(out[len(out)-1])
		if pad < 1 || pad > aes.BlockSize || !bytes.Equal(out[len(out)-pad:], bytes.Repeat([]byte{byte(pad)}, pad)) {
			return nil, nil
		}
		return out[:len(out)-pad], nil
	case "cfb128", "ofb":
		out := make([]byte, len(in))
		var s cipher.Stream
		switch {
		case chain == "ofb":
			s = cipher.NewOFB(block, iv)
		case encrypt:
			s = cipher.NewCFBEncrypter(block, iv)
		default:
			s = cipher.NewCFBDecrypter(block, iv)
		}
		s.XORKeyStream(out, in)
		return out, nil
	}
	return nil, fmt.Errorf("unsupported block_encryption_mode %q", mode)
}

// ---- UUID_SHORT, BENCHMARK ----------------------------------------------------------------

var (
	uuidShortStart = uint64(time.Now().Unix())
	uuidShortSeq   atomic.Uint64
)

// evalUUIDShort is MySQL's (server_id & 255) << 56 + (start time << 24) +
// a counter, with server_id 0.
func evalUUIDShort(*sql.Context, []sql.Expression, []any) (any, error) {
	return (uuidShortStart<<24)&(math.MaxUint64>>8) + uuidShortSeq.Add(1), nil
}

// evalBenchmark evaluates its expression count times and returns 0.
func evalBenchmark(ctx *sql.Context, args []sql.Expression, v []any) (any, error) {
	if v[0] == nil {
		return nil, nil
	}
	n, err := fnInt64(ctx, v[0])
	if err != nil {
		return nil, err
	}
	if n < 0 {
		return nil, mysql.NewSQLError(1210, "HY000", "Incorrect arguments to benchmark")
	}
	for i := int64(0); i < n; i++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if _, err := args[1].Eval(ctx, nil); err != nil {
			return nil, err
		}
	}
	return int64(0), nil
}
