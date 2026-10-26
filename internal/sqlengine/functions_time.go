package sqlengine

import (
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/shopspring/decimal"
)

// Temporal functions go-mysql-server 0.20 lacks or gets wrong (issue #25):
// ADDTIME, SUBTIME, TIMEDIFF, TIME_TO_SEC, SEC_TO_TIME, MAKETIME, MAKEDATE,
// PERIOD_ADD, PERIOD_DIFF, UTC_DATE and UTC_TIME. A TIME result is returned
// as MySQL prints it, with the fractional digits its arguments give
// (go-mysql-server's TIME has no precision), so it is typed as text.

func init() {
	for name, f := range map[string]sql.Function{
		"addtime":     fnN("addtime", 2, 2, fixedType(types.LongText), false, func(ctx *sql.Context, a []sql.Expression, v []any) (any, error) { return addTime(ctx, a, v, 1) }),
		"subtime":     fnN("subtime", 2, 2, fixedType(types.LongText), false, func(ctx *sql.Context, a []sql.Expression, v []any) (any, error) { return addTime(ctx, a, v, -1) }),
		"timediff":    fnN("timediff", 2, 2, fixedType(types.LongText), false, evalTimeDiff),
		"time_to_sec": fnN("time_to_sec", 1, 1, fixedType(types.Int64), false, evalTimeToSec),
		"sec_to_time": fnN("sec_to_time", 1, 1, fixedType(types.LongText), false, evalSecToTime),
		"maketime":    fnN("maketime", 3, 3, fixedType(types.LongText), false, evalMakeTime),
		"makedate":    fnN("makedate", 2, 2, fixedType(types.Date), false, evalMakeDate),
		"period_add":  fnN("period_add", 2, 2, fixedType(types.Int64), false, evalPeriodAdd),
		"period_diff": fnN("period_diff", 2, 2, fixedType(types.Int64), false, evalPeriodDiff),
		"utc_date":    fnN("utc_date", 0, 0, fixedType(types.Date), false, evalUTCDate),
		"utc_time":    fnN("utc_time", 0, 1, fixedType(types.LongText), false, evalUTCTime),
	} {
		overrides[name] = f
	}
}

const (
	usPerSec  = int64(time.Second / time.Microsecond)
	maxTimeUS = (838*3600 + 59*60 + 59) * usPerSec // 838:59:59
)

// temporal is a TIME (a signed span) or a DATETIME argument.
type temporal struct {
	isDT  bool
	dt    time.Time // isDT
	us    int64     // !isDT: microseconds, clamped to ±838:59:59
	scale int       // fractional digits the argument's type gives
	text  bool      // a string: shows 6 digits, or none when they are 0
	over  bool      // !isDT: beyond ±838:59:59, clamped
}

var (
	reDatetime = regexp.MustCompile(`^(\d{1,4})[^\d](\d{1,2})[^\d](\d{1,2})[ T]+(\d{1,2})(?:[^\d](\d{1,2})(?:[^\d](\d{1,2}))?)?(?:\.(\d*))?$`)
	reTime     = regexp.MustCompile(`^(-)?(?:(\d+) +)?(\d+)(?::(\d{1,2})(?::(\d{1,2}))?)?(?:\.(\d*))?`)
)

// fracUS is MySQL's microseconds for a fraction's digits: the first six,
// rounded on the seventh.
func fracUS(f string) int64 {
	if f == "" {
		return 0
	}
	d := (f + "000000")[:6]
	n, _ := strconv.ParseInt(d, 10, 64)
	if len(f) > 6 && f[6] >= '5' {
		n++
	}
	return n
}

func clampTime(us int64) int64 {
	return max(-maxTimeUS, min(maxTimeUS, us))
}

// parseTemporal reads a string as MySQL's temporal functions do: a date and
// a time as a DATETIME, anything else as a TIME.
func parseTemporal(s string) (temporal, bool) {
	s = strings.TrimSpace(s)
	if m := reDatetime.FindStringSubmatch(s); m != nil {
		n := func(i int) int { x, _ := strconv.Atoi(m[i]); return x }
		y := n(1)
		if len(m[1]) <= 2 {
			if y < 70 {
				y += 2000
			} else {
				y += 1900
			}
		}
		mo, d, h, mi, sec := n(2), n(3), n(4), n(5), n(6)
		if mo < 1 || mo > 12 || d < 1 || h > 23 || mi > 59 || sec > 59 {
			return temporal{}, false
		}
		t := time.Date(y, time.Month(mo), d, h, mi, sec, 0, time.UTC)
		if t.Day() != d {
			return temporal{}, false
		}
		t = t.Add(time.Duration(fracUS(m[7])) * time.Microsecond)
		return temporal{isDT: true, dt: t, scale: 6, text: true}, true
	}
	m := reTime.FindStringSubmatch(s)
	if m == nil {
		return temporal{}, false
	}
	var h, mi, sec int64
	if m[4] == "" && m[2] == "" && len(m[0]) < len(s) && m[6] == "" {
		h, _ = strconv.ParseInt(m[3], 10, 64) // 2001-01-01: the hours, as MySQL reads it
	} else if m[4] == "" && m[2] == "" { // [hh]mmss
		digits := m[3]
		part := func(from, to int) int64 {
			l := len(digits)
			a, b := max(0, l-from), max(0, l-to)
			x, _ := strconv.ParseInt(digits[a:b], 10, 64)
			return x
		}
		h, mi, sec = part(100, 4), part(4, 2), part(2, 0)
	} else {
		h, _ = strconv.ParseInt(m[3], 10, 64)
		mi, _ = strconv.ParseInt(m[4], 10, 64)
		sec, _ = strconv.ParseInt(m[5], 10, 64)
		if m[2] != "" {
			d, _ := strconv.ParseInt(m[2], 10, 64)
			h += 24 * d
		}
	}
	if mi > 59 || sec > 59 {
		return temporal{}, false
	}
	h = min(h, 1000) // anything above 838 clamps
	us := ((h*60+mi)*60+sec)*usPerSec + fracUS(m[6])
	if m[1] != "" {
		us = -us
	}
	return temporal{us: clampTime(us), scale: 6, text: true, over: clampTime(us) != us}, true
}

// typeScale is the fractional digits of a DATETIME or TIMESTAMP type.
func typeScale(t sql.Type) int {
	if dt, ok := t.(sql.DatetimeType); ok {
		return dt.Precision()
	}
	return 0
}

// toTemporal converts a function argument.
func toTemporal(ctx *sql.Context, arg sql.Expression, v any) (temporal, bool) {
	switch x := v.(type) {
	case time.Time:
		return temporal{isDT: true, dt: x, scale: typeScale(arg.Type())}, true
	case types.Timespan:
		us := x.AsMicroseconds()
		return temporal{us: us, scale: 6, text: true}, true
	case string, []byte:
		s, _ := fnString(ctx, x)
		if types.IsTime(arg.Type()) { // a DATE or TIMESTAMP literal
			if t, _, err := types.DatetimeMaxPrecision.Convert(ctx, s); err == nil {
				return temporal{isDT: true, dt: t.(time.Time), scale: typeScale(arg.Type())}, true
			}
		}
		return parseTemporal(s)
	case decimal.Decimal:
		t, ok := parseTemporal(x.String())
		t.scale, t.text = min(6, int(max(0, -x.Exponent()))), false
		return t, ok
	case float32, float64:
		f, _ := fnFloat(x)
		t, ok := parseTemporal(strconv.FormatFloat(f, 'f', -1, 64))
		return t, ok
	}
	i, err := fnInt64(ctx, v)
	if err != nil {
		return temporal{}, false
	}
	t, ok := parseTemporal(strconv.FormatInt(i, 10))
	t.scale, t.text = 0, false
	return t, ok
}

func fnFloat(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case float32:
		return float64(x), true
	}
	return 0, false
}

// resultScale is the fractional digits a result of these arguments shows.
func resultScale(us int64, args ...temporal) int {
	s := 0
	for _, a := range args {
		if a.text {
			if us%usPerSec != 0 {
				s = 6
			}
			continue
		}
		s = max(s, a.scale)
	}
	return s
}

func formatFrac(b *strings.Builder, us int64, scale int) {
	if scale == 0 {
		return
	}
	f := strconv.FormatInt(1000000+us%usPerSec, 10)[1:]
	b.WriteByte('.')
	b.WriteString(f[:scale])
}

func formatTime(us int64, scale int) string {
	var b strings.Builder
	if us < 0 {
		b.WriteByte('-')
		us = -us
	}
	sec := us / usPerSec
	h := strconv.FormatInt(sec/3600, 10)
	if len(h) < 2 {
		h = "0" + h
	}
	b.WriteString(h)
	b.WriteString(time.Unix(sec%3600, 0).UTC().Format(":04:05"))
	formatFrac(&b, us, scale)
	return b.String()
}

func formatDatetime(t time.Time, scale int) string {
	var b strings.Builder
	b.WriteString(t.Format("2006-01-02 15:04:05"))
	formatFrac(&b, int64(t.Nanosecond())/1000, scale)
	return b.String()
}

// roundScale rounds microseconds to scale fractional digits.
func roundScale(us int64, scale int) int64 {
	p := int64(math.Pow10(6 - scale))
	if us < 0 {
		return -roundScale(-us, scale)
	}
	return (us + p/2) / p * p
}

func validDatetime(t time.Time) bool {
	return t.Year() >= 0 && t.Year() <= 9999
}

func addTime(ctx *sql.Context, args []sql.Expression, v []any, sign int64) (any, error) {
	a, ok1 := toTemporal(ctx, args[0], v[0])
	b, ok2 := toTemporal(ctx, args[1], v[1])
	if !ok1 || !ok2 || b.isDT {
		return nil, nil
	}
	if a.isDT {
		t := a.dt.Add(time.Duration(sign*b.us) * time.Microsecond)
		if !validDatetime(t) {
			return nil, nil
		}
		return formatDatetime(t, resultScale(int64(t.Nanosecond())/1000, a, b)), nil
	}
	us := clampTime(a.us + sign*b.us)
	return formatTime(us, resultScale(us, a, b)), nil
}

func evalTimeDiff(ctx *sql.Context, args []sql.Expression, v []any) (any, error) {
	a, ok1 := toTemporal(ctx, args[0], v[0])
	b, ok2 := toTemporal(ctx, args[1], v[1])
	if !ok1 || !ok2 || a.isDT != b.isDT {
		return nil, nil
	}
	var us int64
	if a.isDT {
		us = a.dt.Sub(b.dt).Microseconds()
	} else {
		us = a.us - b.us
	}
	us = clampTime(us)
	return formatTime(us, resultScale(us, a, b)), nil
}

func evalTimeToSec(ctx *sql.Context, args []sql.Expression, v []any) (any, error) {
	t, ok := toTemporal(ctx, args[0], v[0])
	if !ok {
		return nil, nil
	}
	if t.isDT {
		h, m, s := t.dt.Clock()
		return int64(h*3600 + m*60 + s), nil
	}
	return t.us / usPerSec, nil
}

// fnDecimal converts a numeric argument, with the fractional digits its
// type shows: an integer none, a decimal its scale, others six.
func fnDecimal(ctx *sql.Context, v any) (decimal.Decimal, int, error) {
	switch x := v.(type) {
	case decimal.Decimal:
		return x, min(6, int(max(0, -x.Exponent()))), nil
	case float32, float64:
		f, _ := fnFloat(x)
		return decimal.NewFromFloat(f), 6, nil
	case string, []byte:
		s, _ := fnString(ctx, x)
		return decimal.NewFromFloat(stringPrefixFloat(s)), 6, nil
	}
	if u, ok := v.(uint64); ok {
		return decimal.NewFromUint64(u), 0, nil
	}
	i, err := fnInt64(ctx, v)
	return decimal.NewFromInt(i), 0, err
}

var reFloatPrefix = regexp.MustCompile(`^\s*[-+]?(\d+\.?\d*|\.\d+)([eE][-+]?\d+)?`)

// stringPrefixFloat is a string's value as a number: its numeric prefix.
func stringPrefixFloat(s string) float64 {
	f, _ := strconv.ParseFloat(strings.TrimSpace(reFloatPrefix.FindString(s)), 64)
	return f
}

func evalSecToTime(ctx *sql.Context, _ []sql.Expression, v []any) (any, error) {
	d, scale, err := fnDecimal(ctx, v[0])
	if err != nil {
		return nil, err
	}
	us := d.Mul(decimal.NewFromInt(usPerSec)).Round(0)
	var n int64
	switch {
	case us.GreaterThan(decimal.NewFromInt(maxTimeUS)):
		n, scale = maxTimeUS, 0
	case us.LessThan(decimal.NewFromInt(-maxTimeUS)):
		n, scale = -maxTimeUS, 0
	default:
		n = roundScale(us.IntPart(), scale)
	}
	return formatTime(n, scale), nil
}

func evalMakeTime(ctx *sql.Context, _ []sql.Expression, v []any) (any, error) {
	hd, _, err := fnDecimal(ctx, v[0])
	if err != nil {
		return nil, err
	}
	md, _, err := fnDecimal(ctx, v[1])
	if err != nil {
		return nil, err
	}
	sd, scale, err := fnDecimal(ctx, v[2])
	if err != nil {
		return nil, err
	}
	h, mi := hd.Round(0).IntPart(), md.Round(0).IntPart()
	if mi < 0 || mi > 59 || sd.IsNegative() || sd.GreaterThanOrEqual(decimal.NewFromInt(60)) {
		return nil, nil
	}
	neg := h < 0
	h = min(max(h, -h), 1000)
	us := (h*60+mi)*60*usPerSec + roundScale(sd.Mul(decimal.NewFromInt(usPerSec)).Round(0).IntPart(), scale)
	if neg {
		us = -us
	}
	if us > maxTimeUS || us < -maxTimeUS {
		us, scale = clampTime(us), 0
	}
	return formatTime(us, scale), nil
}

// fnRoundInt is a numeric argument rounded to an integer, as MySQL passes a
// decimal to an integer parameter.
func fnRoundInt(ctx *sql.Context, v any) (int64, error) {
	d, _, err := fnDecimal(ctx, v)
	if err != nil {
		return 0, err
	}
	return d.Round(0).IntPart(), nil
}

func evalMakeDate(ctx *sql.Context, _ []sql.Expression, v []any) (any, error) {
	y, err := fnRoundInt(ctx, v[0])
	if err != nil {
		return nil, err
	}
	d, err := fnRoundInt(ctx, v[1])
	if err != nil {
		return nil, err
	}
	if d <= 0 || y < 0 || y > 9999 {
		return nil, nil
	}
	if y < 70 {
		y += 2000
	} else if y < 100 {
		y += 1900
	}
	t := time.Date(int(y), 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, int(min(d, 4000000)-1))
	if t.Year() > 9999 {
		return nil, nil
	}
	return t.Format("2006-01-02"), nil // as go-mysql-server's DATE() returns
}

// periodMonths is a YYMM or YYYYMM period as a month count.
func periodMonths(ctx *sql.Context, name string, v any) (int64, error) {
	p, err := fnRoundInt(ctx, v)
	if err != nil {
		return 0, err
	}
	if p <= 0 || p%100 < 1 || p%100 > 12 {
		return 0, mysql.NewSQLError(1210, "HY000", "Incorrect arguments to %s", name)
	}
	y := p / 100
	if y < 70 {
		y += 2000
	} else if y < 100 {
		y += 1900
	}
	return y*12 + p%100 - 1, nil
}

func evalPeriodAdd(ctx *sql.Context, _ []sql.Expression, v []any) (any, error) {
	m, err := periodMonths(ctx, "period_add", v[0])
	if err != nil {
		return nil, err
	}
	n, err := fnRoundInt(ctx, v[1])
	if err != nil {
		return nil, err
	}
	m += n
	return m/12*100 + m%12 + 1, nil
}

func evalPeriodDiff(ctx *sql.Context, _ []sql.Expression, v []any) (any, error) {
	a, err := periodMonths(ctx, "period_diff", v[0])
	if err != nil {
		return nil, err
	}
	b, err := periodMonths(ctx, "period_diff", v[1])
	if err != nil {
		return nil, err
	}
	return a - b, nil
}

func utcNow(ctx *sql.Context) time.Time {
	return ctx.QueryTime().UTC()
}

func evalUTCDate(ctx *sql.Context, _ []sql.Expression, _ []any) (any, error) {
	y, m, d := utcNow(ctx).Date()
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC).Format("2006-01-02"), nil
}

func evalUTCTime(ctx *sql.Context, _ []sql.Expression, v []any) (any, error) {
	fsp := int64(0)
	if len(v) == 1 {
		var err error
		if fsp, err = fnInt64(ctx, v[0]); err != nil {
			return nil, err
		}
		if fsp > 6 {
			return nil, mysql.NewSQLError(1426, "42000", "Too-big precision %d specified for 'utc_time'. Maximum is 6.", fsp)
		}
	}
	t := utcNow(ctx)
	h, m, s := t.Clock()
	us := int64(h*3600+m*60+s)*usPerSec + int64(t.Nanosecond())/1000
	p := int64(math.Pow10(6 - int(fsp)))
	return formatTime(us/p*p, int(fsp)), nil
}
