package sqlengine

import (
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/sqltypes"
)

// DATE '...', TIME '...' and TIMESTAMP '...' (issue #25). The parser makes
// them calls of s2z_date_literal and the like (third_party/vitess); each is
// a value of its type, and one MySQL can't read is an error (1525), not
// NULL. A TIMESTAMP literal has the precision of its fraction.

func init() {
	for _, kind := range []string{"date", "time", "timestamp"} {
		kind := kind
		name := "s2z_" + kind + "_literal"
		overrides[name] = sql.Function1{Name: name, Fn: func(e sql.Expression) sql.Expression {
			return &typedLiteral{kind: kind, arg: e}
		}}
	}
}

type typedLiteral struct {
	kind string
	arg  sql.Expression
}

var _ sql.FunctionExpression = (*typedLiteral)(nil)

func (l *typedLiteral) FunctionName() string       { return "s2z_" + l.kind + "_literal" }
func (l *typedLiteral) Description() string        { return l.kind + " literal" }
func (l *typedLiteral) Resolved() bool             { return l.arg.Resolved() }
func (l *typedLiteral) IsNullable() bool           { return false }
func (l *typedLiteral) Children() []sql.Expression { return []sql.Expression{l.arg} }
func (l *typedLiteral) String() string             { return strings.ToUpper(l.kind) + l.arg.String() }
func (l *typedLiteral) IsNonDeterministic() bool   { return false }
func (l *typedLiteral) WithChildren(c ...sql.Expression) (sql.Expression, error) {
	if len(c) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(l, len(c), 1)
	}
	return &typedLiteral{kind: l.kind, arg: c[0]}, nil
}

func (l *typedLiteral) text() string {
	v, err := l.arg.Eval(nil, nil)
	if err != nil {
		return ""
	}
	s, _ := v.(string)
	return s
}

func (l *typedLiteral) Type() sql.Type {
	switch l.kind {
	case "date":
		return types.Date
	case "time":
		return types.Time
	}
	_, frac, _ := strings.Cut(l.text(), ".")
	return types.MustCreateDatetimeType(sqltypes.Datetime, min(6, len(frac)))
}

func (l *typedLiteral) Eval(ctx *sql.Context, row sql.Row) (any, error) {
	s := l.text()
	bad := mysql.NewSQLError(1525, "HY000", "Incorrect %s value: '%s'", strings.ToUpper(map[string]string{"timestamp": "datetime"}[l.kind]+map[bool]string{true: l.kind}[l.kind != "timestamp"]), s)
	switch l.kind {
	case "time":
		t, ok := parseTemporal(s)
		if !ok || t.isDT || t.over {
			return nil, bad
		}
		return formatTime(t.us, resultScale(t.us, t)), nil // a string: go-mysql-server casts and compares Timespans badly
	case "date":
		t, ok := parseDateLiteral(s)
		if !ok {
			return nil, bad
		}
		return t, nil
	}
	t, ok := parseTimestampLiteral(s)
	if !ok {
		return nil, bad
	}
	return t, nil
}

var (
	reDateLit = regexp.MustCompile(`^(\d{1,4})[[:punct:]](\d{1,2})[[:punct:]](\d{1,2})`)
	reTimeLit = regexp.MustCompile(`^[ T]+(\d{1,2})(?:[[:punct:]](\d{1,2})(?:[[:punct:]](\d{1,2}))?)?(?:\.(\d*))?$`)
)

// literalDate reads the date at the start of s: Y-M-D with any punctuation
// between, or YYYYMMDD, YYMMDD; the rest of s after it.
func literalDate(s string) (time.Time, string, bool) {
	var y, mo, d int
	var ylen int
	rest := ""
	if m := reDateLit.FindStringSubmatch(s); m != nil {
		y, _ = strconv.Atoi(m[1])
		mo, _ = strconv.Atoi(m[2])
		d, _ = strconv.Atoi(m[3])
		ylen, rest = len(m[1]), s[len(m[0]):]
	} else {
		digits, after, _ := strings.Cut(s, " ")
		if !allDigits(digits) || (len(digits) != 6 && len(digits) != 8) {
			return time.Time{}, "", false
		}
		ylen = len(digits) - 4
		y, _ = strconv.Atoi(digits[:ylen])
		mo, _ = strconv.Atoi(digits[ylen : ylen+2])
		d, _ = strconv.Atoi(digits[ylen+2:])
		if after != "" {
			rest = " " + after
		}
	}
	if ylen <= 2 {
		if y < 70 {
			y += 2000
		} else {
			y += 1900
		}
	}
	t := time.Date(y, time.Month(mo), d, 0, 0, 0, 0, time.UTC)
	if mo < 1 || mo > 12 || d < 1 || t.Day() != d {
		return time.Time{}, "", false
	}
	return t, rest, true
}

func parseDateLiteral(s string) (time.Time, bool) {
	t, rest, ok := literalDate(s)
	return t, ok && rest == ""
}

func parseTimestampLiteral(s string) (time.Time, bool) {
	t, rest, ok := literalDate(s)
	if !ok {
		return time.Time{}, false
	}
	m := reTimeLit.FindStringSubmatch(rest)
	if m == nil {
		return time.Time{}, false
	}
	n := func(i int) int { x, _ := strconv.Atoi(m[i]); return x }
	h, mi, sec := n(1), n(2), n(3)
	if h > 23 || mi > 59 || sec > 59 {
		return time.Time{}, false
	}
	us := int64((h*60+mi)*60+sec)*usPerSec + fracUS(m[4])
	return t.Add(time.Duration(us) * time.Microsecond), true
}
