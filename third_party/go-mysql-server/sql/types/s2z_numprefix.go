// Copyright 2026 s2z. Licensed under the Apache License, Version 2.0.

package types

// s2z patch: a string's numeric prefix, as MySQL reads a string as a number.

import (
	"fmt"
	"github.com/shopspring/decimal"
	"regexp"
	"strconv"
	"strings"
)

var numberPrefix = regexp.MustCompile(`^\s*[-+]?(\d+\.?\d*|\.\d+)([eE][-+]?\d+)?`)

// S2zNumberPrefix is the number at the start of s ("1abc" is "1", "A" is
// "0"), and whether s is not wholly a number (MySQL warns: Truncated
// incorrect ... value). A blank string is 0 without a warning.
func S2zNumberPrefix(s string) (string, bool) {
	t := strings.TrimSpace(s)
	if t == "" {
		return "0", false
	}
	if _, err := strconv.ParseFloat(t, 64); err == nil {
		return t, false
	}
	p := strings.TrimSpace(numberPrefix.FindString(s))
	if p == "" {
		return "0", true
	}
	return p, true
}

// S2zNumberValue is whether v is a number (integer, float or DECIMAL).
func S2zNumberValue(v interface{}) (interface{}, bool) {
	switch v.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, decimal.Decimal:
		return v, true
	}
	return nil, false
}

// S2zNumberDatetime is a number as MySQL reads it as a DATETIME:
// YYMMDD, YYYYMMDD, YYMMDDhhmmss or YYYYMMDDhhmmss with a fraction, as the
// text 'YYYY-MM-DD hh:mm:ss[.f]'; false when it is no datetime (0 is the
// zero date).
func S2zNumberDatetime(v interface{}) (string, bool) {
	var d decimal.Decimal
	switch x := v.(type) {
	case decimal.Decimal:
		d = x
	case float32:
		d = decimal.NewFromFloat32(x)
	case float64:
		d = decimal.NewFromFloat(x)
	case uint64:
		d = decimal.RequireFromString(strconv.FormatUint(x, 10))
	default:
		i, ok := toInt64Value(v)
		if !ok {
			return "", false
		}
		d = decimal.NewFromInt(i)
	}
	if d.Sign() < 0 {
		return "", false
	}
	if d.IsZero() {
		return "0000-00-00 00:00:00", true
	}
	n := d.IntPart()
	frac := d.Sub(decimal.NewFromInt(n))
	switch {
	case n < 101:
		return "", false
	case n <= 691231:
		n = (n + 20000000) * 1000000
	case n < 700101:
		return "", false
	case n <= 991231:
		n = (n + 19000000) * 1000000
	case n < 10000101:
		return "", false
	case n <= 99991231:
		n *= 1000000
	case n < 101000000:
		return "", false
	case n <= 691231235959:
		n += 20000000000000
	case n < 700101000000:
		return "", false
	case n <= 991231235959:
		n += 19000000000000
	case n > 99991231235959:
		return "", false
	}
	if !frac.IsZero() && n%1000000 == 0 && d.IntPart() <= 99991231 {
		return "", false // a date with a fraction
	}
	s := fmt.Sprintf("%014d", n)
	mo, _ := strconv.Atoi(s[4:6])
	day, _ := strconv.Atoi(s[6:8])
	h, _ := strconv.Atoi(s[8:10])
	mi, _ := strconv.Atoi(s[10:12])
	sec, _ := strconv.Atoi(s[12:14])
	if mo < 1 || mo > 12 || day < 1 || day > 31 || h > 23 || mi > 59 || sec > 59 {
		return "", false
	}
	out := fmt.Sprintf("%s-%s-%s %s:%s:%s", s[:4], s[4:6], s[6:8], s[8:10], s[10:12], s[12:14])
	if !frac.IsZero() {
		out += strings.TrimPrefix(frac.StringFixed(6), "0")
	}
	return out, true
}

func toInt64Value(v interface{}) (int64, bool) {
	switch x := v.(type) {
	case int:
		return int64(x), true
	case int8:
		return int64(x), true
	case int16:
		return int64(x), true
	case int32:
		return int64(x), true
	case int64:
		return x, true
	case uint:
		return int64(x), true
	case uint8:
		return int64(x), true
	case uint16:
		return int64(x), true
	case uint32:
		return int64(x), true
	}
	return 0, false
}

// S2zHasNumberPrefix is whether s starts with a number (after spaces).
func S2zHasNumberPrefix(s string) bool {
	return numberPrefix.MatchString(s)
}
