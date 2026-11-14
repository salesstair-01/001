// Copyright 2026 s2z. Licensed under the Apache License, Version 2.0.

package types

// s2z patch: a string's numeric prefix, as MySQL reads a string as a number.

import (
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
