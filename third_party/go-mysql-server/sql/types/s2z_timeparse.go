// Copyright 2026 s2z. Licensed under the Apache License, Version 2.0.

package types

// s2z patch: MySQL's parsing of a TIME from a string.

// TimeStringStatus is how a string reads as a TIME.
type TimeStringStatus int

const (
	TimeStringOK         TimeStringStatus = iota
	TimeStringTruncated                   // a TIME, then characters that are not (MySQL: 1265)
	TimeStringOutOfRange                  // past 838:59:59 (MySQL: 1264)
	TimeStringInvalid                     // not a TIME
)

// ParseTimeString reads s as MySQL reads a TIME: [-][D ]H:M[:S[.f]] or a
// number [-]HHMMSS[.f] as text, a fraction past 6 digits rounded half up.
// It returns the TIME in microseconds (clamped to ±838:59:59), the index
// just past it, and the status.
func ParseTimeString(s string) (int64, int, TimeStringStatus) {
	i, n := 0, len(s)
	for i < n && s[i] == ' ' {
		i++
	}
	neg := false
	if i < n && s[i] == '-' {
		neg, i = true, i+1
	}
	digits := func() (int64, int) {
		start := i
		var v int64
		for i < n && s[i] >= '0' && s[i] <= '9' && i-start < 12 {
			v = v*10 + int64(s[i]-'0')
			i++
		}
		return v, i - start
	}
	first, nd := digits()
	if nd == 0 {
		return 0, i, TimeStringInvalid
	}
	var days, h, m, sec int64
	colon, dayOnly := false, false
	switch {
	case i+1 < n && s[i] == ' ' && s[i+1] >= '0' && s[i+1] <= '9':
		// D HH[:MM[:SS]]
		days = first
		i++
		h, _ = digits()
		if i >= n || s[i] != ':' {
			h, dayOnly = h+days*24, true
			break
		}
		colon = true
	case i < n && s[i] == ':':
		h, colon = first, true
	}
	if colon {
		if i >= n || s[i] != ':' {
			return 0, i, TimeStringInvalid
		}
		i++
		var md int
		m, md = digits()
		if md == 0 || md > 2 {
			return 0, i, TimeStringInvalid
		}
		if i < n && s[i] == ':' {
			i++
			var sd int
			sec, sd = digits()
			if sd == 0 || sd > 2 {
				return 0, i, TimeStringInvalid
			}
		}
		h += days * 24
	} else if !dayOnly {
		// a number as text: HHMMSS, MMSS or SS
		if nd > 7 {
			return 0, i, TimeStringInvalid
		}
		h, m, sec = first/10000, first/100%100, first%100
	}
	if m > 59 || sec > 59 {
		return 0, i, TimeStringInvalid
	}
	var us int64
	if i < n && s[i] == '.' {
		i++
		start := i
		for i < n && s[i] >= '0' && s[i] <= '9' {
			if d := i - start; d < 6 {
				us = us*10 + int64(s[i]-'0')
			} else if d == 6 && s[i] >= '5' {
				us++ // rounded half up at the microsecond
			}
			i++
		}
		for d := i - start; d < 6; d++ {
			us *= 10
		}
	}
	total := (h*3600+m*60+sec)*1000000 + us
	status := TimeStringOK
	for j := i; j < n; j++ {
		if s[j] != ' ' {
			status = TimeStringTruncated
			break
		}
	}
	if total > timespanMaximum {
		total, status = timespanMaximum, TimeStringOutOfRange
	}
	if neg {
		total = -total
	}
	return total, i, status
}
