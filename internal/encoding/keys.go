// Package encoding holds the key and row encodings shared by the query engine
// and the KV owner (decisions D2 and D3). Both sides must produce byte-identical
// keys, so every encoding lives here and nowhere else.
package encoding

import (
	"encoding/binary"
	"errors"
	"math"
)

// PrefixLen is the fixed length of every key prefix: partition (8) · table (4) · index (4).
const PrefixLen = 16

// TSLen is the length of the inverted commit_ts suffix on every stored key.
const TSLen = 8

// Reserved system table ids. User tables start at FirstUserTableID.
const (
	TableCatalog       uint32 = 1
	TableStatistics    uint32 = 2
	TableAutoIncrement uint32 = 3
	TablePartitionMeta uint32 = 4

	FirstUserTableID uint32 = 1024
)

// PrimaryIndexID is the index id of a table's clustered primary index.
const PrimaryIndexID uint32 = 0

// Column marker bytes.
const (
	MarkerNull  byte = 0x00
	MarkerValue byte = 0x01
)

// String escaping: 0x00 is written as 00 FF and every string ends with 00 01.
const (
	escByte   byte = 0x00
	escEscape byte = 0xFF
	escTerm   byte = 0x01
)

var ErrCorrupt = errors.New("encoding: corrupt key")

// AppendPrefix appends the 16-byte partition · table · index prefix.
func AppendPrefix(b []byte, partition uint64, table, index uint32) []byte {
	b = binary.BigEndian.AppendUint64(b, partition)
	b = binary.BigEndian.AppendUint32(b, table)
	b = binary.BigEndian.AppendUint32(b, index)
	return b
}

// Prefix returns a new key holding only the prefix.
func Prefix(partition uint64, table, index uint32) []byte {
	return AppendPrefix(make([]byte, 0, PrefixLen), partition, table, index)
}

// DecodePrefix splits a key into its prefix fields and the remainder.
func DecodePrefix(key []byte) (partition uint64, table, index uint32, rest []byte, err error) {
	if len(key) < PrefixLen {
		return 0, 0, 0, nil, ErrCorrupt
	}
	return binary.BigEndian.Uint64(key), binary.BigEndian.Uint32(key[8:]), binary.BigEndian.Uint32(key[12:]), key[PrefixLen:], nil
}

// PartitionPrefix returns the 8-byte prefix that every key of a partition starts with.
func PartitionPrefix(partition uint64) []byte {
	return binary.BigEndian.AppendUint64(nil, partition)
}

// PrefixEnd returns the smallest key greater than every key that starts with p.
// It returns nil when no such key exists (p is all 0xFF).
func PrefixEnd(p []byte) []byte {
	end := append([]byte(nil), p...)
	for i := len(end) - 1; i >= 0; i-- {
		end[i]++
		if end[i] != 0 {
			return end[:i+1]
		}
	}
	return nil
}

// ---- column encodings -------------------------------------------------------

// appendCol runs f and, for a DESC column, inverts every byte it wrote, marker included.
type colAppender func([]byte) []byte

func appendCol(b []byte, desc bool, f colAppender) []byte {
	start := len(b)
	b = f(b)
	if desc {
		for i := start; i < len(b); i++ {
			b[i] = ^b[i]
		}
	}
	return b
}

// AppendNull appends a NULL column. NULL sorts first ascending, last descending.
func AppendNull(b []byte, desc bool) []byte {
	return appendCol(b, desc, func(b []byte) []byte { return append(b, MarkerNull) })
}

// AppendInt appends a signed integer: 8 bytes big-endian with the sign bit flipped.
func AppendInt(b []byte, v int64, desc bool) []byte {
	return appendCol(b, desc, func(b []byte) []byte {
		b = append(b, MarkerValue)
		return binary.BigEndian.AppendUint64(b, uint64(v)^(1<<63))
	})
}

// AppendUint appends an unsigned integer (also BIT, SET bitmask, YEAR, ENUM ordinal).
func AppendUint(b []byte, v uint64, desc bool) []byte {
	return appendCol(b, desc, func(b []byte) []byte {
		b = append(b, MarkerValue)
		return binary.BigEndian.AppendUint64(b, v)
	})
}

// AppendFloat appends a float widened to double. Positive: flip the sign bit;
// negative: flip all bits; -0 becomes 0.
func AppendFloat(b []byte, v float64, desc bool) []byte {
	if v == 0 {
		v = 0 // normalises -0
	}
	u := math.Float64bits(v)
	if u&(1<<63) != 0 {
		u = ^u
	} else {
		u ^= 1 << 63
	}
	return appendCol(b, desc, func(b []byte) []byte {
		b = append(b, MarkerValue)
		return binary.BigEndian.AppendUint64(b, u)
	})
}

// AppendTime appends a temporal value as signed int64 microseconds, sign flipped.
// TIMESTAMP values must already be in UTC.
func AppendTime(b []byte, micros int64, desc bool) []byte {
	return AppendInt(b, micros, desc)
}

// AppendBytes appends raw bytes (binary strings, _bin collations) or collation
// sort weights, escaped and terminated so that every column is self-delimiting.
func AppendBytes(b []byte, v []byte, desc bool) []byte {
	return appendCol(b, desc, func(b []byte) []byte {
		b = append(b, MarkerValue)
		for _, c := range v {
			if c == escByte {
				b = append(b, escByte, escEscape)
			} else {
				b = append(b, c)
			}
		}
		return append(b, escByte, escTerm)
	})
}

// AppendDecimal appends a DECIMAL in the SQLite4 key style that CockroachDB
// also uses: a class byte (0x00 negative, 0x01 zero, 0x02 positive), then for
// non-zero values the exponent and the significant digits with trailing zeros
// removed (so 1.5 = 1.50), all inverted for negatives. The value is
// sign × 0.digits × 10^exp; digits are ASCII and must not start with '0'.
func AppendDecimal(b []byte, sign int, exp int32, digits string, desc bool) []byte {
	return appendCol(b, desc, func(b []byte) []byte {
		b = append(b, MarkerValue)
		if sign == 0 {
			return append(b, 0x01)
		}
		start := len(b) + 1
		if sign < 0 {
			b = append(b, 0x00)
		} else {
			b = append(b, 0x02)
		}
		b = binary.BigEndian.AppendUint32(b, uint32(exp)^(1<<31))
		for i := len(digits); i > 0; i-- {
			if digits[i-1] != '0' {
				digits = digits[:i]
				break
			}
		}
		b = append(b, digits...)
		b = append(b, 0x00)
		if sign < 0 {
			for i := start; i < len(b); i++ {
				b[i] = ^b[i]
			}
		}
		return b
	})
}

// ---- decoding (used for tests, debugging and binary columns) ---------------

func invert(b []byte, desc bool) []byte {
	if !desc {
		return b
	}
	out := make([]byte, len(b))
	for i, c := range b {
		out[i] = ^c
	}
	return out
}

// DecodeMarker reports whether the next column is NULL without consuming it.
func DecodeMarker(b []byte, desc bool) (null bool, err error) {
	if len(b) == 0 {
		return false, ErrCorrupt
	}
	m := b[0]
	if desc {
		m = ^m
	}
	return m == MarkerNull, nil
}

// SkipNull consumes a NULL column.
func SkipNull(b []byte) []byte { return b[1:] }

// DecodeInt decodes a column written by AppendInt.
func DecodeInt(b []byte, desc bool) (int64, []byte, error) {
	if len(b) < 9 {
		return 0, nil, ErrCorrupt
	}
	c := invert(b[:9], desc)
	if c[0] != MarkerValue {
		return 0, nil, ErrCorrupt
	}
	return int64(binary.BigEndian.Uint64(c[1:]) ^ (1 << 63)), b[9:], nil
}

// DecodeUint decodes a column written by AppendUint.
func DecodeUint(b []byte, desc bool) (uint64, []byte, error) {
	if len(b) < 9 {
		return 0, nil, ErrCorrupt
	}
	c := invert(b[:9], desc)
	if c[0] != MarkerValue {
		return 0, nil, ErrCorrupt
	}
	return binary.BigEndian.Uint64(c[1:]), b[9:], nil
}

// DecodeFloat decodes a column written by AppendFloat.
func DecodeFloat(b []byte, desc bool) (float64, []byte, error) {
	if len(b) < 9 {
		return 0, nil, ErrCorrupt
	}
	c := invert(b[:9], desc)
	if c[0] != MarkerValue {
		return 0, nil, ErrCorrupt
	}
	u := binary.BigEndian.Uint64(c[1:])
	if u&(1<<63) != 0 {
		u ^= 1 << 63
	} else {
		u = ^u
	}
	return math.Float64frombits(u), b[9:], nil
}

// DecodeBytes decodes a column written by AppendBytes.
func DecodeBytes(b []byte, desc bool) ([]byte, []byte, error) {
	if len(b) < 1 {
		return nil, nil, ErrCorrupt
	}
	x := func(c byte) byte {
		if desc {
			return ^c
		}
		return c
	}
	if x(b[0]) != MarkerValue {
		return nil, nil, ErrCorrupt
	}
	var out []byte
	for i := 1; i < len(b); i++ {
		c := x(b[i])
		if c != escByte {
			out = append(out, c)
			continue
		}
		if i+1 >= len(b) {
			return nil, nil, ErrCorrupt
		}
		switch x(b[i+1]) {
		case escEscape:
			out = append(out, escByte)
			i++
		case escTerm:
			return out, b[i+2:], nil
		default:
			return nil, nil, ErrCorrupt
		}
	}
	return nil, nil, ErrCorrupt
}

// ---- MVCC timestamp suffix --------------------------------------------------

// AppendTS appends the inverted commit_ts so that newer versions sort first.
func AppendTS(userKey []byte, ts uint64) []byte {
	return binary.BigEndian.AppendUint64(userKey, ^ts)
}

// MVCCKey returns a fresh key holding userKey and its commit_ts suffix.
func MVCCKey(userKey []byte, ts uint64) []byte {
	k := make([]byte, 0, len(userKey)+TSLen)
	k = append(k, userKey...)
	return AppendTS(k, ts)
}

// SplitMVCC splits a stored key into its user key and commit_ts.
func SplitMVCC(key []byte) (userKey []byte, ts uint64, err error) {
	if len(key) < TSLen {
		return nil, 0, ErrCorrupt
	}
	n := len(key) - TSLen
	return key[:n], ^binary.BigEndian.Uint64(key[n:]), nil
}

// Split is the Pebble Comparer.Split function: everything before the last
// 8 bytes is the user key, so bloom filters and prefix seeks ignore versions.
func Split(key []byte) int {
	if len(key) < TSLen {
		return len(key)
	}
	return len(key) - TSLen
}
