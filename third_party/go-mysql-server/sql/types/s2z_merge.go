// Copyright 2026 s2z. Licensed under the Apache License, Version 2.0.

package types

// s2z patch: the type of COALESCE, CASE, IF and IFNULL over temporal values.

import (
	"github.com/dolthub/vitess/go/sqltypes"

	"github.com/dolthub/go-mysql-server/sql"
)

func temporalDigits(t sql.Type) int {
	if tt, ok := t.(TimespanType_); ok {
		if p, fixed := tt.Precision(); fixed {
			return p
		}
		return 0
	}
	if dt, ok := t.(sql.DatetimeType); ok && !IsDateType(t) {
		return dt.Precision()
	}
	return 0
}

// S2zTemporalMerge is MySQL's type for values of temporal types a and b
// taken together: TIME with TIME is a TIME, TIMESTAMP with TIMESTAMP a
// TIMESTAMP, DATE with DATE a DATE, other pairs a DATETIME, with the most
// fractional digits. It is false unless both are temporal.
func S2zTemporalMerge(a, b sql.Type) (sql.Type, bool) {
	if !IsTimespan(a) && !IsTime(a) || !IsTimespan(b) && !IsTime(b) {
		return nil, false
	}
	p := max(temporalDigits(a), temporalDigits(b))
	switch {
	case IsTimespan(a) && IsTimespan(b):
		return CreateTimeType(p), true
	case IsDateType(a) && IsDateType(b):
		return Date, true
	case IsTimestampType(a) && IsTimestampType(b):
		return MustCreateDatetimeType(sqltypes.Timestamp, p), true
	}
	return MustCreateDatetimeType(sqltypes.Datetime, p), true
}
