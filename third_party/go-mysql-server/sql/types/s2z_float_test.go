package types

import "testing"

// TestMySQLFloatString pins MySQL 8.0.46's float output.
func TestMySQLFloatString(t *testing.T) {
	tenth := 0.1
	for _, c := range []struct {
		v      float64
		single bool
		want   string
	}{
		{float64(float32(3.3)), true, "3.3"}, {1e20, true, "1e20"}, {float64(float32(123456789)), true, "123457000"},
		{float64(float32(0.0001)), true, "0.0001"}, {float64(float32(1e-10)), true, "0.0000000001"},
		{float64(float32(-3.4028234663852886e+38)), true, "-3.40282e38"}, {float64(float32(1.0 / 3)), true, "0.333333"},
		{float64(float32(999999.5)), true, "1000000"}, {float64(float32(1e-14)), true, "0.00000000000001"},
		{float64(float32(1e-16)), true, "1e-16"}, {float64(float32(-1.23456e-10)), true, "-0.000000000123456"},
		{float64(float32(1.23456e15)), true, "1.23456e15"}, {float64(float32(1e15)), true, "1e15"},
		{3.3, false, "3.3"}, {1e15, false, "1e15"}, {1e14, false, "100000000000000"},
		{123456789012345678, false, "1.2345678901234568e17"}, {1234567890123456.7, false, "1234567890123456.8"},
		{-1.7976931348623157e308, false, "-1.7976931348623157e308"}, {1.0 / 3, false, "0.3333333333333333"},
		{float64(float32(0.0001)), false, "0.00009999999747378752"}, {1.234567890123456e-15, false, "0.000000000000001234567890123456"},
		{1e-28, false, "1e-28"}, {1.25e15, false, "1.25e15"}, {999999999999999.9, false, "999999999999999.9"},
		{tenth + 2*tenth, false, "0.30000000000000004"}, {100, false, "100"}, {1e301, false, "1e301"},
		{float64(float32(1e-10)), false, "0.0000000001000000013351432"}, {float64(float32(1e-17)), false, "9.99999983775159e-18"},
	} {
		if got := MySQLFloatString(c.v, c.single); got != c.want {
			t.Errorf("%v (single %v): got %s, want %s", c.v, c.single, got, c.want)
		}
	}
}
