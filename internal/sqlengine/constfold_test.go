package sqlengine

import (
	"fmt"
	"testing"
)

// An integer column compared with a constant out of its range, with a
// fraction, or in a string, gives MySQL 8.0.46's results, with and without
// an index; 0.0 goes into unsigned columns.
func TestIntegerConstantFolding(t *testing.T) {
	c := startCluster(t)
	db := c.db(t)
	for _, q := range []string{
		"CREATE TABLE cfu (colA bigint unsigned)",
		"CREATE TABLE cfui (colA bigint unsigned, key k(colA))",
		"CREATE TABLE cfs (colA bigint, key k(colA))",
		"CREATE TABLE cft (a tinyint unsigned, key(a))",
		"INSERT INTO cfu VALUES ('0.0'),(1),(0),(1),(3),(18446744073709551614),('18446744073709551615.0'),(NULL)",
		"INSERT INTO cfui SELECT * FROM cfu",
		"INSERT INTO cfs VALUES (-9223372036854775808),(-9223372036854775807),(0),(1),(3),(9223372036854775806),(9223372036854775807),(NULL)",
		"INSERT INTO cft VALUES (0),(1),(255),(NULL),('0.0'),(0.0E0)",
	} {
		mustExec(t, db, q)
	}
	for _, tb := range []string{"cfu", "cfui"} {
		for _, tc := range []struct{ where, want string }{
			{"colA < '18446744073709551616'", "[0 0 1 1 3 18446744073709551614 18446744073709551615]"},
			{"colA >= '18446744073709551615'", "[18446744073709551615]"},
			{"colA = '18446744073709551616'", "[]"},
			{"colA <> '18446744073709551616'", "[0 0 1 1 3 18446744073709551614 18446744073709551615]"},
			{"colA = '1.0'", "[1 1]"},
			{"colA > '1.5'", "[3 18446744073709551614 18446744073709551615]"},
			{"'1.5' < colA", "[3 18446744073709551614 18446744073709551615]"},
			{"colA = ' 3 '", "[3]"},
			{"colA < '-1'", "[]"},
			{"colA <= 2.9", "[0 0 1 1]"},
		} {
			q := "SELECT colA FROM " + tb + " WHERE " + tc.where + " ORDER BY colA"
			if got := fmt.Sprint(queryStrings(t, db, q)); got != tc.want {
				t.Errorf("%s: %s, want %s", q, got, tc.want)
			}
		}
	}
	for _, tc := range []struct{ q, want string }{
		{"SELECT colA FROM cfs WHERE colA < '-9223372036854775809'", "[]"},
		{"SELECT COUNT(*) FROM cfs WHERE colA > '-9223372036854775809'", "[7]"},
		{"SELECT colA FROM cfs WHERE colA >= '9223372036854775807'", "[9223372036854775807]"},
		{"SELECT colA FROM cfs WHERE colA = '9223372036854775808'", "[]"},
		{"SELECT a, a < 300, a > 300, a = 1.5, a <> 1.5, a <=> 300, a > 1.5, a < -1 FROM cft ORDER BY a",
			"[NULL,NULL,NULL,NULL,NULL,0,NULL,NULL 0,1,0,0,1,0,0,0 0,1,0,0,1,0,0,0 0,1,0,0,1,0,0,0 1,1,0,0,1,0,0,0 255,1,0,0,1,0,1,0]"},
		{"SELECT COUNT(*) FROM cft WHERE a >= '254.5'", "[1]"},
		{"SELECT COUNT(*) FROM cft WHERE a < '0.5'", "[3]"},
	} {
		if got := fmt.Sprint(queryStrings(t, db, tc.q)); got != tc.want {
			t.Errorf("%s: %s, want %s", tc.q, got, tc.want)
		}
	}
}
