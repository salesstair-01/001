package sqlengine

import (
	"context"
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

// TestYearConstants pins MySQL 8.0.46's comparisons of a YEAR with
// constants (1 is 2001, '0' is 2000, 100 compares as a number, IN with
// several integers compares as numbers) and YEAR and float values as text.
func TestYearConstants(t *testing.T) {
	c := startCluster(t)
	conn, err := c.db(t).Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, s := range []struct{ q, want string }{
		{"CREATE TABLE ty (y year)", "[]"},
		{"INSERT INTO ty VALUES (0),(1901),(2018),(2155),(2001),(1970),(2069)", "[]"},
		{"SELECT GROUP_CONCAT(y ORDER BY y) FROM ty WHERE y < 1", "[0000,1901,1970]"},
		{"SELECT GROUP_CONCAT(y ORDER BY y) FROM ty WHERE y = 1", "[2001]"},
		{"SELECT GROUP_CONCAT(y ORDER BY y) FROM ty WHERE y = 0", "[0000]"},
		{"SELECT GROUP_CONCAT(y ORDER BY y) FROM ty WHERE y = 69", "[2069]"},
		{"SELECT GROUP_CONCAT(y ORDER BY y) FROM ty WHERE y = 70", "[1970]"},
		{"SELECT GROUP_CONCAT(y ORDER BY y) FROM ty WHERE y = 99", "[NULL]"},
		{"SELECT GROUP_CONCAT(y ORDER BY y) FROM ty WHERE y < 100", "[0000]"},
		{"SELECT GROUP_CONCAT(y ORDER BY y) FROM ty WHERE y < 1.5", "[0000,1901,1970,2001]"},
		{"SELECT GROUP_CONCAT(y ORDER BY y) FROM ty WHERE y = 1.0", "[2001]"},
		{"SELECT GROUP_CONCAT(y ORDER BY y) FROM ty WHERE y < '1'", "[0000,1901,1970]"},
		{"SELECT GROUP_CONCAT(y ORDER BY y) FROM ty WHERE y = '0'", "[NULL]"},
		{"SELECT GROUP_CONCAT(y ORDER BY y) FROM ty WHERE y = '0000'", "[0000]"},
		{"SELECT GROUP_CONCAT(y ORDER BY y) FROM ty WHERE y > -1", "[0000,1901,1970,2001,2018,2069,2155]"},
		{"SELECT GROUP_CONCAT(y ORDER BY y) FROM ty WHERE y < 2156", "[0000,1901,1970,2001,2018,2069,2155]"},
		{"SELECT GROUP_CONCAT(y ORDER BY y) FROM ty WHERE y = 1901.4", "[1901]"},
		{"SELECT GROUP_CONCAT(y ORDER BY y) FROM ty WHERE y < 1e0", "[0000,1901,1970]"},
		{"SELECT GROUP_CONCAT(y ORDER BY y) FROM ty WHERE y BETWEEN 1 AND 70", "[NULL]"},
		{"SELECT GROUP_CONCAT(y ORDER BY y) FROM ty WHERE y IN (1, 70)", "[NULL]"},
		{"SELECT GROUP_CONCAT(y ORDER BY y) FROM ty WHERE y IN ('1', '70')", "[1970,2001]"},
		{"SELECT GROUP_CONCAT(y ORDER BY y) FROM ty WHERE y IN (2001, 1970, 0)", "[0000,1970,2001]"},
		{"SELECT GROUP_CONCAT(y ORDER BY y) FROM ty WHERE y NOT IN (1, 2018)", "[0000,1901,1970,2001,2069,2155]"},
		{"SELECT GROUP_CONCAT(y ORDER BY y) FROM ty WHERE y IN (1)", "[2001]"},
		{"SELECT GROUP_CONCAT(y ORDER BY y) FROM ty WHERE y < 0+1", "[0000,1901,1970]"},
		{"SELECT CONCAT(y), y+0, CAST(y AS CHAR) FROM ty WHERE y = 0", "[0000,0,0000]"},

		{"CREATE TABLE tf (y year, f float, d double, m float(5,2))", "[]"},
		{"INSERT INTO tf VALUES (0, 3.3, 3.3, 3.3), (1999, 1e20, 1e-20, -999.99), (2001, 123456789, 0.1, 0)", "[]"},
		{"SELECT CONCAT(y), CAST(y AS CHAR), CONCAT(f), CAST(f AS CHAR), CONCAT(d), CONCAT(m), CAST(m AS CHAR) FROM tf ORDER BY y",
			"[0000,0000,3.3,3.3,3.3,3.30,3.30 1999,1999,1e20,1e20,1e-20,-999.99,-999.99 2001,2001,123457000,123457000,0.1,0.00,0.00]"},
		{"SELECT GROUP_CONCAT(y ORDER BY y), GROUP_CONCAT(f ORDER BY f), GROUP_CONCAT(m ORDER BY m), GROUP_CONCAT(d ORDER BY d) FROM tf",
			"[0000,1999,2001,3.3,123457000,1e20,-999.99,0.00,3.30,1e-20,0.1,3.3]"},
	} {
		if got := connRun(t, conn, s.q); got != s.want {
			t.Errorf("%s: got %s, want %s", s.q, got, s.want)
		}
	}
}

// TestDecimalConstants pins a DECIMAL(3,1) compared with constants that
// have more decimals or are out of range, against MySQL 8.0.46.
func TestDecimalConstants(t *testing.T) {
	c := startCluster(t)
	conn, err := c.db(t).Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, s := range []struct{ q, want string }{
		{"CREATE TABLE t (d decimal(3,1))", "[]"},
		{"INSERT INTO t VALUES (-10.5),(-10.4),(-10.3),(10.3),(10.4),(10.5),(NULL)", "[]"},
		{"SELECT GROUP_CONCAT(d ORDER BY d) FROM t WHERE d >= 10.31", "[10.4,10.5]"},
		{"SELECT GROUP_CONCAT(d ORDER BY d) FROM t WHERE d < 10.31", "[-10.5,-10.4,-10.3,10.3]"},
		{"SELECT GROUP_CONCAT(d ORDER BY d) FROM t WHERE d = 10.31", "[NULL]"},
		{"SELECT GROUP_CONCAT(d ORDER BY d) FROM t WHERE d <> 10.31", "[-10.5,-10.4,-10.3,10.3,10.4,10.5]"},
		{"SELECT GROUP_CONCAT(d ORDER BY d) FROM t WHERE d > -10.31", "[-10.3,10.3,10.4,10.5]"},
		{"SELECT GROUP_CONCAT(d ORDER BY d) FROM t WHERE d <= -10.31", "[-10.5,-10.4]"},
		{"SELECT GROUP_CONCAT(d ORDER BY d) FROM t WHERE d < 100", "[-10.5,-10.4,-10.3,10.3,10.4,10.5]"},
		{"SELECT GROUP_CONCAT(d ORDER BY d) FROM t WHERE d > 99.95", "[NULL]"},
		{"SELECT GROUP_CONCAT(d ORDER BY d) FROM t WHERE d <=> 10.31", "[NULL]"},
		{"SELECT GROUP_CONCAT(d ORDER BY d) FROM t WHERE d = 10.3e0", "[10.3]"},
		{"SELECT GROUP_CONCAT(d ORDER BY d) FROM t WHERE 10.31 < d", "[10.4,10.5]"},
		{"CREATE TABLE tu (u tinyint unsigned)", "[]"},
		{"INSERT INTO tu VALUES (0),(3),(255)", "[]"},
		{"SELECT u, u = 256, ABS(u=256), ABS(u=256)=0, (u=256)+0, ABS(u=3) FROM tu ORDER BY u", "[0,0,0,1,0,0 3,0,0,1,0,1 255,0,0,1,0,0]"},
		{"SELECT * FROM tu WHERE ABS(u=256)=0 ORDER BY u", "[0 3 255]"},
	} {
		if got := connRun(t, conn, s.q); got != s.want {
			t.Errorf("%s: got %s, want %s", s.q, got, s.want)
		}
	}
}
