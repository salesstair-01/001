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

// TestTimeAsNumber pins a TIME used as a number against MySQL 8.0.46: stored
// in INT (rounded to a second, hhmmss), DECIMAL and DOUBLE (hhmmss.ffffff),
// in arithmetic (a DECIMAL with the TIME's digits) and ROUND (a DOUBLE,
// half to even); and ROUND(x, n) of a DECIMAL keeping n decimals.
func TestTimeAsNumber(t *testing.T) {
	c := startCluster(t)
	conn, err := c.db(t).Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, s := range []struct{ q, want string }{
		{"CREATE TABLE tn (t time(6), i int, d decimal(20,6))", "[]"},
		{"INSERT INTO tn (t) VALUES ('10:10:59.5'),('10:10:10.5'),('10:10:10.499999'),('-10:10:59.5'),('-10:10:10.5'),('838:59:59'),('0:0:0.000001')", "[]"},
		{"UPDATE tn SET i=t, d=t", "[]"},
		{"SELECT i, CONCAT(d) FROM tn", "[101100,101059.500000 101011,101010.500000 101010,101010.499999 -101100,-101059.500000 -101011,-101010.500000 8385959,8385959.000000 0,0.000001]"},
		{"SELECT ROUND(t), ROUND(t,2), ROUND(t,-2), CONCAT(t+0), CONCAT(t*1), CAST(t AS SIGNED) FROM tn LIMIT 2", "[101060,101059.5,101100,101059.500000,101059.500000,101100 101010,101010.5,101000,101010.500000,101010.500000,101011]"},
		{"CREATE TABLE rr AS SELECT ROUND(t) a, ROUND(t,2) b, t+0 c, t*1 d FROM tn", "[]"},
		{"SHOW CREATE TABLE rr", "[rr,CREATE TABLE `rr` (\n  `a` double DEFAULT NULL,\n  `b` double DEFAULT NULL,\n  `c` decimal(14,6) DEFAULT NULL,\n  `d` decimal(14,6) DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci]"},
		{"SELECT CONCAT(1.50), CONCAT(CAST(1 AS DECIMAL(5,2))), CONCAT(1/3), CONCAT(1.5*2.25), CONCAT(10.30 + 1), CAST(2.500 AS CHAR), CONCAT(7/2)", "[1.50,1.00,0.3333,3.375,11.30,2.500,3.5000]"},
		{"SELECT CONCAT(ROUND(2.567, 1)), CONCAT(ROUND(2.5, 3)), CONCAT(ROUND(9.96, 1)), CONCAT(ROUND(2.567)), CONCAT(ROUND(-2.567, 2))", "[2.6,2.5,10.0,3,-2.57]"},
	} {
		if got := connRun(t, conn, s.q); got != s.want {
			t.Errorf("%s: got %s, want %s", s.q, got, s.want)
		}
	}
}

// TestNumberAsTime pins numbers stored in and cast to TIME against MySQL
// 8.0.46: hhmmss.ffffff rounded to the type's digits, the bound past
// 838:59:59, 00:00:00 for minutes over 59 (warnings outside strict mode,
// 1292 in it), and NULL from CAST.
func TestNumberAsTime(t *testing.T) {
	c := startCluster(t)
	conn, err := c.db(t).Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, s := range []struct{ q, want string }{
		{"CREATE TABLE t3 (a time)", "[]"},
		{"INSERT INTO t3 VALUES (106060)", "ERR 1292"},
		{"INSERT INTO t3 VALUES (8385960)", "ERR 1292"},
		{"INSERT INTO t3 VALUES (106060.5)", "ERR 1292"},
		{"INSERT INTO t3 VALUES (-8385960e0)", "ERR 1292"},
		{"INSERT INTO t3 VALUES (8385959.4)", "ERR 1292"},
		{"INSERT INTO t3 VALUES ('8385960')", "ERR 1292"},
		{"SET sql_mode = ''", "[]"},
		{"CREATE TABLE tt (a time(6), b time)", "[]"},
		{"INSERT INTO tt VALUES (105959.123457, 105959.5),(0.111111,0.5),(0.1111115,1.5),(-105959.12,-0.5),(8385960, 9999999),(-8385960,-1e10),(1e20, 106060),(59.9999995, 595959.9999999),(120,'120'),(1234,1234.5)", "[]"},
		{"SHOW WARNINGS", "[Warning,1264,Out of range value for column 'a' at row 5 Warning,1264,Out of range value for column 'b' at row 5 Warning,1264,Out of range value for column 'a' at row 6 Warning,1264,Out of range value for column 'b' at row 6 Warning,1264,Out of range value for column 'a' at row 7 Warning,1264,Out of range value for column 'b' at row 7]"},
		{"SELECT * FROM tt", "[10:59:59.123457,11:00:00 00:00:00.111111,00:00:01 00:00:00.111112,00:00:02 -10:59:59.120000,-00:00:01 838:59:59.000000,838:59:59 -838:59:59.000000,-838:59:59 838:59:59.000000,00:00:00 00:01:00.000000,60:00:00 00:01:20.000000,00:01:20 00:12:34.000000,00:12:35]"},
		{"SELECT CAST(105959.123457 AS TIME(6)), CAST(0.1111115 AS TIME(6)), CAST(1234.5e0 AS TIME(1)), CAST(-0.5 AS TIME), CAST(8385960 AS TIME)", "[10:59:59.123457,00:00:00.111112,00:12:34.5,-00:00:01,NULL]"},
	} {
		if got := connRun(t, conn, s.q); got != s.want {
			t.Errorf("%s: got %s, want %s", s.q, got, s.want)
		}
	}
}
