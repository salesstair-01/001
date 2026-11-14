package sqlengine

import (
	"context"
	"testing"
)

// TestMySQLComparisons pins against MySQL 8.0.46 comparisons of numbers
// with strings, hexadecimal literals and dates, DECIMAL comparisons,
// subqueries over grouped rows, RAND(seed), PI(), division by zero, and
// statistics of a table created again.
func TestMySQLComparisons(t *testing.T) {
	c := startCluster(t)
	conn, err := c.db(t).Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, s := range []struct{ q, want string }{
		// a number compared with a string that is not one: its prefix,
		// one warning
		{"CREATE TABLE t (i INT, d DECIMAL(5,2), f DOUBLE, s VARCHAR(10))", "[]"},
		{"INSERT INTO t VALUES (-1,-1,-1,'1'), (0,0,0,'2'), (1,1.5,1.5,'x')", "[]"},
		{"SELECT i FROM t WHERE i > 'A'", "[1]"},
		{"SHOW WARNINGS", "[Warning,1292,Truncated incorrect DOUBLE value: 'A']"},
		{"SELECT i FROM t WHERE d >= '1abc'", "[1]"},
		{"SHOW WARNINGS", "[Warning,1292,Truncated incorrect DECIMAL value: '1abc']"},
		{"SELECT i FROM t WHERE f < '1.5e0xyz'", "[-1 0]"},
		{"SELECT i FROM t WHERE '1x' = i", "[1]"},
		{"SELECT i FROM t WHERE i IN ('1x', 'y')", "[0 1]"},
		{"SELECT i FROM t WHERE i = ''", "[0]"},
		{"SHOW WARNINGS", "[]"},
		{"SELECT i, i BETWEEN 0 AND 2, i BETWEEN NULL AND 2 FROM t", "[-1,0,NULL 0,1,NULL 1,1,NULL]"},
		// hexadecimal literals are numbers against numbers
		{"CREATE TABLE h (i INT UNSIGNED, d DECIMAL(65,30))", "[]"},
		{"INSERT INTO h VALUES (1, -184467440737095516160000), (255, 0), (4294967295, 184467440737095516160000)", "[]"},
		{"SELECT i FROM h WHERE i = 0xff", "[255]"},
		{"SELECT i FROM h WHERE i < x'ffffffff'", "[1 255]"},
		{"SELECT i FROM h WHERE i = b'11111111'", "[255]"},
		{"SELECT i FROM h WHERE d > 0xFFFFFFFFFFFFFFFF", "[4294967295]"},
		{"SELECT i FROM h WHERE d <= 0x1FFFFFFFFFFFFFFFF", "[1 255]"},
		{"SHOW WARNINGS", "[Warning,1292,Truncated incorrect BINARY value: 'x'01ffffffffffffffff'']"},
		// a DECIMAL compared exactly, with a float as doubles
		{"CREATE TABLE dd (d DECIMAL(3,1))", "[]"},
		{"INSERT INTO dd VALUES (-10.5), (-10.3), (0.0), (10.3), (10.5)", "[]"},
		{"SELECT d FROM dd WHERE d = 10.31E00", "[]"},
		{"SELECT d FROM dd WHERE d >= 10.31E00", "[10.5]"},
		{"SELECT d FROM dd WHERE d > '-10.31'", "[-10.3 0.0 10.3 10.5]"},
		{"SELECT d FROM dd WHERE d > 0x100", "[]"},
		{"SELECT d FROM dd WHERE d > 0x01", "[10.3 10.5]"},
		// a DATETIME with a number that is no date: as numbers
		{"CREATE TABLE dt (t DATETIME)", "[]"},
		{"INSERT INTO dt VALUES ('1970-01-01 03:00:01'), ('2018-06-19 21:00:00'), ('2038-01-19 06:14:07')", "[]"},
		{"SELECT COUNT(*) FROM dt WHERE t > 2018", "[3]"},
		{"SHOW WARNINGS", "[Warning,1292,Incorrect datetime value: '2018' for column 't' at row 1]"},
		{"SELECT COUNT(*) FROM dt WHERE t >= 0", "[3]"},
		{"SELECT COUNT(*) FROM dt WHERE t >= 20380119061405.55E00", "[1]"},
		{"SELECT COUNT(*) FROM dt WHERE t > '2018-00-03 00:00:00'", "ERR 1525"},
		{"SET sql_mode = 'ALLOW_INVALID_DATES'", "[]"},
		{"SELECT COUNT(*) FROM dt WHERE t > '2018-00-03 00:00:00'", "[2]"},
		{"SELECT COUNT(*) FROM dt WHERE t <= '0000-00-00 10:20:30'", "[0]"},
		{"SET sql_mode = DEFAULT", "[]"},
		// statistics of a dropped table do not survive it
		{"CREATE TABLE s1 (a int, b int, KEY a (b))", "[]"},
		{"INSERT INTO s1 VALUES (1,1),(2,2),(3,3)", "[]"},
		{"ANALYZE TABLE s1", "[app.s1,analyze,status,OK]"},
		{"DROP TABLE s1", "[]"},
		{"CREATE TABLE s1 (a int, b int)", "[]"},
		{"INSERT INTO s1 VALUES (1,1),(2,2),(3,3)", "[]"},
		{"ALTER TABLE s1 ADD INDEX (a)", "[]"},
		{"SELECT a FROM s1 WHERE a > ANY (SELECT a FROM s1 WHERE b = 2)", "[3]"},
		// subqueries over grouped rows
		{"CREATE TABLE g (a int, b int)", "[]"},
		{"INSERT INTO g VALUES (1,1),(1,2),(2,3)", "[]"},
		{"SELECT (SELECT g.a) FROM g GROUP BY g.a", "[1 2]"},
		{"SELECT (SELECT COUNT(*) FROM g x WHERE x.a = g.a) FROM g GROUP BY a", "[2 1]"},
		{"SELECT (SELECT SUM(g.b)) FROM g GROUP BY g.a", "[3 3]"},
		{"SELECT (SELECT COUNT(DISTINCT g.b)) FROM g GROUP BY g.a", "[2 1]"},
		{"SELECT (SELECT MAX(g.b)+1), (SELECT GROUP_CONCAT(g.b)) FROM g GROUP BY g.a", "[3,1,2 4,3]"},
		{"SELECT (SELECT COUNT(*) FROM g x WHERE x.b > (SELECT MAX(g.b))) FROM g GROUP BY a", "[1 0]"},
		{"SELECT COUNT(*) AS c FROM g GROUP BY (SELECT c)", "ERR 1247"},
		{"SELECT COUNT(*) AS c FROM g GROUP BY (SELECT 1 HAVING c)", "ERR 1247"},
		{"SELECT (SELECT y) AS x, a AS y FROM g", "ERR 1247"},
		{"SELECT a AS x FROM g WHERE x > 1", "ERR 1054"},
		// CHECK constraints: a column they use, ALGORITHM=INPLACE
		{"CREATE TABLE ck (f1 int, f2 int, CHECK (f1 + f2 < 10))", "[]"},
		{"ALTER TABLE ck DROP COLUMN f1", "ERR 3959"},
		{"ALTER TABLE ck RENAME COLUMN f1 TO f3", "ERR 3959"},
		{"ALTER TABLE ck ADD f3 INT CHECK (f3 < 10), ALGORITHM=INPLACE", "ERR 1845"},
		{"ALTER TABLE ck ADD f4 INT CHECK (f4 < 10) NOT ENFORCED, ALGORITHM=INPLACE", "[]"},
		// RAND(seed) is MySQL's generator
		{"SELECT RAND(999999), RAND(-1), RAND(1.5), RAND('7'), RAND(NULL)", "[0.014231365187309091,0.9050373219931845,0.6555866465490187,0.9065021936842261,0.15522042769493574]"},
		{"SELECT CAST(RAND(2) * 1000 AS UNSIGNED), CAST(RAND(a) * 1000 AS UNSIGNED) FROM g", "[656,405 122,405 645,656]"},
		{"SELECT PI(), PI()*2, PI()/3, ROUND(PI(),2), CONCAT(PI()), -PI()", "[3.141593,6.283185,1.0471975512,3.14,3.141593,-3.141593]"},
		// division by zero, and DECIMAL out of range, in strict mode
		{"CREATE TABLE z (a decimal(4,2), b int)", "[]"},
		{"INSERT INTO z VALUES (101.55, 1)", "ERR 1264"},
		{"INSERT INTO z VALUES (11, 2)", "[]"},
		{"UPDATE z SET a = a * 50", "ERR 1264"},
		{"UPDATE z SET a = a / 0", "ERR 1365"},
		{"UPDATE z SET b = MOD(b, 0)", "ERR 1365"},
		{"INSERT INTO z SELECT 1, 4/0", "ERR 1365"},
		{"SELECT a/0 FROM z", "[NULL]"},
		{"SHOW WARNINGS", "[Warning,1365,Division by 0]"},
		{"SET sql_mode = 'STRICT_TRANS_TABLES'", "[]"},
		{"INSERT INTO z VALUES (1/0, 5)", "[]"},
		{"SHOW WARNINGS", "[]"},
		{"SET sql_mode = DEFAULT", "[]"},
	} {
		if got := connRun(t, conn, s.q); got != s.want {
			t.Errorf("%s: got %s, want %s", s.q, got, s.want)
		}
	}
}
