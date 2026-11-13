package sqlengine

import (
	"context"
	"testing"
)

// TestFloatMDAndUnsigned pins FLOAT(M,D), DOUBLE(M,D), UNSIGNED floats and
// floats stored in unsigned integer columns against MySQL 8.0.46. (The Go
// driver reads numbers back as floats, so 3.30 reads as 3.3 here; mtr checks
// the text.)
func TestFloatMDAndUnsigned(t *testing.T) {
	c := startCluster(t)
	conn, err := c.db(t).Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, s := range []struct{ q, want string }{
		{"CREATE TABLE f (a float(5,2), b double(7,3), c float(7,4) unsigned)", "[]"},
		{"INSERT INTO f VALUES (-999.985, 1.23456, 1.5), (3.3, 0, 0), (999.984, -9999.9995, 2.71828)", "[]"},
		{"INSERT INTO f (a) VALUES (999.995)", "ERR 1264"},
		{"INSERT INTO f (a) VALUES (1000)", "ERR 1264"},
		{"INSERT INTO f (c) VALUES (-1)", "ERR 1264"},
		{"SELECT * FROM f", "[-999.99,1.235,1.5 3.3,0,0 999.98,-9999.999,2.7183]"},
		{"SELECT * FROM f WHERE a <> -999.99", "[3.3,0,0 999.98,-9999.999,2.7183]"},
		{"SELECT CONCAT(a), CONCAT(a+0), CONCAT(a*2), a = 3.3, a = -999.99, CAST(a AS CHAR) FROM f",
			"[-999.99,-999.99,-1999.98,0,1,-999.99 3.30,3.30,6.60,1,0,3.30 999.98,999.98,1999.96,0,0,999.98]"},
		{"SELECT COUNT(*) FROM f WHERE a = '-999.99'", "[1]"},
		{"SELECT COUNT(*) FROM f WHERE a < '3.3'", "[1]"},
		{"CREATE TABLE f2 AS SELECT a, a+1 AS x FROM f", "[]"},
		{"SHOW CREATE TABLE f2", "[f2,CREATE TABLE `f2` (\n  `a` float(5,2) DEFAULT NULL,\n  `x` double(22,2) DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci]"},
		{"SELECT CONCAT(a), CONCAT(x) FROM f2", "[-999.99,-998.99 3.30,4.30 999.98,1000.98]"},
		{"CREATE TABLE w (w float(20,10))", "[]"},
		{"INSERT INTO w VALUES (1.1), (12345.67)", "[]"},
		{"SELECT CONCAT(w), CONCAT(w+0) FROM w", "[1.1000000238,1.1000000238 12345.6699218750,12345.6699218750]"},
		{"SET sql_mode = ''", "[]"},
		{"CREATE TABLE f30 (f1 float(30, 0))", "[]"},
		{"INSERT INTO f30 VALUES (1e29), (-1e29), (1e30), (-1e30), (2e30), (-2e30)", "[]"},
		{"SELECT CONCAT(f1 + 0e0) FROM f30", "[1.0000000150474662e29 -1.0000000150474662e29 1.0000000150474662e30 -1.0000000150474662e30 1.0000000150474662e30 -1.0000000150474662e30]"},
		{"INSERT INTO f (a, c) VALUES (5000, -1)", "[]"},
		{"SHOW WARNINGS", "[Warning,1264,Out of range value for column 'a' at row 1 Warning,1264,Out of range value for column 'c' at row 1]"},
		{"SELECT a, c FROM f WHERE b IS NULL", "[999.99,0]"},
		{"SET sql_mode = DEFAULT", "[]"},
		{"SHOW CREATE TABLE f", "[f,CREATE TABLE `f` (\n  `a` float(5,2) DEFAULT NULL,\n  `b` double(7,3) DEFAULT NULL,\n  `c` float(7,4) unsigned DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci]"},

		{"CREATE TABLE u (a tinyint unsigned, b smallint unsigned, c int unsigned, d bigint unsigned)", "[]"},
		{"INSERT INTO u VALUES (0.0E0, 0.0E0, 0.0E0, 0.0E0), (2.55E02, 6.5535E4, 4.294967295E9, 1.0E18), (2.544E02, 1.4E0, 3.0E9, 0.4E0)", "[]"},
		{"SELECT * FROM u", "[0,0,0,0 255,65535,4294967295,1000000000000000000 254,1,3000000000,0]"},
		{"INSERT INTO u (a) VALUES (2.56E02)", "ERR 1264"},
		{"INSERT INTO u (a) VALUES (-1.0E0)", "ERR 1264"},
		{"INSERT INTO u (a) VALUES (-0.4E0)", "[]"},
		{"SET sql_mode = ''", "[]"},
		{"INSERT INTO u (a, c) VALUES (2.56E02, -5.0E0)", "[]"},
		{"SHOW WARNINGS", "[Warning,1264,Out of range value for column 'a' at row 1 Warning,1264,Out of range value for column 'c' at row 1]"},
		{"SELECT a, c FROM u WHERE b IS NULL", "[0,NULL 255,0]"},
	} {
		if got := connRun(t, conn, s.q); got != s.want {
			t.Errorf("%s: got %s, want %s", s.q, got, s.want)
		}
	}
}
