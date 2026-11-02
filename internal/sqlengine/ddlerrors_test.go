package sqlengine

import (
	"context"
	"testing"
)

// TestDDLErrors: table definitions MySQL 8 refuses, with its error codes;
// the definitions it accepts next to them succeed.
func TestDDLErrors(t *testing.T) {
	c := startCluster(t)
	conn, err := c.db(t).Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, s := range []struct{ q, want string }{
		{"CREATE TABLE `` (a int)", "ERR 1103"},
		{"CREATE TABLE `t1 ` (a int)", "ERR 1103"},
		{"CREATE TABLE t1 (`` int)", "ERR 1166"},
		{"CREATE TABLE t1 (`a ` int)", "ERR 1166"},
		{"CREATE TABLE t1 (a int(256))", "ERR 1439"},
		{"CREATE TABLE t1 (a int(99999999999999999999999))", "ERR 1439"},
		{"CREATE TABLE t1 (a blob(4294967296))", "ERR 1439"},
		{"CREATE TABLE t1 (a float(0,2))", "ERR 1427"},
		{"CREATE TABLE t1 (a text DEFAULT '')", "ERR 1101"},
		{"CREATE TABLE t1 (a blob DEFAULT 'x')", "ERR 1101"},
		{"CREATE TABLE t1 (a date DEFAULT 0)", "ERR 1067"},
		{"CREATE TABLE t1 (a timestamp DEFAULT '0000-00-00 00:00:00')", "ERR 1067"},
		{"CREATE TABLE t1 (a int CHECK (a))", "ERR 3812"},
		{"CREATE TABLE t1 (a int CHECK (a > 1), b int, CHECK (b + 1))", "ERR 3812"},
		{"CREATE TABLE t1 (a int CHECK (if(a > 1, 1, 0)))", "ERR 3812"},
		{"CREATE TABLE t1 (a int PRIMARY KEY AUTO_INCREMENT, CHECK (a > 1))", "ERR 3818"},
		{"CREATE TABLE t5 (a int(255), b text DEFAULT (''), c blob DEFAULT NULL, d float(2,2), e date DEFAULT '2000-01-01')", ""},
		{"CREATE TABLE t2 (a int, b int, CHECK (a > 1), CHECK (a IS NULL OR b IN (1, 2)), CHECK (NOT a), CHECK (TRUE), CHECK (a BETWEEN 1 AND 2))", ""},
		{"SET SQL_MODE=''", ""},
		{"CREATE TABLE t3 (a date DEFAULT 0)", ""},
		{"CREATE TABLE t4 (a text DEFAULT '')", ""},
	} {
		if got := connRun(t, conn, s.q); got != s.want && !(got == "[]" && s.want == "") {
			t.Errorf("%s: got %q, want %q", s.q, got, s.want)
		}
	}
}

// TestSpatialColumns: geometry columns store and return their values.
func TestSpatialColumns(t *testing.T) {
	c := startCluster(t)
	conn, err := c.db(t).Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, s := range []struct{ q, want string }{
		{"CREATE TABLE g (id int PRIMARY KEY, p point, g geometry, l linestring)", "[]"},
		{"INSERT INTO g VALUES (1, ST_GeomFromText('POINT(1 2)'), ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 0))'), ST_GeomFromText('LINESTRING(0 0,1 1)')), (2, NULL, POINT(3,4), NULL)", "[]"},
		{"SELECT id, ST_AsText(p), ST_AsText(g), ST_AsText(l) FROM g", "[1,POINT(1 2),POLYGON((0 0,1 0,1 1,0 0)),LINESTRING(0 0,1 1) 2,NULL,POINT(3 4),NULL]"},
		{"UPDATE g SET p = POINT(5,6) WHERE id = 2", "[]"},
		{"SELECT ST_X(p), ST_Y(p) FROM g WHERE id = 2", "[5,6]"},
		{"INSERT INTO g (id, p) VALUES (3, 'garbage')", "ERR 3037"},
	} {
		if got := connRun(t, conn, s.q); got != s.want {
			t.Errorf("%s: got %q, want %q", s.q, got, s.want)
		}
	}
}

// TestCreateSelectColumns: CREATE TABLE ... SELECT names its columns by
// the SELECT's text and makes one NOT NULL only when MySQL 8.0.46 does.
func TestCreateSelectColumns(t *testing.T) {
	c := startCluster(t)
	conn, err := c.db(t).Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, s := range []struct{ q, want string }{
		{"CREATE TABLE cs0 (b int NOT NULL, n int, a time)", "[]"},
		{"INSERT INTO cs0 VALUES (1, NULL, '10:00:00')", "[]"},
		{"CREATE TABLE cs1 AS SELECT concat(to_days(20090224)) AS c1, b, b+1 c2, concat(b, 'x') c3, n c5, 5 c6, upper('a') c8, cast(b AS char) c9, b/2 c10 FROM cs0", "[]"},
		{"SELECT column_name, is_nullable FROM information_schema.columns WHERE table_name = 'cs1' ORDER BY ordinal_position",
			"[c1,YES b,NO c2,NO c3,YES c5,YES c6,NO c8,YES c9,YES c10,YES]"},
		{"CREATE TABLE cs2 AS SELECT MAX(b) + 1, CAST(a AS DATETIME), round(1, 6) FROM cs0", "[]"},
		{"SELECT column_name FROM information_schema.columns WHERE table_name = 'cs2' ORDER BY ordinal_position", "[MAX(b) + 1 CAST(a AS DATETIME) round(1, 6)]"},
	} {
		if got := connRun(t, conn, s.q); got != s.want {
			t.Errorf("%s: got %q, want %q", s.q, got, s.want)
		}
	}
}

// TestOrderByErrors: the ORDER BY of a SELECT DISTINCT, and an aggregate
// ORDER BY of a query that aggregates nothing, as MySQL 8.0.46 checks them.
func TestOrderByErrors(t *testing.T) {
	c := startCluster(t)
	conn, err := c.db(t).Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, s := range []struct{ q, want string }{
		{"CREATE TABLE ob (a int, b int, c int)", "[]"},
		{"SELECT DISTINCT b FROM ob ORDER BY c", "ERR 3065"},
		{"SELECT DISTINCT abs(b) FROM ob ORDER BY abs(b)+1", "ERR 3065"},
		{"SELECT DISTINCT min(b) FROM ob GROUP BY a ORDER BY -min(b)", "ERR 3066"},
		{"SELECT a FROM ob ORDER BY sum(b)", "ERR 3029"},
		{"SELECT DISTINCT b FROM ob ORDER BY b+1, t1.b", "ERR 1054"},
		{"SELECT DISTINCT b AS z FROM ob ORDER BY abs(z), 1, b", "[]"},
		{"SELECT DISTINCT ob.b FROM ob ORDER BY b", "[]"},
		{"SELECT DISTINCT min(b) FROM ob GROUP BY a ORDER BY min(b)", "[]"},
		{"SELECT a FROM ob ORDER BY (SELECT sum(b) FROM ob)", "[]"},
		{"SELECT a FROM ob GROUP BY a ORDER BY sum(b)", "[]"},
		{"SELECT DISTINCT * FROM ob ORDER BY c", "[]"},
		{"SELECT a FROM ob ORDER BY sum(b) OVER ()", "[]"},
	} {
		if got := connRun(t, conn, s.q); got != s.want {
			t.Errorf("%s: got %q, want %q", s.q, got, s.want)
		}
	}
}

// TestRoutineBodyErrors: statements MySQL refuses in a stored function's
// or a trigger's body (a procedure may have them).
func TestRoutineBodyErrors(t *testing.T) {
	c := startCluster(t)
	conn, err := c.db(t).Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, s := range []struct{ q, want string }{
		{"CREATE TABLE rb (a int)", "[]"},
		{"CREATE FUNCTION rb1() RETURNS int BEGIN FLUSH HOSTS; RETURN 1; END", "ERR 1336"},
		{"CREATE FUNCTION rb2() RETURNS int BEGIN CREATE TABLE rb2 (a int); RETURN 1; END", "ERR 1422"},
		{"CREATE TRIGGER rbt BEFORE INSERT ON rb FOR EACH ROW BEGIN IF NEW.a > 1 THEN TRUNCATE TABLE rb; END IF; END", "ERR 1422"},
		{"CREATE FUNCTION rb3() RETURNS int BEGIN START TRANSACTION; RETURN 1; END", "ERR 1422"},
		{"CREATE FUNCTION rb4() RETURNS int BEGIN CREATE TEMPORARY TABLE rbt (a int); RETURN 1; END", "[]"},
		{"CREATE PROCEDURE rbp() BEGIN CREATE TABLE rb3 (a int); END", "[]"},
	} {
		if got := connRun(t, conn, s.q); got != s.want {
			t.Errorf("%s: got %q, want %q", s.q, got, s.want)
		}
	}
}

// TestReadOnlyTransactions: START TRANSACTION READ ONLY, SET SESSION
// TRANSACTION READ ONLY (DDL too) and SET TRANSACTION READ ONLY (the next
// transaction only), with MySQL's error 1792.
func TestReadOnlyTransactions(t *testing.T) {
	c := startCluster(t)
	conn, err := c.db(t).Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, s := range []struct{ q, want string }{
		{"CREATE TABLE ro (a int)", "[]"},
		{"START TRANSACTION READ ONLY", "[]"},
		{"INSERT INTO ro VALUES (1)", "ERR 1792"},
		{"COMMIT", "[]"},
		{"SET SESSION TRANSACTION READ ONLY", "[]"},
		{"INSERT INTO ro VALUES (3)", "ERR 1792"},
		{"CREATE TABLE ro2 (a int)", "ERR 1792"},
		{"SET SESSION TRANSACTION READ WRITE", "[]"},
		{"SET TRANSACTION READ ONLY", "[]"},
		{"START TRANSACTION", "[]"},
		{"INSERT INTO ro VALUES (2)", "ERR 1792"},
		{"COMMIT", "[]"},
		{"INSERT INTO ro VALUES (5)", "[]"},
		{"SET TRANSACTION READ ONLY", "[]"},
		{"INSERT INTO ro VALUES (6)", "ERR 1792"},
		{"INSERT INTO ro VALUES (7)", "[]"},
		{"SELECT * FROM ro ORDER BY a", "[5 7]"},
	} {
		if got := connRun(t, conn, s.q); got != s.want {
			t.Errorf("%s: got %q, want %q", s.q, got, s.want)
		}
	}
}

// TestMiscCompat: numeric dates in WEEK and the other date functions,
// ANY_VALUE of an aggregate, and ON DUPLICATE KEY UPDATE of INSERT ...
// SELECT naming the target's column, as MySQL 8.0.46.
func TestMiscCompat(t *testing.T) {
	c := startCluster(t)
	conn, err := c.db(t).Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, s := range []struct{ q, want string }{
		{"SELECT week(19980101), week(19971231, 1), week(950101), week('xx'), year(20090224), dayofyear(20090224), month(19981231235959)", "[0,53,1,NULL,2009,55,12]"},
		{"CREATE TABLE mc1 (a int PRIMARY KEY, b int)", "[]"},
		{"CREATE TABLE mc2 (a int)", "[]"},
		{"INSERT INTO mc2 VALUES (1), (1), (2)", "[]"},
		{"INSERT INTO mc1 (a) VALUES (1)", "[]"},
		{"INSERT INTO mc1 (a) SELECT mc2.a FROM mc2 GROUP BY mc2.a ON DUPLICATE KEY UPDATE a = a + 10", "[]"},
		{"SELECT a FROM mc1 ORDER BY a", "[2 11]"},
		{"SELECT DISTINCT 1 FROM mc1 GROUP BY a ORDER BY any_value(count(*) - count(b))", "[1]"},
		{"SELECT a, any_value(count(*)) FROM mc1 GROUP BY a ORDER BY a", "[2,1 11,1]"},
	} {
		if got := connRun(t, conn, s.q); got != s.want {
			t.Errorf("%s: got %q, want %q", s.q, got, s.want)
		}
	}
}
