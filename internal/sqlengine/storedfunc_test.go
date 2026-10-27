package sqlengine

import (
	"context"
	"testing"
)

// TestStoredFunctions pins issue #20's stored functions against MySQL
// 8.0.46's results for the same statements.
func TestStoredFunctions(t *testing.T) {
	c := startCluster(t)
	conn, err := c.db(t).Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, s := range []struct{ q, want string }{
		{"CREATE TABLE t (id int PRIMARY KEY, v int)", "[]"},
		{"INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)", "[]"},
		{"CREATE FUNCTION add1(a int) RETURNS int DETERMINISTIC RETURN a + 1", "[]"},
		{"SELECT add1(41)", "[42]"},
		{"SELECT id, add1(v) FROM t ORDER BY id", "[1,11 2,21 3,31]"},
		{"SELECT id FROM t WHERE add1(v) = 21", "[2]"},
		{"CREATE FUNCTION sign3(x int) RETURNS varchar(10) DETERMINISTIC BEGIN IF x > 0 THEN RETURN 'pos'; ELSEIF x < 0 THEN RETURN 'neg'; END IF; RETURN 'zero'; END", "[]"},
		{"SELECT sign3(5), sign3(-5), sign3(0)", "[pos,neg,zero]"},
		{"CREATE FUNCTION total() RETURNS int READS SQL DATA BEGIN DECLARE s int; SELECT SUM(v) INTO s FROM t; RETURN s; END", "[]"},
		{"SELECT total()", "[60]"},
		{"CREATE FUNCTION fact(n int) RETURNS bigint DETERMINISTIC BEGIN DECLARE r bigint DEFAULT 1; WHILE n > 1 DO SET r = r * n; SET n = n - 1; END WHILE; RETURN r; END", "[]"},
		{"SELECT fact(10)", "[3628800]"},
		{"SELECT add1(fact(3))", "[7]"},
		{"CREATE FUNCTION nul() RETURNS int RETURN NULL", "[]"},
		{"SELECT nul() IS NULL", "[1]"},
		{"CREATE FUNCTION Twice(s varchar(20)) RETURNS varchar(40) CHARSET utf8mb4 RETURN CONCAT(s, s)", "[]"},
		{"SELECT twice('ab'), TWICE('c')", "[abab,cc]"},
		{"CREATE FUNCTION add1(a int) RETURNS int RETURN a", "ERR 1304"},
		{"SELECT add1(1, 2)", "ERR 1318"},
		{"CREATE FUNCTION rec(n int) RETURNS int RETURN rec(n)", "[]"},
		{"SELECT rec(1)", "ERR 1424"},
		{"DROP FUNCTION add1", "[]"},
		{"DROP FUNCTION add1", "ERR 1305"},
		{"DROP FUNCTION IF EXISTS add1", "[]"},
		{"SELECT add1(1)", "ERR 1305"},
		// routines with parameters of the same name keep their own types and
		// values (they took the last routine's)
		{"CREATE FUNCTION p1d (par1 decimal(18,2)) RETURNS decimal(18,2) RETURN par1", "[]"},
		{"CREATE FUNCTION p1i (par1 int) RETURNS int RETURN par1 + 1", "[]"},
		{"SELECT p1i(1), p1d(1.5), p1i(1)", "[2,1.50,2]"},
		{"CREATE PROCEDURE p1s (par1 varchar(5), OUT r varchar(10)) SET r = concat(par1, '!')", "[]"},
		{"CREATE PROCEDURE p1o (par1 int) BEGIN CALL p1s('ab', @r); SELECT @r, par1 + 1, p1i(par1); END", "[]"},
		{"CALL p1o(41)", "[ab!,42,42]"},
	} {
		if got := connRun(t, conn, s.q); got != s.want {
			t.Errorf("%s: got %s, want %s", s.q, got, s.want)
		}
	}
}

// TestStoredFunctionWrites pins functions that write, called from a
// statement and from a trigger, against MySQL 8.0.46.
func TestStoredFunctionWrites(t *testing.T) {
	c := startCluster(t)
	conn, err := c.db(t).Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, s := range []struct{ q, want string }{
		{"CREATE TABLE log (msg varchar(20))", "[]"},
		{"CREATE FUNCTION logit(m varchar(20)) RETURNS int MODIFIES SQL DATA BEGIN INSERT INTO log VALUES (m); RETURN 1; END", "[]"},
		{"SELECT logit('a')", "[1]"},
		{"SELECT msg FROM log", "[a]"},
		{"CREATE TABLE t (id int PRIMARY KEY)", "[]"},
		{"CREATE TABLE seq (n int)", "[]"},
		{"INSERT INTO seq VALUES (10)", "[]"},
		{"CREATE FUNCTION mx() RETURNS int RETURN (SELECT MAX(n) FROM seq)", "[]"},
		{"CREATE TRIGGER t_bi BEFORE INSERT ON t FOR EACH ROW BEGIN IF NEW.id > mx() THEN SET NEW.id = mx(); END IF; END", "[]"},
		{"INSERT INTO t VALUES (1)", "[]"},
		{"INSERT INTO t VALUES (50)", "[]"},
		{"SELECT id FROM t ORDER BY id", "[1 10]"},
		{"CREATE TRIGGER t_ai AFTER INSERT ON t FOR EACH ROW SET @r = logit('trig')", "[]"},
		{"INSERT INTO t VALUES (2)", "[]"},
		{"SELECT msg FROM log ORDER BY msg", "[a trig]"},
		{"BEGIN", "[]"},
		{"SELECT logit('rolled')", "[1]"},
		{"ROLLBACK", "[]"},
		{"SELECT COUNT(*) FROM log", "[2]"},
		// a function may not change a table its caller uses
		{"CREATE TABLE u (val int)", "[]"},
		{"INSERT INTO u VALUES (1)", "[]"},
		{"CREATE FUNCTION wu(i int) RETURNS int BEGIN INSERT INTO u VALUES (i); RETURN 0; END", "[]"},
		{"SELECT val, wu(val) FROM u", "ERR 1442"},
		{"SELECT val, wu(val) FROM u AS x", "ERR 1442"},
		{"UPDATE u SET val = wu(val)", "ERR 1442"},
		{"SELECT (SELECT wu(1))", "[0]"},
		{"SELECT COUNT(*) FROM u", "[2]"},
	} {
		if got := connRun(t, conn, s.q); got != s.want {
			t.Errorf("%s: got %s, want %s", s.q, got, s.want)
		}
	}
}
