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
