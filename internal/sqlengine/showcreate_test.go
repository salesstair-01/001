package sqlengine

import (
	"context"
	"testing"
)

// TestShowCreateDefaultsAndVirtualColumns pins SHOW CREATE TABLE's NULL
// defaults and a table with a virtual column (its indexes and checks were
// hidden) against MySQL 8.0.46.
func TestShowCreateDefaultsAndVirtualColumns(t *testing.T) {
	c := startCluster(t)
	conn, err := c.db(t).Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, s := range []struct{ q, want string }{
		{"CREATE TABLE sc (i int, t text, b blob, j json, g geometry, ts timestamp NULL, ts2 timestamp(3) NULL DEFAULT NULL, dt datetime, e enum('a','b'), s set('x'), y year, bt bit(3), x int DEFAULT NULL, t2 text DEFAULT NULL, gc int GENERATED ALWAYS AS (i+1), v varchar(3) COMMENT 'c', ai int NOT NULL AUTO_INCREMENT, KEY(ai))", "[]"},
		{"SHOW CREATE TABLE sc", "[sc,CREATE TABLE `sc` (\n" +
			"  `i` int DEFAULT NULL,\n  `t` text,\n  `b` blob,\n  `j` json DEFAULT NULL,\n  `g` geometry DEFAULT NULL,\n" +
			"  `ts` timestamp NULL DEFAULT NULL,\n  `ts2` timestamp(3) NULL DEFAULT NULL,\n  `dt` datetime DEFAULT NULL,\n" +
			"  `e` enum('a','b') DEFAULT NULL,\n  `s` set('x') DEFAULT NULL,\n  `y` year DEFAULT NULL,\n  `bt` bit(3) DEFAULT NULL,\n" +
			"  `x` int DEFAULT NULL,\n  `t2` text,\n  `gc` int GENERATED ALWAYS AS ((`i` + 1)) VIRTUAL,\n" +
			"  `v` varchar(3) DEFAULT NULL COMMENT 'c',\n  `ai` int NOT NULL AUTO_INCREMENT,\n  KEY `ai` (`ai`)\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci]"},
		{"CREATE TABLE sv (i int, gc int GENERATED ALWAYS AS (i+1), ai int, KEY(ai), CHECK (i > 0))", "[]"},
		{"INSERT INTO sv (i, ai) VALUES (0, 1)", "ERR 3819"},
		{"INSERT INTO sv (i, ai) VALUES (2, 1)", "[]"},
		{"ALTER TABLE sv ADD KEY k2 (i)", "[]"},
		{"SHOW CREATE TABLE sv", "[sv,CREATE TABLE `sv` (\n  `i` int DEFAULT NULL,\n  `gc` int GENERATED ALWAYS AS ((`i` + 1)) VIRTUAL,\n" +
			"  `ai` int DEFAULT NULL,\n  KEY `ai` (`ai`),\n  KEY `k2` (`i`),\n  CONSTRAINT `sv_chk_1` CHECK ((`i` > 0))\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci]"},
		{"SELECT ai, gc FROM sv WHERE ai = 1", "[1,3]"},
	} {
		if got := connRun(t, conn, s.q); got != s.want {
			t.Errorf("%s: got %s, want %s", s.q, got, s.want)
		}
	}
}

// TestCreateSelectClockNotNull pins CREATE TABLE ... SELECT of the clock
// functions, whose columns MySQL 8.0.46 makes NOT NULL (FROM_UNIXTIME may be
// NULL).
func TestCreateSelectClockNotNull(t *testing.T) {
	c := startCluster(t)
	conn, err := c.db(t).Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, s := range []struct{ q, want string }{
		{"CREATE TABLE t1 AS SELECT NOW(0), NOW(3), SYSDATE(2), FROM_UNIXTIME(1.12)", "[]"},
		{"SHOW CREATE TABLE t1", "[t1,CREATE TABLE `t1` (\n  `NOW(0)` datetime NOT NULL,\n  `NOW(3)` datetime(3) NOT NULL,\n" +
			"  `SYSDATE(2)` datetime(2) NOT NULL,\n  `FROM_UNIXTIME(1.12)` datetime(2) DEFAULT NULL\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci]"},
	} {
		if got := connRun(t, conn, s.q); got != s.want {
			t.Errorf("%s: got %s, want %s", s.q, got, s.want)
		}
	}
}

// TestCreateSelectTypesAndDefaults pins CREATE TABLE ... SELECT's columns
// against MySQL 8.0.46: a table's column keeps its definition and default,
// an expression that is never NULL is NOT NULL with its type's zero as
// default (not a DATE or DATETIME), and a string literal is a VARCHAR of its
// length.
func TestCreateSelectTypesAndDefaults(t *testing.T) {
	c := startCluster(t)
	conn, err := c.db(t).Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, s := range []struct{ q, want string }{
		{"CREATE TABLE src (i int NOT NULL, v varchar(5) NOT NULL, d decimal(5,2) NOT NULL, t text NOT NULL, dt datetime NOT NULL, dd date NOT NULL, tm time(2) NOT NULL, ts timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP, f double NOT NULL, e enum('x','y') NOT NULL, j json NOT NULL, y year NOT NULL, b blob NOT NULL, bi bit(2) NOT NULL, iv int NOT NULL DEFAULT 7)", "[]"},
		{"CREATE TABLE cz AS SELECT i, v, d, t, dt, dd, tm, ts, f, e, j, y, b, bi, iv, i+1 AS ie, 'ab' AS s, 1.5 AS dl, CURDATE() AS cd, LEAST(TIME'00:00:00.1', TIME'00:00:00.12') AS le FROM src", "[]"},
		{"SHOW CREATE TABLE cz", "[cz,CREATE TABLE `cz` (\n  `i` int NOT NULL,\n  `v` varchar(5) NOT NULL,\n  `d` decimal(5,2) NOT NULL,\n  `t` text NOT NULL,\n  `dt` datetime NOT NULL,\n  `dd` date NOT NULL,\n  `tm` time(2) NOT NULL,\n  `ts` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n  `f` double NOT NULL,\n  `e` enum('x','y') NOT NULL,\n  `j` json NOT NULL,\n  `y` year NOT NULL,\n  `b` blob NOT NULL,\n  `bi` bit(2) NOT NULL,\n  `iv` int NOT NULL DEFAULT '7',\n  `ie` bigint NOT NULL DEFAULT '0',\n  `s` varchar(2) NOT NULL DEFAULT '',\n  `dl` decimal(2,1) NOT NULL DEFAULT '0.0',\n  `cd` date NOT NULL,\n  `le` time(2) NOT NULL DEFAULT '00:00:00.00'\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci]"},
		{"CREATE TABLE cs2 AS SELECT '' AS e, 'ab' AS s, 'é' AS u, X'4142' AS x", "[]"},
		{"SHOW CREATE TABLE cs2", "[cs2,CREATE TABLE `cs2` (\n  `e` char(0) NOT NULL DEFAULT '',\n  `s` varchar(2) NOT NULL DEFAULT '',\n  `u` varchar(1) NOT NULL DEFAULT '',\n  `x` varbinary(2) NOT NULL DEFAULT ''\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci]"},
	} {
		if got := connRun(t, conn, s.q); got != s.want {
			t.Errorf("%s: got %s, want %s", s.q, got, s.want)
		}
	}
}
