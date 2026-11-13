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
