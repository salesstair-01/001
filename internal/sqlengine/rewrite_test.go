package sqlengine

import (
	"context"
	"testing"
)

// TestAlterRewrite pins ALTER TABLE changes that rewrite the table (a
// primary key added or dropped, a type change, NOT NULL, dropping an indexed
// column, a new AUTO_INCREMENT key) against MySQL 8.0.46.
func TestAlterRewrite(t *testing.T) {
	c := startCluster(t)
	conn, err := c.db(t).Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, s := range []struct{ q, want string }{
		{"CREATE TABLE t (a int, b varchar(10), KEY (b))", "[]"},
		{"INSERT INTO t VALUES (1,'x'),(2,'y'),(NULL,'z')", "[]"},
		{"ALTER TABLE t ADD PRIMARY KEY (a)", "ERR 1138"},
		{"SELECT * FROM t ORDER BY b", "[1,x 2,y NULL,z]"},
		{"DELETE FROM t WHERE a IS NULL", "[]"},
		{"INSERT INTO t VALUES (1,'dup')", "[]"},
		{"ALTER TABLE t ADD PRIMARY KEY (a)", "ERR 1062"},
		{"DELETE FROM t WHERE b = 'dup'", "[]"},
		{"ALTER TABLE t ADD PRIMARY KEY (a)", "[]"},
		{"INSERT INTO t VALUES (1,'again')", "ERR 1062"},
		{"SELECT a FROM t WHERE b = 'y'", "[2]"},
		{"ALTER TABLE t MODIFY b int", "ERR 1366"},
		{"SET sql_mode = ''", "[]"},
		{"INSERT INTO t VALUES (5, NULL)", "[]"},
		{"ALTER TABLE t MODIFY b varchar(10) NOT NULL", "[]"},
		{"SELECT a, b FROM t ORDER BY a", "[1,x 2,y 5,]"},
		{"SET sql_mode = DEFAULT", "[]"},
		{"ALTER TABLE t DROP PRIMARY KEY", "[]"},
		{"INSERT INTO t VALUES (1,'one')", "[]"},
		{"SELECT a, b FROM t ORDER BY a, b", "[1,one 1,x 2,y 5,]"},

		{"CREATE TABLE r (a int, b varchar(10), c int, KEY kb (b), KEY kbc (b,c))", "[]"},
		{"INSERT INTO r VALUES (3,'x',1),(1,'y',2),(2,'z',3)", "[]"},
		{"ALTER TABLE r ADD PRIMARY KEY (a)", "[]"},
		{"SELECT * FROM r WHERE b = 'y'", "[1,y,2]"},
		{"ALTER TABLE r MODIFY c bigint NOT NULL", "[]"},
		{"ALTER TABLE r DROP COLUMN b", "[]"},
		{"SHOW CREATE TABLE r", "[r,CREATE TABLE `r` (\n  `a` int NOT NULL,\n  `c` bigint NOT NULL,\n  PRIMARY KEY (`a`),\n  KEY `kbc` (`c`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci]"},
		{"ALTER TABLE r CHANGE c cc varchar(20)", "[]"},
		{"SELECT * FROM r ORDER BY a", "[1,2 2,3 3,1]"},
		{"INSERT INTO r VALUES (2,'q')", "ERR 1062"},
		{"ALTER TABLE r DROP PRIMARY KEY, ADD PRIMARY KEY (cc)", "[]"},
		{"SELECT * FROM r WHERE cc = '3'", "[2,3]"},

		{"CREATE TABLE k (id int PRIMARY KEY AUTO_INCREMENT, v int)", "[]"},
		{"INSERT INTO k (v) VALUES (1),(2),(3)", "[]"},
		{"ALTER TABLE k DROP PRIMARY KEY, MODIFY id int", "[]"},
		{"INSERT INTO k VALUES (1, 9)", "[]"},
		{"SELECT * FROM k ORDER BY id, v", "[1,1 1,9 2,2 3,3]"},
		{"ALTER TABLE k MODIFY id bigint AUTO_INCREMENT PRIMARY KEY", "ERR 1062"},
		{"DELETE FROM k WHERE v = 9", "[]"},
		{"ALTER TABLE k MODIFY id bigint AUTO_INCREMENT PRIMARY KEY", "[]"},
		{"INSERT INTO k (v) VALUES (4)", "[]"},
		{"SELECT * FROM k ORDER BY id", "[1,1 2,2 3,3 4,4]"},
	} {
		if got := connRun(t, conn, s.q); got != s.want {
			t.Errorf("%s: got %s, want %s", s.q, got, s.want)
		}
	}
}
