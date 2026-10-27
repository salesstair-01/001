package sqlengine

import (
	"context"
	"testing"
)

// TestQuantifiedComparisons pins x op ANY / SOME / ALL (subquery), a
// syntax error before, against MySQL 8.0.46, NULLs and empty subqueries
// included.
func TestQuantifiedComparisons(t *testing.T) {
	c := startCluster(t)
	conn, err := c.db(t).Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, s := range []struct{ q, want string }{
		{"CREATE TABLE t2 (b int)", "[]"},
		{"INSERT INTO t2 VALUES (1),(3),(5)", "[]"},
		{"CREATE TABLE t3 (a int)", "[]"},
		{"INSERT INTO t3 VALUES (0),(1),(3),(5),(6),(NULL)", "[]"},
		{"SELECT a FROM t3 WHERE a = SOME (SELECT b FROM t2) ORDER BY a", "[1 3 5]"},
		{"SELECT a FROM t3 WHERE a <> ANY (SELECT b FROM t2) ORDER BY a", "[0 1 3 5 6]"},
		{"SELECT a FROM t3 WHERE a < ANY (SELECT b FROM t2) ORDER BY a", "[0 1 3]"},
		{"SELECT a FROM t3 WHERE a >= ALL (SELECT b FROM t2) ORDER BY a", "[5 6]"},
		{"SELECT a FROM t3 WHERE a = ALL (SELECT b FROM t2) ORDER BY a", "[]"},
		{"SELECT a FROM t3 WHERE a <> ALL (SELECT b FROM t2) ORDER BY a", "[0 6]"},
		{"SELECT a, a > ALL (SELECT b FROM t2), a > ANY (SELECT b FROM t2) FROM t3 ORDER BY a", "[NULL,NULL,NULL 0,0,0 1,0,0 3,0,1 5,0,1 6,1,1]"},
		{"SELECT 1 > ALL (SELECT b FROM t2 WHERE b > 100), 1 > ANY (SELECT b FROM t2 WHERE b > 100), NULL > ALL (SELECT b FROM t2 WHERE b > 100)", "[1,0,1]"},
		{"INSERT INTO t2 VALUES (NULL)", "[]"},
		{"SELECT a, a > ALL (SELECT b FROM t2), a < ANY (SELECT b FROM t2) FROM t3 ORDER BY a", "[NULL,NULL,NULL 0,0,1 1,0,1 3,0,1 5,0,NULL 6,NULL,NULL]"},
		{"SELECT a FROM t3 WHERE a > ALL (SELECT b FROM t2 WHERE b IS NOT NULL AND b < t3.a) ORDER BY a", "[NULL 0 1 3 5 6]"},
		{"SELECT 1 > ALL (SELECT 1, 2)", "ERR 1241"},
		// any and some are still names
		{"SELECT any, some FROM (SELECT 1 AS any, 2 AS some) x", "[1,2]"},
	} {
		if got := connRun(t, conn, s.q); got != s.want {
			t.Errorf("%s: got %s, want %s", s.q, got, s.want)
		}
	}
}
