package sqlengine

import (
	"context"
	"testing"
)

// TestRollup pins GROUP BY ... WITH ROLLUP, a syntax error before, against
// MySQL 8.0.46: values, row order, GROUPING(), HAVING, empty input.
func TestRollup(t *testing.T) {
	c := startCluster(t)
	conn, err := c.db(t).Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, s := range []struct{ q, want string }{
		{"CREATE TABLE t (i int, j int, k varchar(3))", "[]"},
		{"INSERT INTO t VALUES (1,1,'a'),(2,1,'b'),(3,2,'a'),(4,2,NULL),(5,128,'c')", "[]"},
		{"SELECT SUM(i) s, j FROM t WHERE j != 128 GROUP BY j WITH ROLLUP", "[3,1 7,2 10,NULL]"},
		{"SELECT SUM(i) s, j FROM t GROUP BY j WITH ROLLUP HAVING j != 128", "[3,1 7,2]"},
		{"SELECT j, k, COUNT(*), GROUPING(j), GROUPING(k) FROM t GROUP BY j, k WITH ROLLUP",
			"[1,a,1,0,0 1,b,1,0,0 1,NULL,2,0,1 2,NULL,1,0,0 2,a,1,0,0 2,NULL,2,0,1 128,c,1,0,0 128,NULL,1,0,1 NULL,NULL,5,1,1]"},
		{"SELECT j, GROUP_CONCAT(i ORDER BY i) FROM t GROUP BY j WITH ROLLUP", "[1,1,2 2,3,4 128,5 NULL,1,2,3,4,5]"},
		{"SELECT j, SUM(i) FROM t GROUP BY j WITH ROLLUP ORDER BY j DESC", "[128,5 2,7 1,3 NULL,15]"},
		{"SELECT j, SUM(i) FROM t GROUP BY j WITH ROLLUP LIMIT 2", "[1,3 2,7]"},
		{"SELECT SUM(i) FROM t WHERE i > 100 GROUP BY j WITH ROLLUP", "[]"},
		{"SELECT IFNULL(k, 'all') AS kk, COUNT(*) FROM t GROUP BY k WITH ROLLUP", "[all,1 a,2 b,1 c,1 all,5]"},
		{"SELECT j + 1, COUNT(*) FROM t GROUP BY j + 1 WITH ROLLUP", "[2,2 3,2 129,1 NULL,5]"},
		{"SELECT * FROM (SELECT j, SUM(i) AS s FROM t GROUP BY j WITH ROLLUP) d WHERE j IS NULL", "[NULL,15]"},
		{"SELECT rollup FROM (SELECT 1 AS rollup) x", "[1]"},
	} {
		if got := connRun(t, conn, s.q); got != s.want {
			t.Errorf("%s: got %s, want %s", s.q, got, s.want)
		}
	}
}
