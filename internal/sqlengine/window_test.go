package sqlengine

import (
	"context"
	"testing"
)

// TestCumeDistNthValue: CUME_DIST and NTH_VALUE (issue #26), against MySQL
// 8.0.46's results.
func TestCumeDistNthValue(t *testing.T) {
	c := startCluster(t)
	conn, err := c.db(t).Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, s := range []struct{ q, want string }{
		{"CREATE TABLE wf (g int, v int)", "[]"},
		{"INSERT INTO wf VALUES (1, 10), (1, 20), (1, 20), (1, 30), (2, 5), (2, 7)", "[]"},
		{"SELECT g, v, CUME_DIST() OVER (PARTITION BY g ORDER BY v) FROM wf ORDER BY g, v", "[1,10,0.25 1,20,0.75 1,20,0.75 1,30,1 2,5,0.5 2,7,1]"},
		{"SELECT g, v, CUME_DIST() OVER () FROM wf ORDER BY g, v", "[1,10,1 1,20,1 1,20,1 1,30,1 2,5,1 2,7,1]"},
		{"SELECT g, v, NTH_VALUE(v, 2) OVER (PARTITION BY g ORDER BY v) FROM wf ORDER BY g, v", "[1,10,NULL 1,20,20 1,20,20 1,30,20 2,5,NULL 2,7,7]"},
		{"SELECT g, v, NTH_VALUE(v, 2) OVER (PARTITION BY g) FROM wf ORDER BY g, v", "[1,10,20 1,20,20 1,20,20 1,30,20 2,5,7 2,7,7]"},
		{"SELECT g, v, NTH_VALUE(v, 3) OVER (PARTITION BY g ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM wf ORDER BY g, v", "[1,10,20 1,20,20 1,20,20 1,30,20 2,5,NULL 2,7,NULL]"},
		{"SELECT g, v, NTH_VALUE(v, 1) OVER (ORDER BY v ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM wf ORDER BY v, g", "[2,5,5 2,7,5 1,10,7 1,20,10 1,20,20 1,30,20]"},
		{"SELECT NTH_VALUE(v, 0) OVER () FROM wf", "ERR 1210"},
	} {
		if got := connRun(t, conn, s.q); got != s.want {
			t.Errorf("%s: got %s, want %s", s.q, got, s.want)
		}
	}
}

// TestGroupedWindows pins window functions over a grouped query, which
// go-mysql-server refused (1140), against MySQL 8.0.46.
func TestGroupedWindows(t *testing.T) {
	c := startCluster(t)
	conn, err := c.db(t).Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, s := range []struct{ q, want string }{
		{"CREATE TABLE w (a int, b int)", "[]"},
		{"INSERT INTO w VALUES (1,10),(1,20),(2,5),(3,7),(3,8),(3,9)", "[]"},
		{"CREATE TABLE n (a int, name varchar(5))", "[]"},
		{"INSERT INTO n VALUES (1,'one'),(2,'two'),(3,'three')", "[]"},
		{"SELECT a, SUM(b), RANK() OVER (ORDER BY SUM(b) DESC) FROM w GROUP BY a ORDER BY a", "[1,30,1 2,5,3 3,24,2]"},
		{"SELECT a, COUNT(*) AS m, ROW_NUMBER() OVER (ORDER BY a) FROM w GROUP BY a ORDER BY a", "[1,2,1 2,1,2 3,3,3]"},
		{"SELECT a, SUM(SUM(b)) OVER (ORDER BY a) FROM w GROUP BY a ORDER BY a", "[1,30 2,35 3,59]"},
		{"SELECT SUM(b), ROW_NUMBER() OVER () FROM w", "[59,1]"},
		{"SELECT a, COUNT(*) FROM w GROUP BY a ORDER BY ROW_NUMBER() OVER (ORDER BY a DESC)", "[3,3 2,1 1,2]"},
		{"SELECT n.name, SUM(w.b), DENSE_RANK() OVER (ORDER BY SUM(w.b)) AS r FROM w JOIN n ON w.a = n.a GROUP BY n.name ORDER BY r", "[two,5,1 three,24,2 one,30,3]"},
		{"SELECT a, SUM(b) - LAG(SUM(b), 1, 0) OVER (ORDER BY a) AS delta FROM w GROUP BY a ORDER BY a", "[1,30 2,-25 3,19]"},
		{"SELECT a, ROW_NUMBER() OVER win, SUM(b) FROM w GROUP BY a WINDOW win AS (ORDER BY SUM(b)) ORDER BY a", "[1,3,30 2,1,5 3,2,24]"},
		{"SELECT DISTINCT COUNT(*) OVER () FROM w GROUP BY a", "[3]"},
		{"SELECT a, SUM(b), SUM(b), ROW_NUMBER() OVER (ORDER BY a DESC) FROM w GROUP BY a ORDER BY a LIMIT 2", "[1,30,30,3 2,5,5,2]"},
		{"SELECT a % 2 AS p, COUNT(*), SUM(COUNT(*)) OVER (PARTITION BY a % 2) FROM w GROUP BY a ORDER BY a", "[1,2,5 0,1,1 1,3,5]"},
		{"SELECT a, SUM(b) FROM w GROUP BY a HAVING SUM(b) > 10 ORDER BY RANK() OVER (ORDER BY SUM(b))", "[3,24 1,30]"},
	} {
		if got := connRun(t, conn, s.q); got != s.want {
			t.Errorf("%s: got %s, want %s", s.q, got, s.want)
		}
	}
}
