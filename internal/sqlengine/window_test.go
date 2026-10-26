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
