package sqlengine

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// TestStatementLimits pins issue #18: @@cte_max_recursion_depth (3636, as
// MySQL counts iterations), a bound on a recursive CTE's rows (1114),
// max_execution_time and its hint (3024), and KILL QUERY (1317).
func TestStatementLimits(t *testing.T) {
	c := startCluster(t)
	db := c.db(t)
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	const cross = "WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM c WHERE n < 1000) SELECT COUNT(*) FROM c a, c b, c d"
	for _, s := range []struct{ q, want string }{
		{"SET cte_max_recursion_depth = 5", "[]"},
		{"WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM c WHERE n < 5) SELECT COUNT(*) FROM c", "[5]"},
		{"WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM c WHERE n < 6) SELECT COUNT(*) FROM c", "ERR 3636"},
		{"WITH RECURSIVE c(n) AS (SELECT 1 UNION SELECT n FROM c WHERE n < 100) SELECT COUNT(*) FROM c", "[1]"},
		{"SET cte_max_recursion_depth = DEFAULT", "[]"},
		{"WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM c) SELECT COUNT(*) FROM c", "ERR 3636"},
		// the depth counts per execution, here once per outer row
		{"SELECT (WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM c WHERE n < 999) SELECT COUNT(*) FROM c) FROM (SELECT 1 UNION ALL SELECT 2) t", "[999 999]"},
		{"CREATE TABLE t1 (a int)", "[]"},
		{"INSERT INTO t1 VALUES (1), (2)", "[]"},
		{"WITH RECURSIVE qn AS (SELECT 1 FROM t1 UNION ALL SELECT 1 FROM t1 JOIN qn) SELECT COUNT(*) FROM qn", "ERR 1114"},
		{"SET max_execution_time = 200", "[]"},
		{cross, "ERR 3024"},
		{"INSERT INTO t1 VALUES (3)", "[]"}, // not a SELECT
		{"SET max_execution_time = 0", "[]"},
		{"SELECT /*+ MAX_EXECUTION_TIME(100) */ COUNT(*) FROM (WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM c WHERE n < 1000) SELECT * FROM c) a, (WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM c WHERE n < 1000) SELECT * FROM c) b, (WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM c WHERE n < 1000) SELECT * FROM c) d", "ERR 3024"},
		{"SELECT COUNT(*) FROM t1", "[3]"},
	} {
		if got := connRun(t, conn, s.q); got != s.want {
			t.Errorf("%s: got %s, want %s", s.q, got, s.want)
		}
	}

	// KILL QUERY from another session interrupts the statement.
	var id int64
	if err := conn.QueryRowContext(context.Background(), "SELECT CONNECTION_ID()").Scan(&id); err != nil {
		t.Fatal(err)
	}
	done := make(chan string, 1)
	start := time.Now()
	go func() { done <- connRun(t, conn, cross) }()
	time.Sleep(300 * time.Millisecond)
	if _, err := db.Exec(fmt.Sprintf("KILL QUERY %d", id)); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-done:
		if got != "ERR 1317" {
			t.Errorf("killed query: got %s, want ERR 1317", got)
		}
		if d := time.Since(start); d > 5*time.Second {
			t.Errorf("killed query took %v", d)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("KILL QUERY did not stop the query")
	}
	if got := connRun(t, conn, "SELECT 1"); got != "[1]" {
		t.Errorf("after KILL QUERY: got %s", got)
	}
}

// TestMemoryBudget: past the budget, a statement that buffers rows fails
// with 1041 and the engine serves the next one.
func TestMemoryBudget(t *testing.T) {
	c := startCluster(t)
	conn, err := c.db(t).Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	SetMemoryLimit(heapReporter.UsedMemory() + 64<<20)
	defer SetMemoryLimit(0)
	before := memRejected.Value()
	const big = "WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM c WHERE n < 300) " +
		"SELECT COUNT(*) FROM (SELECT a.n, b.n AS m, d.n AS k, REPEAT('x', 50) AS s FROM c a, c b, c d GROUP BY a.n, b.n, d.n) g"
	if got := connRun(t, conn, big); got != "ERR 1041" {
		t.Errorf("over budget: got %s, want ERR 1041", got)
	}
	if memRejected.Value() <= before {
		t.Error("s2z_sqld_memory_rejections_total did not count it")
	}
	if got := connRun(t, conn, "SELECT COUNT(*) FROM (SELECT 1 UNION ALL SELECT 2) t"); got != "[2]" {
		t.Errorf("after: got %s", got)
	}
}
