package sqlengine

import (
	"context"
	"strings"
	"testing"
)

func TestViewsPersist(t *testing.T) {
	c := startCluster(t)
	db := c.db(t)
	mustExec(t, db, "CREATE TABLE p (id INT PRIMARY KEY, v INT)")
	mustExec(t, db, "INSERT INTO p VALUES (1, 10), (2, 20)")
	mustExec(t, db, "CREATE VIEW big AS SELECT id FROM p WHERE v > 15")
	if _, err := db.Exec("CREATE TABLE big (x INT PRIMARY KEY)"); err == nil {
		t.Fatal("a table may not take a view's name")
	}
	c.restartKV() // the view is in the catalog, not in this engine's memory
	other := c.engineOn(t, 1)
	if got := strings.Join(queryStrings(t, other, "SELECT id FROM app.big"), ";"); got != "2" {
		t.Fatalf("view from another engine: %q", got)
	}
	if got := strings.Join(queryStrings(t, other, "SHOW FULL TABLES FROM app"), ";"); got != "big,VIEW;p,BASE TABLE" {
		t.Fatalf("SHOW FULL TABLES: %q", got)
	}
	mustExec(t, other, "DROP VIEW app.big")
	if _, err := other.Exec("DROP VIEW app.big"); err == nil {
		t.Fatal("dropping a missing view")
	}
	mustExec(t, other, "DROP VIEW IF EXISTS app.big")
}

func TestTruncate(t *testing.T) {
	c := startCluster(t)
	db := c.db(t)
	mustExec(t, db, "CREATE TABLE t (id INT AUTO_INCREMENT PRIMARY KEY, v VARCHAR(5), UNIQUE KEY uv (v))")
	mustExec(t, db, "INSERT INTO t (v) VALUES ('a'), ('b'), ('c')")
	mustExec(t, db, "TRUNCATE TABLE t")
	if got := strings.Join(queryStrings(t, db, "SELECT COUNT(*) FROM t"), ";"); got != "0" {
		t.Fatalf("after TRUNCATE: %s rows", got)
	}
	mustExec(t, db, "INSERT INTO t (v) VALUES ('a')") // the unique index is empty too
	if got := strings.Join(queryStrings(t, db, "SELECT id, v FROM t"), ";"); got != "1,a" {
		t.Fatalf("AUTO_INCREMENT restarts after TRUNCATE: %q", got)
	}
	mustExec(t, db, "CREATE TABLE k (v INT)")
	mustExec(t, db, "INSERT INTO k VALUES (1), (1)")
	mustExec(t, db, "TRUNCATE k")
	mustExec(t, db, "INSERT INTO k VALUES (2)")
	if got := strings.Join(queryStrings(t, db, "SELECT v FROM k"), ";"); got != "2" {
		t.Fatalf("keyless table after TRUNCATE: %q", got)
	}
}

// TestWriteThroughViews: INSERT, UPDATE and DELETE of a view whose
// definition selects columns of one table write that table, as MySQL 8.0.46
// does, and a view MySQL can't write is refused with its error.
func TestWriteThroughViews(t *testing.T) {
	c := startCluster(t)
	conn, err := c.db(t).Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, s := range []struct{ q, want string }{
		{"CREATE TABLE t1 (a int PRIMARY KEY, b int, c int)", "[]"},
		{"CREATE VIEW v1 AS SELECT a, b AS bb, c+1 AS cc FROM t1 WHERE a > 0", "[]"},
		{"CREATE VIEW v2 AS SELECT * FROM t1", "[]"},
		{"CREATE VIEW v3 AS SELECT a, bb FROM v1 WHERE bb < 100", "[]"},
		{"CREATE VIEW v4 AS SELECT count(*) n FROM t1", "[]"},
		{"INSERT INTO v1 (a, bb) VALUES (1, 10)", "ERR 1471"},
		{"INSERT INTO v1 VALUES (3, 30, 5)", "ERR 1348"},
		{"INSERT INTO v4 VALUES (1)", "ERR 1471"},
		{"UPDATE v4 SET n = 1", "ERR 1288"},
		{"INSERT INTO v2 VALUES (5, 50, 500)", "[]"},
		{"INSERT INTO v3 VALUES (6, 60)", "[]"},
		{"SELECT * FROM t1", "[5,50,500 6,60,NULL]"},
		{"UPDATE v2 SET c = 9 WHERE b > 40", "[]"},
		{"INSERT INTO t1 VALUES (-1, 1, 1)", "[]"},
		{"UPDATE v1 SET bb = 99", "[]"},
		{"SELECT * FROM t1", "[-1,1,1 5,99,9 6,99,9]"},
		{"DELETE FROM v3 WHERE a = 5", "[]"},
		{"DELETE FROM v1 WHERE bb = 99", "[]"},
		{"SELECT * FROM t1", "[-1,1,1]"},
	} {
		if got := connRun(t, conn, s.q); got != s.want {
			t.Errorf("%s: got %q, want %q", s.q, got, s.want)
		}
	}
}
