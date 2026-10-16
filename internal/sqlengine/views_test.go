package sqlengine

import (
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
