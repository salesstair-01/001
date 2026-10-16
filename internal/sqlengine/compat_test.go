package sqlengine

import (
	"strings"
	"testing"
)

// Fixes found by the MySQL differential tests (mysqldiff_test.go).
func TestMySQLCompatFixes(t *testing.T) {
	c := startCluster(t)
	db := c.db(t)
	check := func(q, want string) {
		t.Helper()
		if got := strings.Join(queryStrings(t, db, q), ";"); got != want {
			t.Errorf("%s: got %q, want %q", q, got, want)
		}
	}
	// Columns without a collation get MySQL 8's default, utf8mb4_0900_ai_ci.
	mustExec(t, db, "CREATE DATABASE d2")
	mustExec(t, db, "CREATE TABLE d2.t (id INT PRIMARY KEY, s VARCHAR(20), KEY ks (s))")
	mustExec(t, db, "INSERT INTO d2.t VALUES (1, 'abc'), (2, 'ab😀'), (3, '9中'), (4, 'b')")
	check("SELECT COLLATION(s) FROM d2.t WHERE id = 1", "utf8mb4_0900_ai_ci")
	check("SELECT id FROM d2.t WHERE s = 'ABC'", "1")
	// A prefix LIKE keeps strings that continue past U+00FF.
	check("SELECT id FROM d2.t WHERE s LIKE 'ab%' ORDER BY id", "1;2")
	check("SELECT id FROM d2.t WHERE s LIKE '9%'", "3")
	// LIKE on a utf8mb4_0900_bin column (stored without its collation once).
	mustExec(t, db, "CREATE TABLE b (id INT PRIMARY KEY, s VARCHAR(20) COLLATE utf8mb4_0900_bin)")
	mustExec(t, db, "INSERT INTO b VALUES (1, 'abc'), (2, 'ABC')")
	check("SELECT id FROM b WHERE s LIKE 'ab%'", "1")
	check("SELECT id FROM b WHERE s COLLATE utf8mb4_0900_bin = 'abc'", "1")
	// CHAR drops trailing spaces on store.
	mustExec(t, db, "CREATE TABLE ch (c CHAR(4) PRIMARY KEY)")
	mustExec(t, db, "INSERT INTO ch VALUES ('a  ')")
	check("SELECT CONCAT('[', c, ']') FROM ch", "[a]")
	if _, err := db.Exec("INSERT INTO ch VALUES ('a')"); mysqlCode(err) != 1062 {
		t.Errorf("'a' after 'a  ' in a CHAR key: %v", err)
	}
}
