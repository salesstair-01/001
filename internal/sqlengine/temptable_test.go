package sqlengine

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
)

// connRun runs q on conn and returns its rows, or "ERR <code>".
func connRun(t *testing.T, conn *sql.Conn, q string) string {
	t.Helper()
	rows, err := conn.QueryContext(context.Background(), q)
	if err != nil {
		return "ERR " + errCode(err)
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	var out []string
	for rows.Next() {
		v := make([]sql.NullString, len(cols))
		p := make([]any, len(cols))
		for i := range v {
			p[i] = &v[i]
		}
		if err := rows.Scan(p...); err != nil {
			t.Fatal(err)
		}
		var s []string
		for _, x := range v {
			if x.Valid {
				s = append(s, x.String)
			} else {
				s = append(s, "NULL")
			}
		}
		out = append(out, strings.Join(s, ","))
	}
	if err := rows.Err(); err != nil {
		return "ERR " + errCode(err)
	}
	return fmt.Sprint(out)
}

func errCode(err error) string {
	s := err.Error()
	if strings.HasPrefix(s, "Error ") {
		if i := strings.IndexByte(s[6:], ' '); i > 0 {
			return s[6 : 6+i]
		}
	}
	return s
}

// Temporary tables (issue #22), against MySQL 8.0.46's behaviour.
func TestTemporaryTables(t *testing.T) {
	c := startCluster(t)
	db := c.db(t)
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, tc := range []struct{ q, want string }{
		{"CREATE TABLE t (a int primary key, b varchar(10))", "[]"},
		{"INSERT INTO t VALUES (1,'perm')", "[]"},
		// A temporary table shadows the permanent one.
		{"CREATE TEMPORARY TABLE t (a int primary key, b varchar(10))", "[]"},
		{"CREATE TEMPORARY TABLE t (a int)", "ERR 1050"},
		{"INSERT INTO t VALUES (1,'tmp'),(2,'x')", "[]"},
		{"SELECT * FROM t ORDER BY a", "[1,tmp 2,x]"},
		// A failed statement and ROLLBACK undo their rows.
		{"INSERT INTO t VALUES (3,'y'),(2,'dup')", "ERR 1062"},
		{"SELECT * FROM t ORDER BY a", "[1,tmp 2,x]"},
		{"BEGIN", "[]"}, {"INSERT INTO t VALUES (9,'r')", "[]"}, {"ROLLBACK", "[]"},
		{"BEGIN", "[]"}, {"UPDATE t SET b = 'c' WHERE a = 2", "[]"}, {"COMMIT", "[]"},
		{"INSERT INTO t VALUES (1,'z') ON DUPLICATE KEY UPDATE b = 'odku'", "[]"},
		{"SELECT * FROM t ORDER BY a", "[1,odku 2,c]"},
		{"REPLACE INTO t VALUES (2,'repl')", "[]"},
		{"DELETE FROM t WHERE a = 1", "[]"},
		{"SELECT * FROM t", "[2,repl]"},
		{"SHOW TABLES", "[t]"},
		// Unique keys follow the column's collation; AUTO_INCREMENT.
		{"CREATE TEMPORARY TABLE u (id int auto_increment primary key, s varchar(5), unique key us(s))", "[]"},
		{"INSERT INTO u (s) VALUES ('a'),('b')", "[]"},
		{"INSERT INTO u (s) VALUES ('A')", "ERR 1062"},
		{"SELECT * FROM u ORDER BY id", "[1,a 2,b]"},
		// Joins use lookups on the temporary table.
		{"SELECT t.a, u.s FROM t JOIN u ON u.id = t.a", "[2,b]"},
		{"SELECT u.id, t.b FROM u LEFT JOIN t ON t.a = u.id ORDER BY u.id", "[1,NULL 2,repl]"},
		// LIKE, AS SELECT, ALTER TABLE, TRUNCATE.
		{"CREATE TEMPORARY TABLE t2 LIKE t", "[]"},
		{"CREATE TEMPORARY TABLE t3 AS SELECT a, b FROM t", "[]"},
		{"ALTER TABLE t3 ADD COLUMN c int DEFAULT 7", "[]"},
		{"INSERT INTO t3 VALUES (2, 'x', 8)", "[]"},
		{"ALTER TABLE t3 ADD PRIMARY KEY (a)", "ERR 1062"},
		{"ALTER TABLE t3 DROP COLUMN b", "[]"},
		{"SELECT * FROM t3 ORDER BY c", "[2,7 2,8]"},
		{"TRUNCATE t3", "[]"},
		{"SELECT COUNT(*) FROM t3", "[0]"},
		// DROP TEMPORARY TABLE drops only temporary tables.
		{"DROP TEMPORARY TABLE t", "[]"},
		{"SELECT * FROM t", "[1,perm]"},
		{"DROP TEMPORARY TABLE t", "ERR 1051"},
		{"DROP TEMPORARY TABLE IF EXISTS t", "[]"},
		{"SELECT * FROM t", "[1,perm]"},
		// A keyless temporary table.
		{"CREATE TEMPORARY TABLE k (x int)", "[]"},
		{"INSERT INTO k VALUES (1),(1),(2)", "[]"},
		{"DELETE FROM k WHERE x = 1", "[]"},
		{"SELECT * FROM k", "[2]"},
	} {
		if got := connRun(t, conn, tc.q); got != tc.want {
			t.Errorf("%s: %s, want %s", tc.q, got, tc.want)
		}
	}
	// Other sessions don't see them.
	other, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if got := connRun(t, other, "SELECT * FROM u"); got != "ERR 1146" {
		t.Errorf("another session's temporary table: %s", got)
	}
}

// go-mysql-server's 1105 errors carry MySQL's codes.
func TestErrorCodes(t *testing.T) {
	c := startCluster(t)
	conn, err := c.db(t).Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	connRun(t, conn, "CREATE TABLE ec (a int)")
	for q, want := range map[string]string{
		"SELECT * FROM":                    "ERR 1064",
		"SELECT nosuch FROM ec":            "ERR 1054",
		"CREATE TABLE ec (a int)":          "ERR 1050",
		"INSERT INTO ec VALUES (1, 2)":     "ERR 1136",
		"SELECT nosuchfunction(1)":         "ERR 1305",
		"SELECT a FROM ec JOIN ec e2 ON 1": "ERR 1052",
	} {
		if got := connRun(t, conn, q); got != want {
			t.Errorf("%s: %s, want %s", q, got, want)
		}
	}
}
