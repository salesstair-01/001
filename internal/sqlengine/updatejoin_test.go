package sqlengine

import (
	"context"
	"testing"
)

// A multi-table UPDATE that only changes case stores the new value and
// counts the row, as MySQL 8.0.46 does (issue #29).
func TestCaseOnlyUpdateJoin(t *testing.T) {
	c := startCluster(t)
	conn, err := c.db(t).Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, tc := range []struct{ q, want string }{
		{"CREATE TABLE uj_a (id int primary key, v varchar(5))", "[]"},
		{"CREATE TABLE uj_b (id int primary key, w varchar(5))", "[]"},
		{"INSERT INTO uj_a VALUES (1,'b'),(2,'c')", "[]"},
		{"INSERT INTO uj_b VALUES (1,'B'),(2,'c')", "[]"},
		{"UPDATE uj_a JOIN uj_b ON uj_a.id = uj_b.id SET uj_a.v = uj_b.w", "[]"},
		{"SELECT ROW_COUNT()", "[1]"},
		{"SELECT id, v, HEX(v) FROM uj_a ORDER BY id", "[1,B,42 2,c,63]"},
		{"UPDATE uj_a JOIN uj_b ON uj_a.id = uj_b.id SET uj_a.v = UPPER(uj_b.w), uj_b.w = LOWER(uj_b.w)", "[]"},
		{"SELECT ROW_COUNT()", "[2]"},
		{"SELECT uj_a.v, uj_b.w FROM uj_a JOIN uj_b USING (id) ORDER BY id", "[B,b C,c]"},
		{"UPDATE uj_a, uj_b SET uj_a.v = 'zz' WHERE uj_a.id = uj_b.id AND uj_b.w = 'C'", "[]"},
		{"SELECT * FROM uj_a ORDER BY id", "[1,B 2,zz]"},
	} {
		if got := connRun(t, conn, tc.q); got != tc.want {
			t.Errorf("%s: %s, want %s", tc.q, got, tc.want)
		}
	}
}

// TestKeylessUpdateJoin: a multi-table UPDATE of tables without a primary
// key (issue #29), identical rows included, against MySQL 8.0.46's rows and
// affected counts.
func TestKeylessUpdateJoin(t *testing.T) {
	c := startCluster(t)
	conn, err := c.db(t).Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, q := range []string{
		"CREATE TABLE k1 (i int, j int)",
		"CREATE TABLE k2 (i int, v varchar(5))",
		"INSERT INTO k1 VALUES (1, 10), (1, 10), (2, 20), (3, 30)",
		"INSERT INTO k2 VALUES (1, 'a'), (1, 'b'), (2, 'c')",
	} {
		if got := connRun(t, conn, q); got != "[]" {
			t.Fatalf("%s: %s", q, got)
		}
	}
	for _, s := range []struct {
		q        string
		affected int64
		k1, k2   string
	}{
		{"UPDATE k1, k2 SET j = j + 1 WHERE k1.i = k2.i", 3, "[1,11 1,11 2,21 3,30]", "[1,a 1,b 2,c]"},
		{"UPDATE k1 JOIN k2 ON k1.i = k2.i SET k1.j = 0, k2.v = 'z' WHERE k2.v <> 'b'", 5, "[1,0 1,0 2,0 3,30]", "[1,b 1,z 2,z]"},
		{"UPDATE k1 LEFT JOIN k2 ON k1.i = k2.i SET k1.j = 99 WHERE k2.i IS NULL", 1, "[1,0 1,0 2,0 3,99]", "[1,b 1,z 2,z]"},
	} {
		res, err := conn.ExecContext(context.Background(), s.q)
		if err != nil {
			t.Fatalf("%s: %v", s.q, err)
		}
		if n, _ := res.RowsAffected(); n != s.affected {
			t.Errorf("%s: %d rows affected, want %d", s.q, n, s.affected)
		}
		if got := connRun(t, conn, "SELECT * FROM k1 ORDER BY i, j"); got != s.k1 {
			t.Errorf("%s: k1 %s, want %s", s.q, got, s.k1)
		}
		if got := connRun(t, conn, "SELECT * FROM k2 ORDER BY i, v"); got != s.k2 {
			t.Errorf("%s: k2 %s, want %s", s.q, got, s.k2)
		}
	}
}
