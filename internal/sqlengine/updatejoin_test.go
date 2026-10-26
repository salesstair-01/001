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
