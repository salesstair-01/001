package sqlengine

import (
	"context"
	"testing"
)

// utf8mb3, latin1 and ascii tables (issue #24), as MySQL 8.0.46 behaves;
// ordering and comparisons are in TestMySQLDiffCollations.
func TestOlderCharacterSets(t *testing.T) {
	c := startCluster(t)
	conn, err := c.db(t).Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, tc := range []struct{ q, want string }{
		{"CREATE TABLE l1 (s varchar(10)) DEFAULT CHARSET=latin1", "[]"},
		{"INSERT INTO l1 VALUES ('ž€'), ('åäö')", "[]"},
		{"INSERT INTO l1 VALUES ('ı')", "ERR 1366"},
		{"SELECT s FROM l1 ORDER BY s", "[åäö ž€]"},
		{"CREATE TABLE u3 (s varchar(10) CHARACTER SET utf8, k varchar(10) COLLATE utf8mb3_bin, KEY (k))", "[]"},
		{"INSERT INTO u3 VALUES ('中', 'b'), ('İ', 'B')", "[]"},
		{"INSERT INTO u3 VALUES ('😀', 'x')", "ERR 1366"},
		{"SELECT COUNT(*) FROM u3 WHERE k = 'b'", "[1]"},
		{"CREATE TABLE a1 (s varchar(10) CHARACTER SET ascii)", "[]"},
		{"INSERT INTO a1 VALUES ('é')", "ERR 1366"},
		{"INSERT INTO a1 VALUES ('abc')", "[]"},
		{"CREATE TEMPORARY TABLE tl (s varchar(10)) DEFAULT CHARSET=latin1", "[]"},
		{"INSERT INTO tl VALUES ('ı')", "ERR 1366"},
	} {
		if got := connRun(t, conn, tc.q); got != tc.want {
			t.Errorf("%s: %s, want %s", tc.q, got, tc.want)
		}
	}
}
