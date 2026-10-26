package sqlengine

import (
	"context"
	"testing"
)

// TestOutOfRangeIndexKeys pins comparisons of an indexed integer column
// with a constant outside its range against MySQL 8.0.46: go-mysql-server
// failed the query (1366) building the index lookup.
func TestOutOfRangeIndexKeys(t *testing.T) {
	c := startCluster(t)
	conn, err := c.db(t).Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, s := range []struct{ q, want string }{
		{"CREATE TABLE t1 (some_id smallint unsigned, key (some_id))", "[]"},
		{"INSERT INTO t1 VALUES (1),(2)", "[]"},
		{"SELECT some_id FROM t1 WHERE some_id NOT IN ('-1', '0')", "[1 2]"},
		{"SELECT some_id FROM t1 WHERE some_id IN ('-1', '1')", "[1]"},
		{"SELECT some_id FROM t1 WHERE some_id = '-1'", "[]"},
		{"SELECT some_id FROM t1 WHERE some_id <> '-1'", "[1 2]"},
		{"SELECT some_id FROM t1 WHERE some_id > '-5'", "[1 2]"},
		{"SELECT some_id FROM t1 WHERE some_id < '-5'", "[]"},
		{"SELECT some_id FROM t1 WHERE some_id < '99999'", "[1 2]"},
		{"SELECT some_id FROM t1 WHERE some_id >= '99999'", "[]"},
		{"SELECT some_id FROM t1 WHERE some_id = 'abc'", "[]"},
	} {
		if got := connRun(t, conn, s.q); got != s.want {
			t.Errorf("%s: got %s, want %s", s.q, got, s.want)
		}
	}
}
