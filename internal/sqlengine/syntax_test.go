package sqlengine

import (
	"context"
	"testing"
)

// Statements MySQL accepts that the vitess parser refused (issue #27; the
// grammar changes are in third_party/vitess), with MySQL 8.0.46's results.
func TestMySQLSyntax(t *testing.T) {
	c := startCluster(t)
	conn, err := c.db(t).Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, tc := range []struct{ q, want string }{
		{"CREATE TABLE s1 (a int, b varchar(5))", "[]"},
		{"INSERT INTO s1 VALUES (1,'x'),(2,'y'),(3,'z')", "[]"},
		// CREATE TABLE (columns) SELECT: columns by name, others take defaults.
		{"CREATE TABLE s2 (a int) SELECT * FROM s1", "[]"},
		{"SELECT * FROM s2 ORDER BY a", "[1,x 2,y 3,z]"},
		{"CREATE TABLE s3 (c int default 9, a bigint) SELECT a, b FROM s1", "[]"},
		{"SELECT * FROM s3 ORDER BY a", "[9,1,x 9,2,y 9,3,z]"},
		{"CREATE TABLE s4 (k int primary key) AS SELECT a AS k FROM s1", "[]"},
		{"SELECT * FROM s4 ORDER BY k", "[1 2 3]"},
		{"SELECT NULL IS UNKNOWN, 1 IS UNKNOWN, NULL IS NOT UNKNOWN, (1=1) IS NOT UNKNOWN", "[1,0,0,1]"},
		{"LOCK TABLE s1 READ", "[]"},
		{"UNLOCK TABLES", "[]"},
		{"DROP TABLES s2, s3", "[]"},
		{"CREATE TABLE y1 (y year(4), b blob(1), t text(100), b2 blob(70000))", "[]"},
		{"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'y1' ORDER BY ordinal_position", "[y,year b,tinyblob t,text b2,mediumblob]"},
		{"CREATE TABLE y2 (y year(2))", "ERR 1818"},
		{"(SELECT a FROM s1 ORDER BY a DESC LIMIT 2) ORDER BY a LIMIT 1", "[2]"},
		{"(SELECT a FROM s1) ORDER BY a DESC", "[3 2 1]"},
		{"(SELECT 1)", "[1]"},
		{"SELECT a FROM s1 USE INDEX FOR JOIN (PRIMARY) WHERE a > 2", "[3]"},
		{"CREATE TABLE k1 (c1 varchar(10), KEY USING BTREE (c1))", "[]"},
		{"CREATE TEMPORARY TABLE k2 (f1 int, f2 int)", "[]"},
		{"ALTER TABLE k2 ADD CONSTRAINT PRIMARY KEY (f1)", "[]"},
		{"ALTER TABLE k2 DROP PRIMARY KEY, ADD CONSTRAINT pk PRIMARY KEY (f2)", "[]"},
		{"SELECT a, STD(a) OVER () std FROM s1 WHERE a = 1", "[1,0]"},
	} {
		if got := connRun(t, conn, tc.q); got != tc.want {
			t.Errorf("%s: %s, want %s", tc.q, got, tc.want)
		}
	}
}
