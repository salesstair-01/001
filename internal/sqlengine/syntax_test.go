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
		// the table spec's keys, checks and options stay; a failed copy
		// leaves no table
		{"CREATE TABLE s6 (PRIMARY KEY (a)) SELECT * FROM s1", "[]"},
		{"SELECT column_name, column_key FROM information_schema.columns WHERE table_name = 's6' ORDER BY ordinal_position", "[a,PRI b,]"},
		{"INSERT INTO s6 SELECT * FROM s1", "ERR 1062"},
		{"CREATE TABLE s7 (a int, KEY ka (a)) SELECT * FROM s1", "[]"},
		{"SELECT index_name, column_name FROM information_schema.statistics WHERE table_name = 's7'", "[ka,a]"},
		{"CREATE TABLE s8 ENGINE=InnoDB, CHARSET latin1 AS SELECT a FROM s1", "[]"},
		{"SELECT table_collation FROM information_schema.tables WHERE table_name = 's8'", "[latin1_swedish_ci]"},
		{"CREATE TABLE s9 (b int CHECK (b > 0)) SELECT 1 AS a, 0 AS b", "ERR 3819"},
		{"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 's9'", "[0]"},
		// @v := expr in an expression; ROW(...) constructors
		{"SET @n := 0", "[]"},
		{"SELECT a, @n := @n + 1 FROM s1 ORDER BY a", "[1,1 2,2 3,3]"},
		{"SELECT @n, @g := GROUP_CONCAT(a ORDER BY a) FROM s1", "[3,1,2,3]"},
		{"SELECT @g", "[1,2,3]"},
		{"SELECT ROW(1, 2) = ROW(1, 2), ROW(a, 1) > ROW(1, 1) FROM s1 ORDER BY a", "[1,0 1,1 1,1]"},
		{"SELECT (SELECT 1, 2, 3) = ROW(1, 2, 3)", "[1]"},
		// DO; DELETE LOW_PRIORITY / IGNORE
		{"DO 1 + 1, SLEEP(0)", "[]"},
		{"DO nosuchfn(1)", "ERR 1305"},
		{"CREATE TABLE d1 (a int primary key)", "[]"},
		{"INSERT INTO d1 VALUES (1), (2), (3)", "[]"},
		{"DELETE IGNORE FROM d1 WHERE a = 1", "[]"},
		{"DELETE LOW_PRIORITY FROM d1 WHERE a = 2", "[]"},
		{"DELETE LOW_PRIORITY IGNORE d1 FROM d1 WHERE a = 3", "[]"},
		{"SELECT COUNT(*) FROM d1", "[0]"},
		// columns of NULLs and of user variables take MySQL's types (a NULL
		// column made the table unreadable)
		{"SET @a := 1, @b := 'x', @c := 1.5, @d := NULL, @e := 1e3", "[]"},
		{"CREATE TABLE s5 SELECT @a, @b, @c, @d, @e, NULL AS n, @nosuch AS u", "[]"},
		{"SELECT * FROM s5", "[1,x,1.500000000000000000000000000000,NULL,1000,NULL,NULL]"},
		{"SELECT GROUP_CONCAT(column_type ORDER BY ordinal_position) FROM information_schema.columns WHERE table_name = 's5'", "[bigint,longtext,decimal(65,30),longblob,double,binary(0),longtext]"},
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
