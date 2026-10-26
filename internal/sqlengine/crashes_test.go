package sqlengine

import (
	"fmt"
	"strings"
	"testing"
)

// Statements from MySQL's test suite that panicked in go-mysql-server or
// vitess (issue #17); each now gives MySQL 8.0.46's result.
func TestMySQLSuitePanics(t *testing.T) {
	c := startCluster(t)
	db := c.db(t)
	for _, q := range []string{
		"CREATE TABLE an1 (a1 int, a2 char(3), key k1(a1), key k2(a2))",
		"INSERT INTO an1 VALUES (10,'aaa'), (10,null), (10,'bbb'), (20,'zzz')",
		"CREATE TABLE an2 (a1 char(3), a2 int, a3 real, key k1(a1), key k2(a2, a1))",
		"CREATE TABLE an3 (a int primary key, b varbinary(10), key kb(b))",
		"INSERT INTO an3 VALUES (1, 'x'), (2, 'y')",
	} {
		mustExec(t, db, q)
	}
	// ANALYZE of an empty table divided by zero; []byte keys panicked.
	for _, q := range []string{"ANALYZE TABLE an1, an2", "ANALYZE TABLE an2", "ANALYZE TABLE an3"} {
		if got := fmt.Sprint(queryStrings(t, db, q)); !strings.Contains(got, "OK") {
			t.Errorf("%s: %s", q, got)
		}
	}
	for _, tc := range []struct{ q, want string }{
		{`select 'a' like 'a' escape '', 'a\\b' like 'a\\b' escape '', 'a%' like 'a\\%' escape ''`, "[1,1,0]"},
		{"select 1 + /*!2*/", "[3]"},
		{"SELECT INET6_ATON(123), INET6_ATON(123.45), hex(INET6_ATON('::1'))", "[NULL,NULL,00000000000000000000000000000001]"},
		{"SELECT format('nan', 2), length(format(-1.79e307, 256)), format(1234567.891, 2)", "[0.00,442,1,234,567.89]"},
	} {
		if got := fmt.Sprint(queryStrings(t, db, tc.q)); got != tc.want {
			t.Errorf("%s: %s, want %s", tc.q, got, tc.want)
		}
	}
	if _, err := db.Exec("select 1/*!2*/"); err == nil {
		t.Error("select 1/*!2*/ (select 1 2) succeeded; MySQL gives a syntax error")
	}
}

// SELECT DISTINCT ... ORDER BY an ordinal or a select expression sorts as
// MySQL 8.0.46 does (it failed, or did not sort; issue #17).
func TestDistinctOrderBy(t *testing.T) {
	c := startCluster(t)
	db := c.db(t)
	mustExec(t, db, "CREATE TABLE dob (a varchar(5), b varchar(5))")
	mustExec(t, db, "INSERT INTO dob VALUES ('a','b'),('a','b'),('c','d'),('e','b'),('a','z')")
	for _, tc := range []struct{ q, want string }{
		{"SELECT DISTINCT CONCAT(a,b) AS c FROM dob ORDER BY 1", "[ab az cd eb]"},
		{"SELECT DISTINCT CONCAT(a,b) FROM dob ORDER BY 1 DESC", "[eb cd az ab]"},
		{"SELECT DISTINCT a AS x FROM dob ORDER BY 1", "[a c e]"},
		{"SELECT DISTINCT a, COUNT(*) AS n FROM dob GROUP BY a ORDER BY 2, 1", "[c,1 e,1 a,3]"},
		{"SELECT DISTINCT COUNT(*) AS n FROM dob GROUP BY a ORDER BY 1", "[1 3]"},
		{"SELECT DISTINCT UPPER(b) AS u FROM dob ORDER BY 1 DESC LIMIT 2", "[Z D]"},
		{"SELECT DISTINCT CONCAT(a,'-') AS c, b FROM dob ORDER BY 1, 2", "[a-,b a-,z c-,d e-,b]"},
		{"SELECT DISTINCT CONCAT(a,b) AS c FROM dob ORDER BY c", "[ab az cd eb]"},
	} {
		if got := fmt.Sprint(queryStrings(t, db, tc.q)); got != tc.want {
			t.Errorf("%s: %s, want %s", tc.q, got, tc.want)
		}
	}
}
