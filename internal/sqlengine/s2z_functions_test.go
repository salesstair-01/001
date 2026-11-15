package sqlengine

import (
	"context"
	"testing"
)

// TestMySQLFunctions pins against MySQL 8.0.46 temporal and numeric
// functions, GROUP_CONCAT, the order of groups, window sums of DECIMAL, and
// DISTINCT and UNION under a case- and accent-insensitive collation.
func TestMySQLFunctions(t *testing.T) {
	c := startCluster(t)
	conn, err := c.db(t).Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, s := range []struct{ q, want string }{
		{"SELECT CONCAT(FROM_DAYS(TO_DAYS('960101'))), TO_DAYS(960201)-TO_DAYS('19960101'), CONCAT(DATE('010203')), TO_DAYS('123')", "[1996-01-01,31,2001-02-03,NULL]"},
		{"SELECT TO_SECONDS('2009-11-29'), TO_SECONDS(950501), TO_SECONDS('2009-11-29 13:43:32')", "[63426672000,62966505600,63426721412]"},
		{"SELECT TIME_FORMAT('100:00:00', '%H %k %h %I %l'), TIME_FORMAT('-10:00:00.5', '%H:%i:%s.%f %p'), TIME_FORMAT('10:11:12','%Y %y %c'), TIME_FORMAT('10:11:12','%M')", "[100 100 04 04 4,-10:00:00.500000 AM,0000 00 0,NULL]"},
		{"SELECT CONCAT(TIME('2001-01-01 10:00:00.5')), CONCAT(CONVERT_TZ('2004-01-01 12:00:00','+00:00','+10:00'))", "[10:00:00.5,2004-01-01 22:00:00]"},
		{"SELECT CONCAT(STR_TO_DATE('2003-01-02 10:11:12.0012', '%Y-%m-%d %H:%i:%S.%f')), STR_TO_DATE('2001-02-30','%Y-%m-%d'), STR_TO_DATE('2001', '%Y')", "[2003-01-02 10:11:12.001200,NULL,NULL]"},
		{"SELECT INTERVAL(23, 1, 15, 17, 30, 44, 200), INTERVAL(NULL, 1, 2), INTERVAL(3, NULL, 2, 4), INTERVAL(1.5, 1, 2)", "[3,-1,2,1]"},
		{"SELECT CEILING(99999999999999999999), FLOOR(-999999999999999999999.999), CEILING(-1.5), ROUND(15000111000111000155,-1), ROUND(18446744073709551614, -1), ROUND(1.5, 2147483640)", "[99999999999999999999,-1000000000000000000000,-1,15000111000111000160,18446744073709551610,1.5]"},
		{"CREATE TABLE fg (a int, b varchar(5), c double, d decimal(5,2))", "[]"},
		{"INSERT INTO fg VALUES (3,'c',3.5,1.25),(1,'a',1.5,NULL),(NULL,NULL,NULL,3.75),(2,'b',2.5,4),(1,'A',1.5,2)", "[]"},
		{"SELECT a, COUNT(*) FROM fg GROUP BY a", "[3,1 1,2 NULL,1 2,1]"},
		{"SELECT a, GROUP_CONCAT(b ORDER BY b) FROM fg WHERE b <> 'A' OR b IS NULL GROUP BY a", "[NULL,NULL 2,b 3,c]"},
		{"SELECT GROUP_CONCAT(a, b ORDER BY a, b), GROUP_CONCAT(DISTINCT a, 'z' ORDER BY a) FROM fg WHERE b <> 'A'", "[2b,3c,2z,3z]"},
		{"SELECT a, SUM(d) OVER (ORDER BY a, b ROWS UNBOUNDED PRECEDING), AVG(a) OVER () FROM fg ORDER BY a, b", "[NULL,3.75,1.7500 1,3.75,1.7500 1,5.75,1.7500 2,9.75,1.7500 3,11.00,1.7500]"},
		{"SELECT DISTINCT b FROM fg ORDER BY b", "[NULL a b c]"},
		{"SELECT COUNT(DISTINCT b), COUNT(DISTINCT UPPER(b)) FROM fg", "[3,3]"},
		{"SELECT b FROM fg UNION SELECT UPPER(b) FROM fg ORDER BY 1", "[NULL a b c]"},
		{"SELECT DISTINCT CONCAT(b, '') FROM fg ORDER BY 1", "[NULL a b c]"},
	} {
		if got := connRun(t, conn, s.q); got != s.want {
			t.Errorf("%s: got %s, want %s", s.q, got, s.want)
		}
	}
}
