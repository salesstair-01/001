package sqlengine

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// BIGINT UNSIGNED arithmetic and integer casts give MySQL 8.0.46's results
// and warnings (issue #16).
func TestUnsignedArithmeticAndCasts(t *testing.T) {
	c := startCluster(t)
	conn, err := c.db(t).Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, q := range []string{
		"CREATE TABLE u (id bigint unsigned primary key, v bigint unsigned, key kv(v))",
		"INSERT INTO u VALUES (18446744073709551615, 18446744073709551615), (9223372036854775808, 1), (1, 9223372036854775809)",
	} {
		mustExecConn(t, conn, q)
	}
	for _, tc := range []struct{ q, want, warnings string }{
		{"SELECT id, v FROM u ORDER BY id", "[1,9223372036854775809 9223372036854775808,1 18446744073709551615,18446744073709551615]", ""},
		{"SELECT id FROM u WHERE v > 9223372036854775807 ORDER BY id", "[1 18446744073709551615]", ""},
		{"SELECT 9223372036854775808 + 1, CAST(9223372036854775808 AS UNSIGNED) + 1", "[9223372036854775809,9223372036854775809]", ""},
		{"SELECT 18446744073709551615 + 0, 18446744073709551615 - 1, 18446744073709551615 * 1", "[18446744073709551615,18446744073709551614,18446744073709551615]", ""},
		{"SELECT id + 1 FROM u WHERE id = 9223372036854775808", "[9223372036854775809]", ""},
		{"SELECT -9223372036854775807 - 1, 3 - 5, 7 * -6", "[-9223372036854775808,-2,-42]", ""},
		{"SELECT CAST(-1 AS UNSIGNED), CAST(18446744073709551615 AS SIGNED), CAST(9223372036854775808 AS SIGNED)", "[18446744073709551615,-1,-9223372036854775808]", ""},
		{"SELECT CAST('18446744073709551616' AS UNSIGNED), CAST('99999999999999999999' AS SIGNED), CAST('1e30' AS UNSIGNED)", "[18446744073709551615,-1,1]",
			"1292 1292 1292"},
		{"SELECT CAST('12abc' AS SIGNED), CAST(' 12 ' AS SIGNED), CAST('' AS SIGNED), CAST('-0' AS UNSIGNED), CAST('  -5x' AS UNSIGNED), CAST('-9223372036854775809' AS SIGNED)",
			"[12,12,0,0,18446744073709551611,-9223372036854775808]", "1292 1292 1105 1292 1292"},
	} {
		if got := fmt.Sprint(connStrings(t, conn, tc.q)); got != tc.want {
			t.Errorf("%s:\n got %s\nwant %s", tc.q, got, tc.want)
		}
		var codes []string
		for _, w := range connStrings(t, conn, "SHOW WARNINGS") {
			codes = append(codes, strings.SplitN(w, ",", 3)[1])
		}
		if got := strings.Join(codes, " "); got != tc.warnings {
			t.Errorf("%s: warnings %s, want %s", tc.q, got, tc.warnings)
		}
	}
	for _, q := range []string{
		"SELECT 18446744073709551615 + 1", "SELECT CAST(1 AS UNSIGNED) - 2", "SELECT 9223372036854775807 + 1",
		"SELECT 4294967296 * 4294967295", "SELECT id * 2 FROM u WHERE id = 18446744073709551615",
	} {
		rows, err := conn.QueryContext(context.Background(), q)
		if err == nil {
			for rows.Next() {
			}
			err = rows.Err()
			rows.Close()
		}
		if err == nil || !strings.Contains(err.Error(), "1690") {
			t.Errorf("%s: %v, want error 1690", q, err)
		}
	}
}
