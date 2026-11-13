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

// TestBinaryBitwise pins MySQL 8.0.46's bit operators on binary strings:
// bytewise &, |, ^, ~, << and >> (3513 for unequal lengths), two
// hexadecimal literals as numbers, a binary string next to an integer as a
// number by its text, and BIT_AND/OR/XOR of binary strings.
func TestBinaryBitwise(t *testing.T) {
	c := startCluster(t)
	conn, err := c.db(t).Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, s := range []struct{ q, want string }{
		{"SELECT HEX(0x0F | 0xF0), HEX(_binary 0x0F | _binary 0xF0), HEX(_binary 0x0F00 | _binary 0xF0F0)", "[FF,FF,FFF0]"},
		{"SELECT HEX(_binary 0x0F00 | _binary 0xF0)", "ERR 3513"},
		{"SELECT HEX(~ _binary 0x0F00), HEX(_binary 0x0F01 << 4), HEX(_binary 0x0F01 >> 4), BIT_COUNT(_binary 0xFFFF), HEX(_binary 0x0F01 << 20)", "[F0FF,F010,00F0,16,0000]"},
		{"SET @a = _binary 0x8001", "[]"},
		{"SELECT HEX(@a & 0xFF)", "ERR 3513"},
		{"SELECT ~0, ~5, HEX(0x0F00 | 0xF0)", "[18446744073709551615,18446744073709551610,FF0]"},
		{"CREATE TABLE bt (g int, v varbinary(4), b binary(2), i int)", "[]"},
		{"INSERT INTO bt VALUES (1, 0x0102, 0x0F0F, 3), (1, 0x0201, 0xF0F0, 5)", "[]"},
		{"SELECT g, HEX(BIT_OR(v)), HEX(BIT_AND(b)), HEX(BIT_XOR(v)), BIT_OR(i) FROM bt GROUP BY g", "[1,0303,0000,0303,7]"},
		{"SELECT HEX(v | b), HEX(v | i), HEX(v ^ 1), HEX(v & b) FROM bt", "[0F0F,3,1,0102 F2F1,5,1,0000]"},
		{"SELECT HEX(BIT_AND(v)), HEX(BIT_OR(v)), HEX(BIT_XOR(v)), BIT_AND(v) IS NULL FROM bt WHERE g = 9", "[FFFFFFFF,00000000,00000000,0]"},
		{"SELECT HEX(BIT_AND(b)), HEX(BIT_OR(b)) FROM bt WHERE g = 9", "[FFFF,0000]"},
		{"INSERT INTO bt VALUES (2, 0x01, NULL, 1), (2, NULL, NULL, 2)", "[]"},
		{"SELECT g, HEX(BIT_OR(v)), HEX(BIT_AND(b)) FROM bt GROUP BY g", "[1,0303,0000 2,01,FFFF]"},
		{"INSERT INTO bt VALUES (2, 0x0102, NULL, 1)", "[]"},
		{"SELECT g, HEX(BIT_OR(v)) FROM bt GROUP BY g", "ERR 3513"},
		{"SELECT HEX('ab' | 'cd'), 'ab' | 1", "[0,1]"},
	} {
		if got := connRun(t, conn, s.q); got != s.want {
			t.Errorf("%s: got %s, want %s", s.q, got, s.want)
		}
	}
}
