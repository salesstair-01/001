package sqlengine

import (
	"context"
	"testing"
)

// Functions go-mysql-server lacks (issue #26), against MySQL 8.0.46's
// results.
func TestExtraFunctions(t *testing.T) {
	c := startCluster(t)
	conn, err := c.db(t).Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, tc := range []struct{ q, want string }{
		{"SELECT TRUNCATE(1.999,1), TRUNCATE(1.999,3), TRUNCATE(122,-2), TRUNCATE(1.5e0,0), TRUNCATE(10.28*100,0), TRUNCATE(-1.999,1), TRUNCATE(1.223e0,1), TRUNCATE(NULL,1), TRUNCATE(18446744073709551615,-1), TRUNCATE('1.29',1), TRUNCATE(-0.5,0)",
			"[1.9,1.999,100,1,1028,-1.9,1.2,NULL,18446744073709551610,1.2,0]"},
		{"SELECT INSERT('Quadratic', 3, 4, 'What'), INSERT('Quadratic', -1, 4, 'What'), INSERT('Quadratic', 3, 100, 'What'), INSERT('abc',4,1,'x'), INSERT('abc',3,0,'x'), INSERT('héllo',2,1,'E'), INSERT(NULL,1,1,'x')",
			"[QuWhattic,Quadratic,QuWhat,abc,abxc,hEllo,NULL]"},
		{"SELECT EXPORT_SET(5,'Y','N','|',4), EXPORT_SET(6,'1','0','',10), EXPORT_SET(5,'Y','N','',3), LENGTH(EXPORT_SET(5,'Y','N'))",
			"[Y|N|Y|N,0110000000,YNY,127]"},
		{"SELECT MAKE_SET(1,'a','b','c'), MAKE_SET(1 | 4,'hello','nice','world'), MAKE_SET(1 | 4,'hello','nice',NULL,'world'), MAKE_SET(0,'a','b','c'), MAKE_SET(NULL,'a')",
			"[a,hello,world,hello,,NULL]"},
		{"SELECT HEX(AES_ENCRYPT('text','key')), AES_DECRYPT(AES_ENCRYPT('text','key'),'key'), HEX(AES_ENCRYPT('','k')), AES_DECRYPT('garbage','key'), HEX(AES_ENCRYPT('a much longer text value','a very long key that is longer than sixteen bytes'))",
			"[15E36637363712FC2E699B9C95B75393,text,932F1B1E9189536B938A6B67666D4031,NULL,2E5E8DCA855D4D1EC88D907483EA86C67EF7D2F01401FE01713B27145B669489]"},
		{"SET block_encryption_mode = 'aes-256-cbc'", "[]"},
		{"SELECT HEX(AES_ENCRYPT('text','key','1234567890123456')), AES_DECRYPT(AES_ENCRYPT('text','key','1234567890123456'),'key','1234567890123456')",
			"[53C2A3389ACA49DC6D54A64FF0D653E4,text]"},
		{"SELECT AES_ENCRYPT('text','key')", "ERR 1582"},
		{"SET block_encryption_mode = 'aes-128-cfb128'", "[]"},
		{"SELECT HEX(AES_ENCRYPT('text','key','1234567890123456'))", "[068817FE]"},
		{"SET block_encryption_mode = 'aes-192-ofb'", "[]"},
		{"SELECT HEX(AES_ENCRYPT('text','key','1234567890123456'))", "[0D1C7401]"},
		{"SELECT UUID_SHORT() < UUID_SHORT(), BENCHMARK(10, 1+1)", "[1,0]"},
		{"SELECT ~5, ~0, ~-1, ~'3', ~1.6, ~NULL, ~x'0F', ~b'1'",
			"[18446744073709551610,18446744073709551615,0,18446744073709551612,18446744073709551613,NULL,18446744073709551600,18446744073709551614]"},
		{"SELECT 18446744073709551615 & 255, -1 | 0, 1 << 64, 1 << 63, -8 >> 1, '3' ^ 1, 1.5 & 3, 2.5 | 0, -2.5 | 0, x'0F' | 0, HEX(x'0F' | x'F0'), '1e3' | 0",
			"[255,18446744073709551615,0,9223372036854775808,9223372036854775804,2,2,3,18446744073709551613,15,FF,1]"},
	} {
		if got := connRun(t, conn, tc.q); got != tc.want {
			t.Errorf("%s:\n got %s\nwant %s", tc.q, got, tc.want)
		}
	}
}
