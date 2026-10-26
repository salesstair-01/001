package sqlengine

import (
	"context"
	"database/sql"
	"strings"
	"testing"
)

// The deprecation warnings of CREATE and ALTER TABLE match MySQL 8.0.46's
// for the same statements (W display width, Z ZEROFILL, D float digits, U
// unsigned decimal or float).
func TestColumnTypeDeprecationWarnings(t *testing.T) {
	c := startCluster(t)
	conn, err := c.db(t).Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, tc := range []struct{ q, want string }{
		{"CREATE TABLE dw (a int(11), b tinyint(1), c tinyint(1) unsigned, d tinyint(2), e int(5) zerofill, f int zerofill, " +
			"g bigint(20) unsigned, h float(7,2), i double(10,3), j decimal(10,2) unsigned, k float unsigned, l float(10), m smallint, o bool, p serial)",
			"W W W W Z W Z W D D U U"},
		{"ALTER TABLE dw ADD COLUMN q mediumint(9), MODIFY a int(3)", "W W"},
		{"/* comment */ create table dw2 (a int)", ""},
		{"CREATE TABLE IF NOT EXISTS dw3 (a int(10) unsigned NOT NULL)", "W"},
		{"SELECT 1", ""},
	} {
		check(t, conn, tc.q, tc.want)
	}
}

// Notes and warnings about the tables and indexes a statement creates, as
// MySQL 8.0.46 gives them.
func TestTableAndIndexNotes(t *testing.T) {
	c := startCluster(t)
	conn, err := c.db(t).Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	dup := func(idx, table string) string {
		return "Warning,1831,Duplicate index '" + idx + "' defined on the table 'app." + table + "'. This is deprecated and will be disallowed in a future release."
	}
	for _, tc := range []struct{ q, want string }{
		{"CREATE TABLE d1 (a int, b int, unique(a), key(a))", ""},
		{"CREATE TABLE d2 (a int, b int, key k1(a,b), key k2(a))", ""},
		{"CREATE TABLE d3 (a int primary key, key(a))", ""},
		{"CREATE TABLE d4 (a varchar(10), key k1(a(3)), key k2(a(3)), key k3(a(4)))", dup("k2", "d4")},
		{"CREATE TABLE d5 (a int, b int, key k1(a,b), key k2(b,a), unique u1(a,b), unique u2(a,b))", dup("u2", "d5")},
		{"CREATE TABLE d6 (a int, key k1(a))", ""},
		{"CREATE INDEX k2 ON d6(a)", dup("k2", "d6")},
		{"ALTER TABLE d6 ADD KEY k3(a)", dup("k3", "d6")},
		{"CREATE TABLE d7 (a int, key(a), key(a))", dup("a_2", "d7")},
		{"CREATE TABLE IF NOT EXISTS d6 (x int)", "Note,1050,Table 'd6' already exists"},
	} {
		check(t, conn, tc.q, tc.want)
	}
}

// check runs q and compares its warnings with want, space-separated, with
// 1681 warnings abbreviated.
func check(t *testing.T, conn *sql.Conn, q, want string) {
	t.Helper()
	abbrev := map[string]string{warnDisplayWidth: "W", warnZerofill: "Z", warnFloatDigits: "D", warnUnsigned: "U"}
	mustExecConn(t, conn, q)
	var got []string
	for _, row := range connStrings(t, conn, "SHOW WARNINGS") {
		if f := strings.SplitN(row, ",", 3); len(f) == 3 && f[1] == "1681" {
			got = append(got, abbrev[f[2]])
		} else {
			got = append(got, row)
		}
	}
	if g := strings.Join(got, " "); g != want {
		t.Errorf("%s:\n got %s\nwant %s", q, g, want)
	}
}
