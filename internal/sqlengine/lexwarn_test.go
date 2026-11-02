package sqlengine

import (
	"context"
	"strings"
	"testing"
)

// TestLexWarnings: MySQL 8.0.46's parse-time deprecation warnings.
func TestLexWarnings(t *testing.T) {
	c := startCluster(t)
	conn, err := c.db(t).Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	codes := func(q string) string {
		if got := connRun(t, conn, q); strings.HasPrefix(got, "ERR") {
			return got
		}
		rows, err := conn.QueryContext(context.Background(), "SHOW WARNINGS")
		if err != nil {
			return "ERR " + err.Error()
		}
		defer rows.Close()
		var out []string
		for rows.Next() {
			var level, code, msg string
			_ = rows.Scan(&level, &code, &msg)
			out = append(out, code)
		}
		return strings.Join(out, " ")
	}
	for _, s := range []struct{ q, want string }{
		{"CREATE TABLE u1 (a varchar(5) CHARACTER SET utf8)", "3719"},
		{"CREATE TABLE u2 (a varchar(5) CHARSET utf8mb3)", "1287"},
		{"CREATE TABLE u3 (a varchar(5) COLLATE utf8_bin)", "3778"},
		{"CREATE TABLE u4 (a varchar(5)) CHARSET=utf8 COLLATE utf8_general_ci", "3719 3778"},
		{"CREATE TABLE u5 (a varbinary(5), b binary(3))", ""},
		{"SELECT CONVERT('a' USING utf8)", "3719"},
		{"SELECT _utf8'a'", "3719"},
		{"SELECT 'a' = BINARY 'A', BINARY 'x' LIKE 'X'", "1287 1287"},
		{"SELECT CAST('a' AS BINARY), CONVERT('a', BINARY)", ""},
		{"INSERT INTO u1 VALUES (BINARY 'x')", "1287"},
		{"SELECT 'utf8', 'binary', `utf8mb4`.a FROM u1 AS utf8mb4", ""},
		{"SELECT @x := 1, @y := 2", "1287 1287"},
		{"SET @z := (@w := 5)", "1287"},
		{"SET @a = 1, @b := 2", ""},
		{"CREATE FUNCTION lw1() RETURNS int BEGIN DECLARE res int; SET res := 1; SET res = 2, res := 3; RETURN res; END", ""},
		{"CREATE FUNCTION lw2() RETURNS int BEGIN DECLARE res int; SELECT @x := 1 INTO res; RETURN res; END", "1287"},
		{"SET NAMES binary", ""},
		{"SET NAMES utf8mb4", ""},
		{"SELECT CAST(19999999999999999999 AS SIGNED), CAST('abcdef' AS CHAR(3)), CAST(-1 AS UNSIGNED), CAST('ab' AS CHAR(5))", "1292 1292"},
		{"SELECT SQL_CALC_FOUND_ROWS 1", "1287"},
		{"SELECT FOUND_ROWS()", "1287"},
		{"SET SQL_MODE='NO_ZERO_DATE'", "3135"},
		{"SET SQL_MODE='STRICT_TRANS_TABLES'", "3135"},
		{"SET SQL_MODE=''", ""},
		{"SET SQL_MODE='TRADITIONAL'", ""},
		{"SET SQL_MODE='ERROR_FOR_DIVISION_BY_ZERO,NO_ZERO_IN_DATE'", "3135"},
		{"SET SQL_MODE=DEFAULT", ""},
		// duplicate keys skipped by INSERT IGNORE: MySQL's text
		{"CREATE TABLE dup (a int PRIMARY KEY)", ""},
		{"INSERT IGNORE INTO dup VALUES (1), (1)", "1062"},
	} {
		if got := codes(s.q); got != s.want {
			t.Errorf("%s: warnings %q, want %q", s.q, got, s.want)
		}
	}
}

// TestIgnoreWarnings pins the warnings INSERT and UPDATE give for values
// they adjust to a column, against MySQL 8.0.46.
func TestIgnoreWarnings(t *testing.T) {
	c := startCluster(t)
	conn, err := c.db(t).Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, s := range []struct{ q, want string }{
		{"CREATE TABLE w1 (a tinyint, b varchar(3), c enum('x','y'), d decimal(5,2), e int)", "[]"},
		{"INSERT IGNORE INTO w1 VALUES (300, 'abcdef', 'z', 1.234, 1)", "[]"},
		{"SHOW WARNINGS", "[Warning,1264,Out of range value for column 'a' at row 1 Warning,1265,Data truncated for column 'b' at row 1 Warning,1265,Data truncated for column 'c' at row 1 Note,1265,Data truncated for column 'd' at row 1]"},
		{"INSERT IGNORE INTO w1 (e) VALUES (1), ('abc')", "[]"},
		{"SHOW WARNINGS", "[Warning,1366,Incorrect integer value: 'abc' for column 'e' at row 2]"},
		{"INSERT INTO w1 (d) VALUES (10.0/3)", "[]"},
		{"SHOW WARNINGS", "[Note,1265,Data truncated for column 'd' at row 1]"},
		{"INSERT INTO w1 (d) VALUES (1.5)", "[]"},
		{"SHOW WARNINGS", "[]"},
		{"UPDATE IGNORE w1 SET b = 'toolong' WHERE e = 1", "[]"},
		{"SHOW WARNINGS", "[Warning,1265,Data truncated for column 'b' at row 1 Warning,1265,Data truncated for column 'b' at row 2]"},
		// without strict mode, as with IGNORE: a value a temporal column
		// can't hold is truncated (a string) or out of range (a number)
		{"CREATE TABLE w2 (a datetime(6), d date, y year, i int)", "[]"},
		{"INSERT INTO w2 VALUES ('x', 'x', 'x', 'x')", "ERR 1292"},
		{"SET SQL_MODE = 'NO_ENGINE_SUBSTITUTION'", "[]"},
		{"INSERT INTO w2 VALUES ('x', 'x', 'x', 'x')", "[]"},
		{"SHOW WARNINGS", "[Warning,1265,Data truncated for column 'a' at row 1 Warning,1265,Data truncated for column 'd' at row 1 Warning,1366,Incorrect integer value: 'x' for column 'y' at row 1 Warning,1366,Incorrect integer value: 'x' for column 'i' at row 1]"},
		{"INSERT INTO w2 VALUES (-1.5, -1, 3000, 99999999999)", "[]"},
		{"SHOW WARNINGS", "[Warning,1264,Out of range value for column 'a' at row 1 Warning,1264,Out of range value for column 'd' at row 1 Warning,1264,Out of range value for column 'y' at row 1 Warning,1264,Out of range value for column 'i' at row 1]"},
		{"UPDATE w2 SET d = 'zz'", "[]"},
		{"SHOW WARNINGS", "[Warning,1265,Data truncated for column 'd' at row 1 Warning,1265,Data truncated for column 'd' at row 2]"},
		{"SET SQL_MODE = DEFAULT", "[]"},
	} {
		if got := connRun(t, conn, s.q); got != s.want {
			t.Errorf("%s: got %s, want %s", s.q, got, s.want)
		}
	}
}
