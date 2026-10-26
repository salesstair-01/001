package sqlengine

import (
	"context"
	"testing"
)

// TestCheckConstraints pins issue #21 against MySQL 8.0's results for the
// same statements.
func TestCheckConstraints(t *testing.T) {
	c := startCluster(t)
	conn, err := c.db(t).Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, s := range []struct{ q, want string }{
		{"CREATE TABLE ck (a int CHECK (a > 0), b int, CONSTRAINT b_pos CHECK (b >= 0), CHECK (a < b))", "[]"},
		{"INSERT INTO ck VALUES (1, 2)", "[]"},
		{"INSERT INTO ck VALUES (0, 2)", "ERR 3819"},
		{"INSERT INTO ck VALUES (1, -1)", "ERR 3819"},
		{"INSERT INTO ck VALUES (3, 2)", "ERR 3819"},
		{"UPDATE ck SET a = 5", "ERR 3819"},
		{"INSERT INTO ck VALUES (NULL, NULL)", "[]"}, // unknown is not a violation
		{"SELECT constraint_name FROM information_schema.check_constraints ORDER BY constraint_name", "[b_pos ck_chk_1 ck_chk_2]"},
		{"ALTER TABLE ck ADD CONSTRAINT c3 CHECK (a <> 7)", "[]"},
		{"INSERT INTO ck VALUES (7, 9)", "ERR 3819"},
		{"ALTER TABLE ck DROP CHECK c3", "[]"},
		{"INSERT INTO ck VALUES (7, 9)", "[]"},
		{"ALTER TABLE ck DROP CHECK nosuch", "ERR 3821"},
		{"ALTER TABLE ck ADD CONSTRAINT b_pos CHECK (a <> 100)", "ERR 3822"},
		// names are unique per database; a failed CREATE leaves no table
		{"CREATE TABLE ck2 (x int, CONSTRAINT b_pos CHECK (x > 0))", "ERR 3822"},
		{"SELECT * FROM ck2", "ERR 1146"},
		{"CREATE TABLE ck2 (x int, CONSTRAINT d CHECK (x > 0), CONSTRAINT d CHECK (x < 9))", "ERR 3822"},
		{"SELECT * FROM ck2", "ERR 1146"},
		{"CREATE TABLE IF NOT EXISTS ck (a int, CONSTRAINT b_pos CHECK (a > 0))", "[]"},
		{"CREATE TABLE ck3 (x int, CHECK (x > 0) NOT ENFORCED)", "[]"},
		{"INSERT INTO ck3 VALUES (-1)", "[]"},
		{"DROP TABLE ck3", "[]"},
		{"CREATE TABLE ck4 (x int, CONSTRAINT ck3_chk_1 CHECK (x > 0))", "[]"},
		{"CREATE TEMPORARY TABLE tc (x int CHECK (x < 10))", "[]"},
		{"INSERT INTO tc VALUES (11)", "ERR 3819"},
		{"INSERT INTO tc VALUES (5)", "[]"},
	} {
		if got := connRun(t, conn, s.q); got != s.want {
			t.Errorf("%s: got %s, want %s", s.q, got, s.want)
		}
	}
}
