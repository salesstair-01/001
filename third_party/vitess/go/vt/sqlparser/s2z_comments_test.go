package sqlparser

import "testing"

// s2z patch: fewer than 5 digits are not a version are SQL, as in
// MySQL, and no longer panic.
func TestMySQLCommentShortVersion(t *testing.T) {
	if _, err := Parse("select 1/*!2*/"); err == nil {
		t.Fatal("select 1/*!2*/ parsed; MySQL reads it as select 1 2")
	}
	for q, want := range map[string]string{
		"select 1 + /*!2*/":       "select 1 + 2",
		"select 1 /*!50000 + 1*/": "select 1 + 1",
	} {
		st, err := Parse(q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		if got := String(st); got != want {
			t.Errorf("%s: %s, want %s", q, got, want)
		}
	}
}
