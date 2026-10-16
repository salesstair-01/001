package sqlengine

import (
	"fmt"
	"strings"
	"testing"
)

func TestKeylessTables(t *testing.T) {
	c := startCluster(t)
	db := c.db(t)
	mustExec(t, db, "CREATE TABLE k (a INT, b VARCHAR(10), KEY ib (b))")
	mustExec(t, db, "INSERT INTO k VALUES (1,'x'),(1,'x'),(1,'x'),(2,'y'),(NULL,NULL)")
	check := func(q, want string) {
		t.Helper()
		if got := strings.Join(queryStrings(t, db, q), ";"); got != want {
			t.Fatalf("%s: got %q, want %q", q, got, want)
		}
	}
	check("SELECT a,b FROM k ORDER BY a,b", "NULL,NULL;1,x;1,x;1,x;2,y")

	// Duplicates are separate rows: LIMIT deletes one of them.
	mustExec(t, db, "DELETE FROM k WHERE a = 1 LIMIT 1")
	check("SELECT count(*) FROM k WHERE a = 1", "2")
	mustExec(t, db, "UPDATE k SET b = 'z' WHERE a = 1 LIMIT 1")
	check("SELECT a,b FROM k WHERE a = 1 ORDER BY b", "1,x;1,z")
	check("SELECT a FROM k WHERE b = 'z'", "1") // the index entry moved

	mustExec(t, db, "UPDATE k SET a = a + 10")
	check("SELECT a,b FROM k ORDER BY a,b", "NULL,NULL;11,x;11,z;12,y")
	mustExec(t, db, "DELETE FROM k WHERE b = 'x'")
	check("SELECT a,b FROM k ORDER BY a,b", "NULL,NULL;11,z;12,y")

	// Transactions and rollback.
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, tx, "INSERT INTO k VALUES (5,'q')")
	mustExec(t, tx, "DELETE FROM k WHERE a = 12")
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	check("SELECT a,b FROM k ORDER BY a,b", "NULL,NULL;11,z;12,y")

	// A unique secondary index still enforces uniqueness.
	mustExec(t, db, "CREATE TABLE u (a INT, UNIQUE KEY ua (a))")
	mustExec(t, db, "INSERT INTO u VALUES (1),(2)")
	if _, err := db.Exec("INSERT INTO u VALUES (1)"); mysqlCode(err) != 1062 {
		t.Fatalf("duplicate in unique index: %v", err)
	}
	mustExec(t, db, "REPLACE INTO u VALUES (2)")
	check("SELECT a FROM u ORDER BY a", "1;2")

	// Many rows, through a fresh query engine session.
	mustExec(t, db, "CREATE TABLE big (v INT)")
	var vals []string
	for i := 0; i < 1000; i++ {
		vals = append(vals, fmt.Sprintf("(%d)", i%10))
	}
	mustExec(t, db, "INSERT INTO big VALUES "+strings.Join(vals, ","))
	mustExec(t, db, "DELETE FROM big WHERE v < 5")
	check("SELECT count(*), sum(v) FROM big", "500,3500")
}

// Locking reads through a non-unique index once lost every row: the empty
// entry value read back as nil, i.e. absent.
func TestLockingReadThroughSecondaryIndex(t *testing.T) {
	c := startCluster(t)
	db := c.db(t)
	mustExec(t, db, "CREATE TABLE p (id INT PRIMARY KEY, b VARCHAR(10), KEY ib (b))")
	mustExec(t, db, "INSERT INTO p VALUES (1,'x'),(2,'y')")
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(queryStrings(t, tx, "SELECT id FROM p WHERE b = 'x' FOR UPDATE"), ";"); got != "1" {
		t.Fatalf("FOR UPDATE through index: %q", got)
	}
	_ = tx.Rollback()
	r := mustExec(t, db, "DELETE FROM p WHERE b = 'x'")
	if n, _ := r.RowsAffected(); n != 1 {
		t.Fatalf("DELETE through index affected %d rows", n)
	}
}
