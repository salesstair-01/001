package sqlengine

import (
	"strings"
	"testing"
	"time"
)

func TestForeignKeys(t *testing.T) {
	c := startCluster(t)
	db := c.db(t)
	check := func(q, want string) {
		t.Helper()
		if got := strings.Join(queryStrings(t, db, q), ";"); got != want {
			t.Errorf("%s: got %q, want %q", q, got, want)
		}
	}
	code := func(q string) uint16 {
		t.Helper()
		_, err := db.Exec(q)
		return mysqlCode(err)
	}
	mustExec(t, db, "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(20))")
	mustExec(t, db, "CREATE TABLE posts (id INT PRIMARY KEY, author_id INT NOT NULL, FOREIGN KEY (author_id) REFERENCES users(id) ON DELETE CASCADE)")
	mustExec(t, db, "CREATE TABLE notes (id INT PRIMARY KEY, user_id INT, CONSTRAINT fk_notes_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL)")
	mustExec(t, db, "CREATE TABLE likes (post_id INT, CONSTRAINT fk_likes_post FOREIGN KEY (post_id) REFERENCES posts(id))") // keyless child, RESTRICT
	mustExec(t, db, "INSERT INTO users VALUES (1, 'a'), (2, 'b')")
	mustExec(t, db, "INSERT INTO posts VALUES (10, 1), (11, 1), (12, 2)")
	mustExec(t, db, "INSERT INTO notes VALUES (100, 1), (101, 2)")
	mustExec(t, db, "INSERT INTO likes VALUES (12)")

	if got := code("INSERT INTO posts VALUES (13, 99)"); got != 1452 {
		t.Errorf("orphan insert: code %d, want 1452", got)
	}
	if got := code("UPDATE posts SET author_id = 99 WHERE id = 10"); got != 1452 {
		t.Errorf("orphan update: code %d, want 1452", got)
	}
	if got := code("DELETE FROM posts WHERE id = 12"); got != 1451 {
		t.Errorf("restricted delete: code %d, want 1451", got)
	}
	mustExec(t, db, "DELETE FROM users WHERE id = 1")
	check("SELECT id FROM posts ORDER BY id", "12")                      // cascaded
	check("SELECT id, user_id FROM notes ORDER BY id", "100,NULL;101,2") // set null
	if got := code("DELETE FROM users WHERE id = 2"); got != 1451 {
		t.Errorf("cascade into a restricted child: code %d, want 1451", got) // posts 12 has a like
	}
	check("SELECT COUNT(*) FROM users", "1")

	// Self-reference, parent and child in one statement.
	mustExec(t, db, "CREATE TABLE tree (id INT PRIMARY KEY, parent INT, FOREIGN KEY (parent) REFERENCES tree(id))")
	mustExec(t, db, "INSERT INTO tree VALUES (1, NULL), (2, 1), (3, 2)")
	if got := code("INSERT INTO tree VALUES (4, 42)"); got != 1452 {
		t.Errorf("self-reference orphan: code %d", got)
	}

	// Declared keys survive a restart and show in SHOW CREATE TABLE.
	c.restartKV()
	show := strings.Join(queryStrings(t, db, "SHOW CREATE TABLE notes"), " ")
	if !strings.Contains(show, "CONSTRAINT `fk_notes_user` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`) ON DELETE SET NULL") {
		t.Errorf("SHOW CREATE TABLE: %s", show)
	}
	if got := code("INSERT INTO notes VALUES (102, 77)"); got != 1452 {
		t.Errorf("after restart: code %d", got)
	}
	mustExec(t, db, "ALTER TABLE notes DROP FOREIGN KEY fk_notes_user")
	mustExec(t, db, "INSERT INTO notes VALUES (102, 77)")
	if got := code("ALTER TABLE notes ADD CONSTRAINT fk2 FOREIGN KEY (user_id) REFERENCES users(id)"); got != 1452 {
		t.Errorf("adding a key over an orphan row: code %d, want 1452", got)
	}
	mustExec(t, db, "DELETE FROM notes WHERE id = 102")
	mustExec(t, db, "ALTER TABLE notes ADD CONSTRAINT fk2 FOREIGN KEY (user_id) REFERENCES users(id)")
}

// A child insert holds a shared lock on its parent row until it ends, so a
// concurrent delete of the parent waits, then sees the child.
func TestForeignKeyCheckLocksParent(t *testing.T) {
	c := startCluster(t)
	db := c.db(t)
	mustExec(t, db, "CREATE TABLE p (id INT PRIMARY KEY)")
	mustExec(t, db, "CREATE TABLE ch (id INT PRIMARY KEY, pid INT, FOREIGN KEY (pid) REFERENCES p(id))")
	mustExec(t, db, "INSERT INTO p VALUES (1)")
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, tx, "INSERT INTO ch VALUES (1, 1)")
	done := make(chan error, 1)
	go func() {
		_, err := db.Exec("DELETE FROM p WHERE id = 1")
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("parent delete did not wait for the child's transaction: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; mysqlCode(err) != 1451 {
		t.Fatalf("parent delete after the child committed: %v, want 1451", err)
	}
}
