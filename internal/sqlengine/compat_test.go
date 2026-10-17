package sqlengine

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

// Fixes found by the MySQL differential tests (mysqldiff_test.go).
func TestMySQLCompatFixes(t *testing.T) {
	c := startCluster(t)
	db := c.db(t)
	check := func(q, want string) {
		t.Helper()
		if got := strings.Join(queryStrings(t, db, q), ";"); got != want {
			t.Errorf("%s: got %q, want %q", q, got, want)
		}
	}
	// Columns without a collation get MySQL 8's default, utf8mb4_0900_ai_ci.
	mustExec(t, db, "CREATE DATABASE d2")
	mustExec(t, db, "CREATE TABLE d2.t (id INT PRIMARY KEY, s VARCHAR(20), KEY ks (s))")
	mustExec(t, db, "INSERT INTO d2.t VALUES (1, 'abc'), (2, 'ab😀'), (3, '9中'), (4, 'b')")
	check("SELECT COLLATION(s) FROM d2.t WHERE id = 1", "utf8mb4_0900_ai_ci")
	check("SELECT id FROM d2.t WHERE s = 'ABC'", "1")
	// A prefix LIKE keeps strings that continue past U+00FF.
	check("SELECT id FROM d2.t WHERE s LIKE 'ab%' ORDER BY id", "1;2")
	check("SELECT id FROM d2.t WHERE s LIKE '9%'", "3")
	// LIKE on a utf8mb4_0900_bin column (stored without its collation once).
	mustExec(t, db, "CREATE TABLE b (id INT PRIMARY KEY, s VARCHAR(20) COLLATE utf8mb4_0900_bin)")
	mustExec(t, db, "INSERT INTO b VALUES (1, 'abc'), (2, 'ABC')")
	check("SELECT id FROM b WHERE s LIKE 'ab%'", "1")
	check("SELECT id FROM b WHERE s COLLATE utf8mb4_0900_bin = 'abc'", "1")
	// CHAR drops trailing spaces on store.
	mustExec(t, db, "CREATE TABLE ch (c CHAR(4) PRIMARY KEY)")
	mustExec(t, db, "INSERT INTO ch VALUES ('a  ')")
	check("SELECT CONCAT('[', c, ']') FROM ch", "[a]")
	if _, err := db.Exec("INSERT INTO ch VALUES ('a')"); mysqlCode(err) != 1062 {
		t.Errorf("'a' after 'a  ' in a CHAR key: %v", err)
	}
}

// handshakeStatus reads the status flags of a MySQL server's initial
// handshake (protocol v10).
func handshakeStatus(t *testing.T, addr string) uint16 {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	hdr := make([]byte, 4)
	if _, err := io.ReadFull(conn, hdr); err != nil {
		t.Fatal(err)
	}
	body := make([]byte, int(hdr[0])|int(hdr[1])<<8|int(hdr[2])<<16)
	if _, err := io.ReadFull(conn, body); err != nil {
		t.Fatal(err)
	}
	// protocol(1) version(NUL-terminated) connection id(4) auth data(8)
	// filler(1) capabilities(2) charset(1) status(2)
	i := 1 + bytes.IndexByte(body[1:], 0) + 1 + 4 + 8 + 1 + 2 + 1
	return binary.LittleEndian.Uint16(body[i:])
}

// Drivers such as PyMySQL send SET autocommit = 0 only when the handshake
// says autocommit is on; without the flag their transactions autocommitted.
func TestHandshakeAnnouncesAutocommit(t *testing.T) {
	c := startCluster(t)
	c.db(t)
	if st := handshakeStatus(t, c.addr); st&0x0002 == 0 {
		t.Fatalf("handshake status %#x lacks SERVER_STATUS_AUTOCOMMIT", st)
	}
}
