package sqlengine

import (
	"context"
	"net"
	"strconv"
	"testing"

	"github.com/dolthub/vitess/go/mysql"
)

// An OK packet's info string reaches the client (the vitess patch in
// third_party/vitess writes it as MySQL does; issue #15). A client that
// parses it, as the Rust mysql crate does, used to drop the connection on
// PREPARE.
func TestOKPacketInfo(t *testing.T) {
	c := startCluster(t)
	c.db(t) // wait until the server accepts connections
	conn := vitessConn(t, c.addr, "root", "")
	for _, q := range []string{"CREATE TABLE ok_info (a INT PRIMARY KEY)", "INSERT INTO ok_info VALUES (1), (2)", "SET @x = 11"} {
		if _, err := conn.ExecuteFetch(q, 0, false); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	for _, tc := range []struct{ q, info string }{
		{"UPDATE ok_info SET a = a + 10", "Rows matched: 2  Changed: 2  Warnings: 0"},
		{"PREPARE p FROM 'SELECT * FROM ok_info WHERE a > ?'", "Statement prepared"},
	} {
		res, err := conn.ExecuteFetch(tc.q, 0, false)
		if err != nil {
			t.Fatalf("%s: %v", tc.q, err)
		}
		if res.Info != tc.info {
			t.Errorf("%s: info %q, want %q", tc.q, res.Info, tc.info)
		}
	}
	res, err := conn.ExecuteFetch("EXECUTE p USING @x", 10, false)
	if err != nil || len(res.Rows) != 1 {
		t.Fatalf("EXECUTE: %v rows, %v", res, err)
	}
}

func vitessConn(t *testing.T, addr, user, pass string) *mysql.Conn {
	t.Helper()
	host, port, _ := net.SplitHostPort(addr)
	p, _ := strconv.Atoi(port)
	conn, err := mysql.Connect(context.Background(), &mysql.ConnParams{Host: host, Port: p, Uname: user, Pass: pass, DbName: "app"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(conn.Close)
	return conn
}
