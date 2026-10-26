package proxy

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/dolthub/vitess/go/mysql"
)

// The proxy passes the query engine's OK-packet info and warning count on
// to the client (issue #15).
func TestInfoAndWarningsPassThrough(t *testing.T) {
	s := newStack(t, stackOpts{backendIdle: time.Minute, engineIdle: time.Minute})
	setup := s.db(t, "app@acme", "")
	mustExec(t, setup, "CREATE DATABASE app")
	mustExec(t, setup, "CREATE TABLE app.t (a INT PRIMARY KEY)")
	mustExec(t, setup, "INSERT INTO app.t VALUES (1), (2)")

	host, port, _ := net.SplitHostPort(s.proxy.Addr())
	p, _ := strconv.Atoi(port)
	conn, err := mysql.Connect(context.Background(), &mysql.ConnParams{Host: host, Port: p, Uname: "app@acme", Pass: "s3cret", DbName: "app"})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	res, err := conn.ExecuteFetch("UPDATE t SET a = a + 10", 0, false)
	if err != nil {
		t.Fatal(err)
	}
	if want := "Rows matched: 2  Changed: 2  Warnings: 0"; res.Info != want {
		t.Errorf("UPDATE info %q, want %q", res.Info, want)
	}
	res, err = conn.ExecuteFetch("PREPARE p FROM 'SELECT a FROM t WHERE a > ?'", 0, false)
	if err != nil || res.Info != "Statement prepared" {
		t.Fatalf("PREPARE: info %q, %v", res.Info, err)
	}

	_, warnings, err := conn.ExecuteFetchWithWarningCount("SELECT 1/0", 1, false)
	if err != nil || warnings != 1 {
		t.Fatalf("SELECT 1/0: %d warnings, %v", warnings, err)
	}
	_, warnings, err = conn.ExecuteFetchWithWarningCount("SELECT 1", 1, false)
	if err != nil || warnings != 0 {
		t.Fatalf("SELECT 1 after a warning: %d warnings, %v", warnings, err)
	}
}
