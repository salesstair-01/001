package sqlengine

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/salesstair-01/001/internal/kv"
	"github.com/salesstair-01/001/internal/kvrpc"
)

// engineOn starts a query engine for another partition of the cluster's KV node.
func (c *cluster) engineOn(t *testing.T, partition uint64) *sql.DB {
	cc, err := grpc.NewClient(c.kvAddr, append(kvrpc.DialOptions(), grpc.WithTransportCredentials(insecure.NewCredentials()))...)
	if err != nil {
		t.Fatal(err)
	}
	st := NewStore(kvrpc.NewClient(cc), partition)
	eng := NewEngine(st)
	lis, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := lis.Addr().String()
	lis.Close()
	srv, err := eng.NewServer(addr)
	if err != nil {
		t.Fatal(err)
	}
	go srv.Start()
	t.Cleanup(func() { srv.Close(); st.Close(); cc.Close() })
	db, _ := sql.Open("mysql", fmt.Sprintf("root@tcp(%s)/?parseTime=true", addr))
	t.Cleanup(func() { db.Close() })
	for i := 0; i < 50 && db.Ping() != nil; i++ {
		time.Sleep(20 * time.Millisecond)
	}
	return db
}

func TestPointInTimeRestoreSQL(t *testing.T) {
	c := startCluster(t)
	db := c.db(t)
	mustExec(t, db, "CREATE TABLE t (id INT AUTO_INCREMENT PRIMARY KEY, v VARCHAR(10), UNIQUE KEY uv (v))")
	mustExec(t, db, "INSERT INTO t (v) VALUES ('a'),('b'),('c')")
	time.Sleep(5 * time.Millisecond)
	at := time.Now()
	time.Sleep(5 * time.Millisecond)
	mustExec(t, db, "DELETE FROM t WHERE v <> 'a'") // the mistake to undo
	mustExec(t, db, "DROP TABLE t")

	if _, err := kv.Restore(context.Background(), c.store, 1, 2, at, t.TempDir()); err != nil {
		t.Fatal(err)
	}
	r := c.engineOn(t, 2)
	if got := strings.Join(queryStrings(t, r, "SELECT id, v FROM app.t ORDER BY id"), ";"); got != "1,a;2,b;3,c" {
		t.Fatalf("restored rows: %q", got)
	}
	if got := strings.Join(queryStrings(t, r, "SELECT id FROM app.t WHERE v = 'b'"), ";"); got != "2" {
		t.Fatalf("restored index: %q", got)
	}
	mustExec(t, r, "INSERT INTO app.t (v) VALUES ('d')")
	if got := strings.Join(queryStrings(t, r, "SELECT id FROM app.t WHERE v = 'd'"), ";"); got != "4" {
		t.Fatalf("AUTO_INCREMENT after restore: %q", got)
	}
	if _, err := r.Exec("INSERT INTO app.t (v) VALUES ('a')"); mysqlCode(err) != 1062 {
		t.Fatalf("unique index after restore: %v", err)
	}
	// The source is untouched.
	if _, err := db.Exec("SELECT * FROM t"); err == nil {
		t.Fatal("the dropped table came back in the source")
	}
}
