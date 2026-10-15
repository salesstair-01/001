package sqlengine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	gomysql "github.com/go-sql-driver/mysql"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/salesstair-01/001/internal/kv"
	"github.com/salesstair-01/001/internal/kvrpc"
	"github.com/salesstair-01/001/internal/objstore"
)

// cluster runs a KV node and a query engine in-process.
type cluster struct {
	t      *testing.T
	store  objstore.Store
	node   *kv.Node
	grpc   *grpc.Server
	kvAddr string
	engine *Engine
	addr   string
}

func startKV(t *testing.T, store objstore.Store, owner, addr string) (*kv.Node, *grpc.Server, string) {
	t.Helper()
	node := kv.NewNode(kv.Config{
		Store: store, DataDir: t.TempDir(), OwnerID: owner,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	var lis net.Listener
	var err error
	for i := 0; i < 100; i++ { // the old listener's port may take a moment to free
		if lis, err = net.Listen("tcp", addr); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	g := grpc.NewServer(kvrpc.ServerOption())
	kvrpc.Register(g, &kvrpc.Server{Node: node, AutoOpen: true})
	go g.Serve(lis)
	return node, g, lis.Addr().String()
}

func startCluster(t *testing.T) *cluster {
	t.Helper()
	c := &cluster{t: t, store: objstore.NewMem()}
	c.node, c.grpc, c.kvAddr = startKV(t, c.store, "kv-a", "127.0.0.1:0")
	cc, err := grpc.NewClient(c.kvAddr, append(kvrpc.DialOptions(), grpc.WithTransportCredentials(insecure.NewCredentials()))...)
	if err != nil {
		t.Fatal(err)
	}
	st := NewStore(kvrpc.NewClient(cc), 1)
	c.engine = NewEngine(st)
	if err := c.engine.EnsureDatabase(context.Background(), "app"); err != nil {
		t.Fatal(err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	c.addr = lis.Addr().String()
	lis.Close()
	srv, err := c.engine.NewServer(c.addr)
	if err != nil {
		t.Fatal(err)
	}
	go srv.Start()
	t.Cleanup(func() {
		srv.Close()
		st.Close()
		cc.Close()
		c.grpc.Stop()
		c.node.Close()
	})
	return c
}

// restartKV replaces the KV node with a fresh one on the same address over
// the same object store, as after kill -9 and a restart: the next request
// gets EpochChanged and the partition is taken over.
func (c *cluster) restartKV() {
	c.grpc.Stop()
	c.node.Close()
	c.node, c.grpc, _ = startKV(c.t, c.store, fmt.Sprint("kv-", time.Now().UnixNano()), c.kvAddr)
}

func (c *cluster) db(t *testing.T) *sql.DB {
	db, err := sql.Open("mysql", fmt.Sprintf("root@tcp(%s)/app?parseTime=true", c.addr))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	for i := 0; i < 50; i++ {
		if err = db.Ping(); err == nil {
			return db
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal(err)
	return nil
}

func mustExec(t *testing.T, db interface {
	Exec(string, ...any) (sql.Result, error)
}, q string, args ...any) sql.Result {
	t.Helper()
	r, err := db.Exec(q, args...)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return r
}

func queryStrings(t *testing.T, db interface {
	Query(string, ...any) (*sql.Rows, error)
}, q string, args ...any) []string {
	t.Helper()
	rows, err := db.Query(q, args...)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	var out []string
	for rows.Next() {
		vals := make([]sql.NullString, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatal(err)
		}
		s := ""
		for i, v := range vals {
			if i > 0 {
				s += ","
			}
			if v.Valid {
				s += v.String
			} else {
				s += "NULL"
			}
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func mysqlCode(err error) uint16 {
	var me *gomysql.MySQLError
	if errors.As(err, &me) {
		return me.Number
	}
	return 0
}

// Slice 0 done-when: CREATE TABLE (INT key, utf8mb4_0900_bin VARCHAR),
// INSERT, SELECT by key and a full scan, through a separate KV process.
func TestWalkingSkeleton(t *testing.T) {
	c := startCluster(t)
	db := c.db(t)
	mustExec(t, db, "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50) COLLATE utf8mb4_0900_bin)")
	mustExec(t, db, "INSERT INTO users VALUES (1, 'ada'), (2, 'grace'), (3, NULL)")
	if got := queryStrings(t, db, "SELECT name FROM users WHERE id = 2"); fmt.Sprint(got) != "[grace]" {
		t.Fatalf("select by key: %v", got)
	}
	if got := queryStrings(t, db, "SELECT id, name FROM users"); fmt.Sprint(got) != "[1,ada 2,grace 3,NULL]" {
		t.Fatalf("full scan: %v", got)
	}
	if got := queryStrings(t, db, "SELECT id FROM users WHERE id > 1 AND id <= 3"); fmt.Sprint(got) != "[2 3]" {
		t.Fatalf("range: %v", got)
	}
	mustExec(t, db, "UPDATE users SET name = 'lovelace' WHERE id = 1")
	mustExec(t, db, "DELETE FROM users WHERE id = 3")
	if got := queryStrings(t, db, "SELECT id, name FROM users"); fmt.Sprint(got) != "[1,lovelace 2,grace]" {
		t.Fatalf("after update/delete: %v", got)
	}
	_, err := db.Exec("INSERT INTO users VALUES (1, 'dup')")
	if mysqlCode(err) != 1062 {
		t.Fatalf("duplicate key: %v", err)
	}
	if got := queryStrings(t, db, "SHOW TABLES"); fmt.Sprint(got) != "[users]" {
		t.Fatalf("show tables: %v", got)
	}
}

func TestTypes(t *testing.T) {
	c := startCluster(t)
	db := c.db(t)
	mustExec(t, db, `CREATE TABLE t (
		id BIGINT UNSIGNED PRIMARY KEY,
		i TINYINT, s SMALLINT, m MEDIUMINT, b BIGINT,
		f FLOAT, d DOUBLE, dec1 DECIMAL(10,3),
		dt DATETIME(6), da DATE, ts TIMESTAMP, tm TIME, y YEAR,
		c CHAR(5), v VARCHAR(20) COLLATE utf8mb4_0900_ai_ci, tx TEXT, bl BLOB, vb VARBINARY(10),
		e ENUM('a','b','c'), st SET('x','y'), j JSON, bt BIT(8), bo BOOLEAN DEFAULT TRUE)`)
	mustExec(t, db, `INSERT INTO t VALUES (18446744073709551615, -5, -300, 70000, -9000000000,
		1.5, 2.25, 12.345, '2024-02-29 12:34:56.789012', '2024-03-01', '2024-01-01 00:00:01', '-12:30:00', 2024,
		'ab', 'Hello', 'long text', 'blobby', x'00ff', 'b', 'x,y', '{"a": [1, 2]}', b'101', FALSE)`)
	mustExec(t, db, "INSERT INTO t (id) VALUES (1)")
	got := queryStrings(t, db, "SELECT * FROM t ORDER BY id")
	want := []string{
		"1,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,1",
		"18446744073709551615,-5,-300,70000,-9000000000,1.5,2.25,12.345,2024-02-29T12:34:56.789012Z,2024-03-01T00:00:00Z,2024-01-01T00:00:01Z,-12:30:00,2024,ab,Hello,long text,blobby,\x00\xff,b,x,y,{\"a\": [1, 2]},\x05,0",
	}
	if fmt.Sprintf("%q", got) != fmt.Sprintf("%q", want) {
		t.Fatalf("got\n%q\nwant\n%q", got, want)
	}
}

func TestCompositeAndStringKeys(t *testing.T) {
	c := startCluster(t)
	db := c.db(t)
	mustExec(t, db, "CREATE TABLE kv (a VARCHAR(10) COLLATE utf8mb4_0900_ai_ci, b INT, v INT, PRIMARY KEY (a, b))")
	mustExec(t, db, "INSERT INTO kv VALUES ('apple', 2, 1), ('apple', 1, 2), ('Banana', 1, 3), ('cherry', -1, 4)")
	if got := queryStrings(t, db, "SELECT v FROM kv WHERE a = 'APPLE' ORDER BY b"); fmt.Sprint(got) != "[2 1]" {
		t.Fatalf("case-insensitive prefix lookup: %v", got)
	}
	if got := queryStrings(t, db, "SELECT v FROM kv WHERE a = 'banana' AND b = 1"); fmt.Sprint(got) != "[3]" {
		t.Fatalf("point lookup: %v", got)
	}
	_, err := db.Exec("INSERT INTO kv VALUES ('BANANA', 1, 9)")
	if mysqlCode(err) != 1062 {
		t.Fatalf("ci duplicate: %v", err)
	}
	if got := queryStrings(t, db, "SELECT a FROM kv WHERE a > 'b' ORDER BY a"); fmt.Sprint(got) != "[Banana cherry]" {
		t.Fatalf("range on ci string: %v", got)
	}
	mustExec(t, db, "CREATE TABLE dk (d DECIMAL(10,2) PRIMARY KEY)")
	mustExec(t, db, "INSERT INTO dk VALUES (-1.5), (0), (2.25), (-10), (100)")
	if got := queryStrings(t, db, "SELECT d FROM dk WHERE d >= -2"); fmt.Sprint(got) != "[-1.50 0.00 2.25 100.00]" {
		t.Fatalf("decimal key range: %v", got)
	}
}

// Slice 3 done-when: snapshot reads across two sessions; ROLLBACK leaves no
// trace; a duplicate key mid-transaction undoes only that statement.
func TestTransactions(t *testing.T) {
	c := startCluster(t)
	db := c.db(t)
	mustExec(t, db, "CREATE TABLE acct (id INT PRIMARY KEY, bal INT)")
	mustExec(t, db, "INSERT INTO acct VALUES (1, 100), (2, 100)")
	ctx := context.Background()
	a, _ := db.Conn(ctx)
	b, _ := db.Conn(ctx)
	defer a.Close()
	defer b.Close()

	mustExecConn(t, a, "START TRANSACTION")
	if got := connStrings(t, a, "SELECT bal FROM acct WHERE id = 1"); fmt.Sprint(got) != "[100]" {
		t.Fatal(got)
	}
	mustExecConn(t, b, "UPDATE acct SET bal = 50 WHERE id = 1")
	if got := connStrings(t, a, "SELECT bal FROM acct WHERE id = 1"); fmt.Sprint(got) != "[100]" {
		t.Fatalf("REPEATABLE READ must keep the old value: %v", got)
	}
	mustExecConn(t, a, "COMMIT")
	if got := connStrings(t, a, "SELECT bal FROM acct WHERE id = 1"); fmt.Sprint(got) != "[50]" {
		t.Fatalf("after commit: %v", got)
	}

	// ROLLBACK leaves no trace.
	mustExecConn(t, a, "BEGIN")
	mustExecConn(t, a, "INSERT INTO acct VALUES (3, 1)")
	mustExecConn(t, a, "DELETE FROM acct WHERE id = 2")
	if got := connStrings(t, a, "SELECT id FROM acct"); fmt.Sprint(got) != "[1 3]" {
		t.Fatalf("read your writes: %v", got)
	}
	if got := connStrings(t, b, "SELECT id FROM acct"); fmt.Sprint(got) != "[1 2]" {
		t.Fatalf("uncommitted writes visible to another session: %v", got)
	}
	mustExecConn(t, a, "ROLLBACK")
	if got := connStrings(t, b, "SELECT id, bal FROM acct"); fmt.Sprint(got) != "[1,50 2,100]" {
		t.Fatalf("rollback left a trace: %v", got)
	}

	// A duplicate key mid-transaction undoes only that statement.
	mustExecConn(t, a, "BEGIN")
	mustExecConn(t, a, "INSERT INTO acct VALUES (4, 4)")
	_, err := a.ExecContext(ctx, "INSERT INTO acct VALUES (5, 5), (1, 1)")
	if mysqlCode(err) != 1062 {
		t.Fatalf("expected 1062, got %v", err)
	}
	mustExecConn(t, a, "COMMIT")
	if got := connStrings(t, b, "SELECT id FROM acct"); fmt.Sprint(got) != "[1 2 4]" {
		t.Fatalf("statement rollback: %v", got)
	}

	// READ COMMITTED sees new commits per statement.
	mustExecConn(t, a, "SET SESSION transaction_isolation = 'READ-COMMITTED'")
	mustExecConn(t, a, "BEGIN")
	connStrings(t, a, "SELECT bal FROM acct WHERE id = 2")
	mustExecConn(t, b, "UPDATE acct SET bal = 7 WHERE id = 2")
	if got := connStrings(t, a, "SELECT bal FROM acct WHERE id = 2"); fmt.Sprint(got) != "[7]" {
		t.Fatalf("READ COMMITTED: %v", got)
	}
	mustExecConn(t, a, "COMMIT")

	_, err = a.ExecContext(ctx, "SET SESSION transaction_isolation = 'SERIALIZABLE'")
	if err == nil {
		_, err = a.ExecContext(ctx, "SELECT * FROM acct")
	}
	if err == nil {
		t.Fatal("SERIALIZABLE must be refused")
	}
}

// Moving a primary key in one UPDATE must touch each row once.
func TestUpdateMovesPrimaryKey(t *testing.T) {
	c := startCluster(t)
	db := c.db(t)
	mustExec(t, db, "CREATE TABLE seq (id INT PRIMARY KEY)")
	for i := 1; i <= 300; i++ {
		mustExec(t, db, "INSERT INTO seq VALUES (?)", i)
	}
	mustExec(t, db, "UPDATE seq SET id = id + 1000")
	if got := queryStrings(t, db, "SELECT COUNT(*), MIN(id), MAX(id) FROM seq"); fmt.Sprint(got) != "[300,1001,1300]" {
		t.Fatalf("got %v", got)
	}
}

// Slice 1 done-when, through SQL: kill the KV process and restart it; every
// acknowledged row comes back from object storage.
func TestDurableAcrossKVRestart(t *testing.T) {
	c := startCluster(t)
	db := c.db(t)
	mustExec(t, db, "CREATE TABLE log (id INT PRIMARY KEY, msg TEXT)")
	for i := 0; i < 100; i++ {
		mustExec(t, db, "INSERT INTO log VALUES (?, ?)", i, fmt.Sprint("m", i))
	}
	c.restartKV()
	if got := queryStrings(t, db, "SELECT COUNT(*) FROM log"); fmt.Sprint(got) != "[100]" {
		t.Fatalf("rows after restart: %v", got)
	}
	mustExec(t, db, "INSERT INTO log VALUES (100, 'after restart')")
}

func TestConcurrentWritersStopgap(t *testing.T) {
	c := startCluster(t)
	db := c.db(t)
	mustExec(t, db, "CREATE TABLE ctr (id INT PRIMARY KEY, n INT)")
	mustExec(t, db, "INSERT INTO ctr VALUES (1, 0)")
	var wg sync.WaitGroup
	var mu sync.Mutex
	ok := 0
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				_, err := db.Exec("UPDATE ctr SET n = n + 1 WHERE id = 1")
				if err == nil {
					mu.Lock()
					ok++
					mu.Unlock()
				} else if mysqlCode(err) != 1213 {
					t.Error(err)
				}
			}
		}()
	}
	wg.Wait()
	// Every successful increment is counted exactly once; conflicts got 1213.
	if got := queryStrings(t, db, "SELECT n FROM ctr"); fmt.Sprint(got) != fmt.Sprintf("[%d]", ok) {
		t.Fatalf("lost update: counter %v, successful updates %d", got, ok)
	}
}

func mustExecConn(t *testing.T, c *sql.Conn, q string) {
	t.Helper()
	if _, err := c.ExecContext(context.Background(), q); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
}

func connStrings(t *testing.T, c *sql.Conn, q string) []string {
	t.Helper()
	rows, err := c.QueryContext(context.Background(), q)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	var out []string
	for rows.Next() {
		vals := make([]sql.NullString, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		rows.Scan(ptrs...)
		s := ""
		for i, v := range vals {
			if i > 0 {
				s += ","
			}
			s += v.String
		}
		out = append(out, s)
	}
	return out
}
