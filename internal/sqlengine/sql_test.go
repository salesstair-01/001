package sqlengine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	gomysql "github.com/go-sql-driver/mysql"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/salesstair-01/001/internal/encoding"
	"github.com/salesstair-01/001/internal/kv"
	"github.com/salesstair-01/001/internal/kvrpc"
	"github.com/salesstair-01/001/internal/kvtest"
)

// cluster runs a KV owner (a kvd process, internal/kvtest) and a query
// engine in-process.
type cluster struct {
	t      *testing.T
	kv     *kvtest.Server // nil when the owner is external
	kvAddr string
	engine *Engine
	addr   string
}

func startCluster(t *testing.T) *cluster {
	return startClusterOn(t, "", 1)
}

// startClusterOn runs the query engine against the KV owner at kvAddr, on
// partition, or against a kvd of its own (kvAddr empty).
func startClusterOn(t *testing.T, kvAddr string, partition uint64) *cluster {
	t.Helper()
	c := &cluster{t: t, kvAddr: kvAddr}
	if kvAddr == "" {
		c.kv = kvtest.Start(t, "-txn-lease", "3s")
		c.kvAddr = c.kv.Addr
	}
	cc, err := grpc.NewClient(c.kvAddr, append(kvrpc.DialOptions(), grpc.WithTransportCredentials(insecure.NewCredentials()))...)
	if err != nil {
		t.Fatal(err)
	}
	st := NewStore(kvrpc.NewClient(cc), partition)
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
	})
	return c
}

// restartKV kills the kvd and starts another on the same address and
// store, as after kill -9 and a restart: the next request gets
// EpochChanged and the partition is taken over.
func (c *cluster) restartKV() {
	c.kv.Restart()
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

	// SERIALIZABLE is refused when a transaction reads or writes data. The
	// session can still commit, roll back and change the level back.
	mustExecConn(t, a, "SET SESSION transaction_isolation = 'SERIALIZABLE'")
	for _, q := range []string{"SELECT * FROM acct", "UPDATE acct SET bal = 0", "INSERT INTO acct VALUES (9, 9)"} {
		if _, err := a.ExecContext(ctx, q); err == nil || !strings.Contains(err.Error(), "SERIALIZABLE is not supported") {
			t.Fatalf("%s under SERIALIZABLE: %v", q, err)
		}
	}
	for _, q := range []string{"SELECT 1", "START TRANSACTION", "COMMIT", "BEGIN", "ROLLBACK", "SET @@autocommit = 0", "COMMIT", "SET @@autocommit = 1",
		"SET TRANSACTION ISOLATION LEVEL READ COMMITTED", "SET SESSION transaction_isolation = 'REPEATABLE-READ'"} {
		mustExecConn(t, a, q)
	}
	if got := connStrings(t, a, "SELECT bal FROM acct WHERE id = 2"); fmt.Sprint(got) != "[7]" {
		t.Fatalf("after leaving SERIALIZABLE: %v", got)
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

// With row locks, concurrent increments of one row all succeed and none is lost.
func TestConcurrentIncrements(t *testing.T) {
	c := startCluster(t)
	db := c.db(t)
	mustExec(t, db, "CREATE TABLE ctr (id INT PRIMARY KEY, n INT)")
	mustExec(t, db, "INSERT INTO ctr VALUES (1, 0)")
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				if _, err := db.Exec("UPDATE ctr SET n = n + 1 WHERE id = 1"); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	wg.Wait()
	if got := queryStrings(t, db, "SELECT n FROM ctr"); fmt.Sprint(got) != "[80]" {
		t.Fatalf("lost update: %v", got)
	}
}

func TestLockWaitTimeoutAndDeadlock(t *testing.T) {
	c := startCluster(t)
	db := c.db(t)
	mustExec(t, db, "CREATE TABLE r (id INT PRIMARY KEY, v INT)")
	mustExec(t, db, "INSERT INTO r VALUES (1, 0), (2, 0)")
	ctx := context.Background()
	a, _ := db.Conn(ctx)
	b, _ := db.Conn(ctx)
	defer a.Close()
	defer b.Close()

	// A blocked statement returns 1205 after innodb_lock_wait_timeout and
	// rolls back only that statement.
	mustExecConn(t, a, "BEGIN")
	mustExecConn(t, a, "UPDATE r SET v = 1 WHERE id = 1")
	mustExecConn(t, b, "SET SESSION innodb_lock_wait_timeout = 1")
	mustExecConn(t, b, "BEGIN")
	mustExecConn(t, b, "UPDATE r SET v = 2 WHERE id = 2")
	start := time.Now()
	_, err := b.ExecContext(ctx, "UPDATE r SET v = 2 WHERE id = 1")
	if mysqlCode(err) != 1205 {
		t.Fatalf("want 1205, got %v", err)
	}
	if d := time.Since(start); d < 900*time.Millisecond || d > 5*time.Second {
		t.Fatalf("lock wait took %v", d)
	}
	mustExecConn(t, b, "COMMIT") // b's earlier statement survives
	mustExecConn(t, a, "COMMIT")
	if got := queryStrings(t, db, "SELECT id, v FROM r"); fmt.Sprint(got) != "[1,1 2,2]" {
		t.Fatalf("after 1205: %v", got)
	}

	// A two-session deadlock returns 1213 to one session.
	mustExecConn(t, b, "SET SESSION innodb_lock_wait_timeout = 50")
	mustExecConn(t, a, "BEGIN")
	mustExecConn(t, b, "BEGIN")
	mustExecConn(t, a, "UPDATE r SET v = 10 WHERE id = 1")
	mustExecConn(t, b, "UPDATE r SET v = 20 WHERE id = 2")
	aDone := make(chan error, 1)
	go func() { _, err := a.ExecContext(ctx, "UPDATE r SET v = 10 WHERE id = 2"); aDone <- err }()
	time.Sleep(100 * time.Millisecond)
	_, errB := b.ExecContext(ctx, "UPDATE r SET v = 20 WHERE id = 1")
	errA := <-aDone
	if mysqlCode(errA) == 1213 == (mysqlCode(errB) == 1213) {
		t.Fatalf("exactly one session must get 1213: a=%v b=%v", errA, errB)
	}
	a.ExecContext(ctx, "COMMIT")
	b.ExecContext(ctx, "ROLLBACK")
}

func TestSelectForUpdateBlocks(t *testing.T) {
	c := startCluster(t)
	db := c.db(t)
	mustExec(t, db, "CREATE TABLE q (id INT PRIMARY KEY, v INT)")
	mustExec(t, db, "INSERT INTO q VALUES (1, 0)")
	ctx := context.Background()
	a, _ := db.Conn(ctx)
	b, _ := db.Conn(ctx)
	defer a.Close()
	defer b.Close()
	mustExecConn(t, a, "BEGIN")
	if got := connStrings(t, a, "SELECT v FROM q WHERE id = 1 FOR UPDATE"); fmt.Sprint(got) != "[0]" {
		t.Fatal(got)
	}
	mustExecConn(t, b, "SET SESSION innodb_lock_wait_timeout = 1")
	if _, err := b.ExecContext(ctx, "UPDATE q SET v = 5 WHERE id = 1"); mysqlCode(err) != 1205 {
		t.Fatalf("FOR UPDATE must block writers: %v", err)
	}
	// Plain reads don't block on locks.
	if got := connStrings(t, b, "SELECT v FROM q WHERE id = 1"); fmt.Sprint(got) != "[0]" {
		t.Fatal(got)
	}
	mustExecConn(t, a, "UPDATE q SET v = 1 WHERE id = 1")
	mustExecConn(t, a, "COMMIT")
	mustExecConn(t, b, "UPDATE q SET v = v + 1 WHERE id = 1")
	if got := queryStrings(t, db, "SELECT v FROM q"); fmt.Sprint(got) != "[2]" {
		t.Fatal(got)
	}
}

// Slice 4 done-when: killing a query engine mid-transaction frees its locks
// within 10 s (the transaction lease).
func TestDeadQueryEngineLocksExpire(t *testing.T) {
	c := startCluster(t)
	db := c.db(t)
	mustExec(t, db, "CREATE TABLE z (id INT PRIMARY KEY, v INT)")
	mustExec(t, db, "INSERT INTO z VALUES (1, 0)")

	// A second query engine writes in a transaction, then dies without a
	// rollback: its heartbeats stop.
	cc, err := grpc.NewClient(c.kvAddr, append(kvrpc.DialOptions(), grpc.WithTransportCredentials(insecure.NewCredentials()))...)
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()
	other := NewStore(kvrpc.NewClient(cc), 1)
	txn := other.NewTxn(kv.RepeatableRead, false)
	key := encoding.AppendInt(encoding.Prefix(1, encoding.FirstUserTableID, 0), 1, false)
	if err := txn.Mutate(context.Background(), 1, []kv.Mutation{{Op: kv.OpDelete, Key: key}}, time.Second); err != nil {
		t.Fatal(err)
	}
	other.Close()

	start := time.Now()
	mustExec(t, db, "UPDATE z SET v = 7 WHERE id = 1")
	if d := time.Since(start); d > 12*time.Second {
		t.Fatalf("locks freed after %v", d)
	}
	if got := queryStrings(t, db, "SELECT v FROM z"); fmt.Sprint(got) != "[7]" {
		t.Fatalf("the dead transaction must have been aborted: %v", got)
	}
}

// Slice 4 concurrency test through SQL: many sessions move money between
// accounts; the total never changes.
func TestMoneyTransfersSQL(t *testing.T) {
	c := startCluster(t)
	db := c.db(t)
	mustExec(t, db, "CREATE TABLE acct (id INT PRIMARY KEY, bal INT)")
	for i := 0; i < 5; i++ {
		mustExec(t, db, "INSERT INTO acct VALUES (?, 100)", i)
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	commits, deadlocks := 0, 0
	for w := 0; w < 6; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 15; i++ {
				from, to := (w+i)%5, (w+2*i+1)%5
				if from == to {
					continue
				}
				tx, err := db.Begin()
				if err != nil {
					t.Error(err)
					return
				}
				_, err = tx.Exec("UPDATE acct SET bal = bal - 1 WHERE id = ?", from)
				if err == nil {
					_, err = tx.Exec("UPDATE acct SET bal = bal + 1 WHERE id = ?", to)
				}
				if err == nil {
					err = tx.Commit()
				} else {
					tx.Rollback()
				}
				mu.Lock()
				switch {
				case err == nil:
					commits++
				case mysqlCode(err) == 1213:
					deadlocks++
				default:
					t.Error(err)
				}
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()
	if got := queryStrings(t, db, "SELECT SUM(bal) FROM acct"); fmt.Sprint(got) != "[500]" {
		t.Fatalf("total changed: %v", got)
	}
	t.Logf("commits=%d deadlocks=%d", commits, deadlocks)
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

func TestAutoIncrement(t *testing.T) {
	c := startCluster(t)
	db := c.db(t)
	mustExec(t, db, "CREATE TABLE ai (id BIGINT AUTO_INCREMENT PRIMARY KEY, v VARCHAR(10))")
	r := mustExec(t, db, "INSERT INTO ai (v) VALUES ('a')")
	if id, _ := r.LastInsertId(); id != 1 {
		t.Fatalf("first id %d", id)
	}
	mustExec(t, db, "INSERT INTO ai (v) VALUES ('b'), ('c'), ('d')")
	mustExec(t, db, "INSERT INTO ai VALUES (100, 'explicit')")
	r = mustExec(t, db, "INSERT INTO ai (v) VALUES ('after')")
	if id, _ := r.LastInsertId(); id != 101 {
		t.Fatalf("after an explicit value: %d", id)
	}
	// A rolled-back insert doesn't hand its value out again.
	tx, _ := db.Begin()
	tx.Exec("INSERT INTO ai (v) VALUES ('rolled back')")
	tx.Rollback()
	r = mustExec(t, db, "INSERT INTO ai (v) VALUES ('next')")
	if id, _ := r.LastInsertId(); id != 103 {
		t.Fatalf("after rollback: %d", id)
	}
	// The counter survives a KV restart.
	c.restartKV()
	r = mustExec(t, db, "INSERT INTO ai (v) VALUES ('restart')")
	if id, _ := r.LastInsertId(); id != 104 {
		t.Fatalf("after restart: %d", id)
	}
	if got := queryStrings(t, db, "SELECT id FROM ai ORDER BY id"); fmt.Sprint(got) != "[1 2 3 4 100 101 103 104]" {
		t.Fatalf("ids: %v", got)
	}
	// Concurrent inserts never collide.
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				if _, err := db.Exec("INSERT INTO ai (v) VALUES ('c')"); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	wg.Wait()
	if got := queryStrings(t, db, "SELECT COUNT(*), COUNT(DISTINCT id) FROM ai"); fmt.Sprint(got) != "[88,88]" {
		t.Fatalf("concurrent ids: %v", got)
	}
	mustExec(t, db, "ALTER TABLE ai AUTO_INCREMENT = 1000")
	r = mustExec(t, db, "INSERT INTO ai (v) VALUES ('altered')")
	if id, _ := r.LastInsertId(); id != 1000 {
		t.Fatalf("after ALTER TABLE AUTO_INCREMENT: %d", id)
	}
}

func TestSecondaryIndexes(t *testing.T) {
	c := startCluster(t)
	db := c.db(t)
	mustExec(t, db, `CREATE TABLE people (
		id INT PRIMARY KEY,
		email VARCHAR(100) COLLATE utf8mb4_0900_ai_ci,
		city VARCHAR(50),
		age INT,
		UNIQUE KEY email_uq (email),
		KEY city_age (city, age))`)
	mustExec(t, db, `INSERT INTO people VALUES
		(1, 'ada@x.org', 'London', 36), (2, 'grace@x.org', 'NYC', 85),
		(3, 'alan@x.org', 'London', 41), (4, NULL, 'Paris', 30), (5, NULL, 'Paris', 31)`)

	// Unique: case-insensitive duplicates are rejected; several NULLs are fine.
	if _, err := db.Exec("INSERT INTO people VALUES (6, 'ADA@x.org', 'Oslo', 1)"); mysqlCode(err) != 1062 {
		t.Fatalf("unique violation: %v", err)
	}
	mustExec(t, db, "INSERT INTO people VALUES (6, NULL, 'Oslo', 1)")
	if _, err := db.Exec("UPDATE people SET email = 'grace@x.org' WHERE id = 1"); mysqlCode(err) != 1062 {
		t.Fatalf("unique violation on update: %v", err)
	}

	// Lookups through the indexes.
	if got := queryStrings(t, db, "SELECT id FROM people WHERE email = 'GRACE@X.ORG'"); fmt.Sprint(got) != "[2]" {
		t.Fatalf("unique lookup: %v", got)
	}
	if got := queryStrings(t, db, "SELECT id FROM people WHERE city = 'London' ORDER BY id"); fmt.Sprint(got) != "[1 3]" {
		t.Fatalf("prefix lookup: %v", got)
	}
	if got := queryStrings(t, db, "SELECT id FROM people WHERE city = 'Paris' AND age > 30"); fmt.Sprint(got) != "[5]" {
		t.Fatalf("range on second column: %v", got)
	}
	if got := queryStrings(t, db, "SELECT id FROM people WHERE email IS NULL ORDER BY id"); fmt.Sprint(got) != "[4 5 6]" {
		t.Fatalf("IS NULL on unique index: %v", got)
	}
	if plan := strings.Join(queryStrings(t, db, "EXPLAIN FORMAT=TREE SELECT id FROM people WHERE city = 'London'"), "\n"); !strings.Contains(plan, "index: [people.city,people.age]") {
		t.Fatalf("the planner should use city_age:\n%s", plan)
	}

	// Updates and deletes keep the indexes in step.
	mustExec(t, db, "UPDATE people SET city = 'Berlin' WHERE id = 3")
	mustExec(t, db, "UPDATE people SET email = 'ada@new.org' WHERE id = 1")
	mustExec(t, db, "DELETE FROM people WHERE id = 2")
	if got := queryStrings(t, db, "SELECT id FROM people WHERE city = 'London'"); fmt.Sprint(got) != "[1]" {
		t.Fatalf("after update: %v", got)
	}
	if got := queryStrings(t, db, "SELECT id FROM people WHERE email = 'ada@x.org'"); len(got) != 0 {
		t.Fatalf("stale unique entry: %v", got)
	}
	mustExec(t, db, "INSERT INTO people VALUES (7, 'ada@x.org', 'Rome', 2)") // the old email is free again
	if got := queryStrings(t, db, "SELECT id FROM people WHERE email = 'grace@x.org'"); len(got) != 0 {
		t.Fatalf("deleted row still indexed: %v", got)
	}
	// Moving a primary key moves its index entries.
	mustExec(t, db, "UPDATE people SET id = 70 WHERE id = 7")
	if got := queryStrings(t, db, "SELECT id FROM people WHERE email = 'ada@x.org'"); fmt.Sprint(got) != "[70]" {
		t.Fatalf("after moving the primary key: %v", got)
	}

	// ON DUPLICATE KEY UPDATE through a unique secondary key.
	mustExec(t, db, "INSERT INTO people VALUES (8, 'ada@x.org', 'X', 0) ON DUPLICATE KEY UPDATE age = age + 100")
	if got := queryStrings(t, db, "SELECT id, age FROM people WHERE email = 'ada@x.org'"); fmt.Sprint(got) != "[70,102]" {
		t.Fatalf("on duplicate key update: %v", got)
	}

	// CREATE INDEX backfills, and rejects existing duplicates for UNIQUE.
	mustExec(t, db, "CREATE INDEX age_idx ON people (age)")
	if got := queryStrings(t, db, "SELECT id FROM people WHERE age BETWEEN 30 AND 40 ORDER BY id"); fmt.Sprint(got) != "[1 4 5]" {
		t.Fatalf("backfilled index: %v", got)
	}
	if _, err := db.Exec("CREATE UNIQUE INDEX city_uq ON people (city)"); err == nil {
		t.Fatal("unique index over duplicate cities must fail")
	}
	mustExec(t, db, "CREATE INDEX city_prefix ON people (city(2))")
	if got := queryStrings(t, db, "SELECT id FROM people WHERE city = 'Paris' ORDER BY id"); fmt.Sprint(got) != "[4 5]" {
		t.Fatalf("prefix index: %v", got)
	}
	mustExec(t, db, "ALTER TABLE people RENAME INDEX age_idx TO age_i")
	mustExec(t, db, "DROP INDEX city_prefix ON people")
	got := strings.Join(queryStrings(t, db, "SHOW INDEX FROM people"), "\n")
	for _, want := range []string{"email_uq", "city_age", "age_i"} {
		if !strings.Contains(got, want) {
			t.Fatalf("SHOW INDEX missing %s:\n%s", want, got)
		}
	}
	if strings.Contains(got, "city_prefix") {
		t.Fatal("dropped index still listed")
	}
	// Indexes survive a KV restart.
	c.restartKV()
	if got := queryStrings(t, db, "SELECT id FROM people WHERE city = 'Paris' AND age = 31"); fmt.Sprint(got) != "[5]" {
		t.Fatalf("after restart: %v", got)
	}
}

// Unique-key locks make concurrent inserts of one value race-free.
func TestUniqueIndexConcurrentInserts(t *testing.T) {
	c := startCluster(t)
	db := c.db(t)
	mustExec(t, db, "CREATE TABLE u (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(20), UNIQUE KEY (name))")
	var wg sync.WaitGroup
	var mu sync.Mutex
	ok, dup := 0, 0
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				_, err := db.Exec("INSERT INTO u (name) VALUES (?)", fmt.Sprint("n", i))
				mu.Lock()
				switch {
				case err == nil:
					ok++
				case mysqlCode(err) == 1062 || mysqlCode(err) == 1213:
					dup++
				default:
					t.Error(err)
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if ok != 10 {
		t.Fatalf("exactly one insert per name must win: %d won, %d lost", ok, dup)
	}
	if got := queryStrings(t, db, "SELECT COUNT(DISTINCT name), COUNT(*) FROM u"); fmt.Sprint(got) != "[10,10]" {
		t.Fatalf("got %v", got)
	}
}

// An UPDATE that changes only the case of a value under a case-insensitive
// collation stores the new bytes and counts the row, as MySQL does.
func TestCaseOnlyUpdate(t *testing.T) {
	c := startCluster(t)
	db := c.db(t)
	mustExec(t, db, "CREATE TABLE ci (id INT PRIMARY KEY, v VARCHAR(20), n INT, KEY (v))")
	mustExec(t, db, "INSERT INTO ci VALUES (1, 'b', 1), (2, 'c', 2)")
	affected := func(q string) int64 {
		t.Helper()
		n, err := mustExec(t, db, q).RowsAffected()
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	if n := affected("UPDATE ci SET v = 'B' WHERE id = 1"); n != 1 {
		t.Fatalf("rows affected %d, want 1", n)
	}
	if n := affected("UPDATE ci SET v = 'C' WHERE v = 'c'"); n != 1 {
		t.Fatalf("through the index: rows affected %d, want 1", n)
	}
	if n := affected("UPDATE ci SET v = 'B' WHERE id = 1"); n != 0 {
		t.Fatalf("unchanged: rows affected %d, want 0", n)
	}
	if got := fmt.Sprint(queryStrings(t, db, "SELECT CAST(v AS BINARY) FROM ci ORDER BY id")); got != "[B C]" {
		t.Fatalf("stored %s, want [B C]", got)
	}
	// The column keeps its collation: lookups ignore case, through the index too.
	if got := fmt.Sprint(queryStrings(t, db, "SELECT id FROM ci WHERE v = 'b'")); got != "[1]" {
		t.Fatalf("lookup %s, want [1]", got)
	}
	if got := fmt.Sprint(queryStrings(t, db, "SELECT id FROM ci FORCE INDEX (v) WHERE v = 'c'")); got != "[2]" {
		t.Fatalf("index lookup %s, want [2]", got)
	}

	// A table with a foreign key is updated through another path.
	mustExec(t, db, "CREATE TABLE child (id INT PRIMARY KEY, pid INT, v VARCHAR(20), FOREIGN KEY (pid) REFERENCES ci (id))")
	mustExec(t, db, "INSERT INTO child VALUES (10, 1, 'z')")
	if n := affected("UPDATE child SET v = 'Z' WHERE id = 10"); n != 1 {
		t.Fatalf("foreign-key table: rows affected %d, want 1", n)
	}
	if got := fmt.Sprint(queryStrings(t, db, "SELECT CAST(v AS BINARY) FROM child")); got != "[Z]" {
		t.Fatalf("foreign-key table: stored %s, want [Z]", got)
	}
}

// Statements that would read or write files on the query engine's host are
// refused (1290), as MySQL refuses them with secure_file_priv = NULL.
func TestNoServerFileAccess(t *testing.T) {
	c := startCluster(t)
	db := c.db(t)
	mustExec(t, db, "CREATE TABLE f (a INT)")
	out := filepath.Join(t.TempDir(), "out.txt")
	for _, q := range []string{
		"SELECT 1 INTO OUTFILE '" + out + "'",
		"SELECT 1 INTO DUMPFILE '" + out + "'",
		"SELECT LOAD_FILE('/etc/hostname')",
		"SELECT (SELECT LOAD_FILE('/etc/hostname'))",
		"LOAD DATA INFILE '/etc/hostname' INTO TABLE f",
	} {
		_, err := db.Exec(q)
		var me *gomysql.MySQLError
		if !errors.As(err, &me) || me.Number != 1290 {
			t.Errorf("%s: got %v, want error 1290", q, err)
		}
	}
	if _, err := os.Stat(out); !os.IsNotExist(err) {
		t.Fatalf("%s was written", out)
	}
	if got := fmt.Sprint(queryStrings(t, db, "SELECT 1")); got != "[1]" {
		t.Fatalf("plain SELECT: %s", got)
	}
}

// A binary cast longer than max_allowed_packet is NULL with warning 1301,
// as in MySQL, instead of allocating its length.
func TestCastBinaryOverMaxAllowedPacket(t *testing.T) {
	c := startCluster(t)
	db := c.db(t)
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	var v sql.NullString
	if err := conn.QueryRowContext(context.Background(), "SELECT CAST('a' AS BINARY(4294967294))").Scan(&v); err != nil {
		t.Fatal(err)
	}
	if v.Valid {
		t.Fatalf("got %d bytes, want NULL", len(v.String))
	}
	rows, err := conn.QueryContext(context.Background(), "SHOW WARNINGS")
	if err != nil {
		t.Fatal(err)
	}
	var codes []int
	for rows.Next() {
		var level, msg string
		var code int
		if err := rows.Scan(&level, &code, &msg); err != nil {
			t.Fatal(err)
		}
		codes = append(codes, code)
	}
	rows.Close()
	if fmt.Sprint(codes) != "[1301]" {
		t.Fatalf("warnings %v, want [1301]", codes)
	}
	if got := fmt.Sprint(queryStrings(t, db, "SELECT HEX(CAST('a' AS BINARY(3)))")); got != "[610000]" {
		t.Fatalf("short cast %s, want [610000]", got)
	}
}
