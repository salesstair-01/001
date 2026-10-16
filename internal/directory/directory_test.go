package directory

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/salesstair-01/001/internal/kv"
	"github.com/salesstair-01/001/internal/kvrpc"
	"github.com/salesstair-01/001/internal/objstore"
	"github.com/salesstair-01/001/internal/sqlengine"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))
var _ = os.Stderr

// kvProc is an in-process stand-in for a kvd process.
type kvProc struct {
	id     string
	node   *kv.Node
	grpc   *grpc.Server
	exited chan struct{}
	once   sync.Once
}

func (p *kvProc) kill() { // like kill -9: no release, no checkpoint
	p.once.Do(func() {
		p.grpc.Stop()
		p.node.Close()
		close(p.exited)
	})
}

type env struct {
	t       *testing.T
	store   objstore.Store
	cache   string
	dir     *Directory
	dirAddr string

	mu    sync.Mutex
	procs []*kvProc
}

func (e *env) startNode(id string, idle time.Duration, dedicated bool) *kvProc {
	e.t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		e.t.Fatal(err)
	}
	cc, err := grpc.NewClient(e.dirAddr, append(kvrpc.DialOptions(), grpc.WithTransportCredentials(insecure.NewCredentials()))...)
	if err != nil {
		e.t.Fatal(err)
	}
	p := &kvProc{id: id, exited: make(chan struct{})}
	agent := &Agent{Dir: NewClient(cc), NodeID: id, Addr: lis.Addr().String(), ExitWhenIdle: dedicated, Logger: quiet}
	agent.Exit = func() { go p.kill() }
	p.node = kv.NewNode(kv.Config{
		Store: e.store, DataDir: e.t.TempDir(), CacheDir: e.cache, OwnerID: id, Logger: quiet,
		IdleSuspend: idle, OnSuspend: agent.OnSuspend, OnEmpty: agent.OnEmpty,
	})
	agent.Attach(p.node)
	p.grpc = grpc.NewServer(kvrpc.ServerOption())
	kvrpc.Register(p.grpc, &kvrpc.Server{Node: p.node, AutoOpen: true})
	go p.grpc.Serve(lis)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { <-p.exited; cancel(); cc.Close() }()
	if err := agent.Dir.RegisterNode(ctx, id, agent.Addr); err != nil {
		e.t.Fatal(err)
	}
	go agent.Run(ctx, 0)
	e.mu.Lock()
	e.procs = append(e.procs, p)
	e.mu.Unlock()
	return p
}

func newEnv(t *testing.T, idle time.Duration, spawn bool) *env {
	e := &env{t: t, store: objstore.NewMem(), cache: filepath.Join(t.TempDir(), "cache")}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	e.dirAddr = lis.Addr().String()
	cfg := Config{DBPath: filepath.Join(t.TempDir(), "dir.db"), Dial: kvrpc.Dial, Logger: quiet, NodeTTL: 3 * time.Second}
	if spawn {
		cfg.DedicatedNodes = true
		cfg.Spawn = func(_ context.Context, id string) (<-chan struct{}, error) {
			go e.startNode(id, idle, true)
			p := e.waitProc(id)
			return p.exited, nil
		}
	}
	e.dir, err = Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	g := grpc.NewServer(kvrpc.ServerOption())
	Register(g, e.dir)
	go g.Serve(lis)
	t.Cleanup(func() {
		e.mu.Lock()
		for _, p := range e.procs {
			p.kill()
		}
		e.mu.Unlock()
		g.Stop()
		e.dir.Close()
	})
	return e
}

func (e *env) waitProc(id string) *kvProc {
	for {
		e.mu.Lock()
		for _, p := range e.procs {
			if p.id == id {
				e.mu.Unlock()
				return p
			}
		}
		e.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}
}

// sqlServer starts a query engine routed through the directory.
func (e *env) sqlServer(partition uint64) *sql.DB {
	e.t.Helper()
	cc, err := grpc.NewClient(e.dirAddr, append(kvrpc.DialOptions(), grpc.WithTransportCredentials(insecure.NewCredentials()))...)
	if err != nil {
		e.t.Fatal(err)
	}
	store := sqlengine.NewRoutedStore(&Router{Dir: NewClient(cc), Dial: kvrpc.Dial}, partition)
	eng := sqlengine.NewEngine(store)
	if err := eng.EnsureDatabase(context.Background(), "app"); err != nil {
		e.t.Fatal(err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		e.t.Fatal(err)
	}
	addr := lis.Addr().String()
	lis.Close()
	srv, err := eng.NewServer(addr)
	if err != nil {
		e.t.Fatal(err)
	}
	go srv.Start()
	db, err := sql.Open("mysql", fmt.Sprintf("root@tcp(%s)/app", addr))
	if err != nil {
		e.t.Fatal(err)
	}
	e.t.Cleanup(func() { db.Close(); srv.Close(); store.Close(); cc.Close() })
	for i := 0; i < 100 && db.Ping() != nil; i++ {
		time.Sleep(20 * time.Millisecond)
	}
	return db
}

func exec(t *testing.T, db *sql.DB, q string, args ...any) {
	t.Helper()
	if _, err := db.Exec(q, args...); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
}

func count(t *testing.T, db *sql.DB, q string) int {
	t.Helper()
	var n int
	if err := db.QueryRow(q).Scan(&n); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return n
}

func (e *env) liveProcs() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	n := 0
	for _, p := range e.procs {
		select {
		case <-p.exited:
		default:
			n++
		}
	}
	return n
}

func waitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// Slice 6 done-when: after the idle time the tenant's KV process exits, and
// the next query wakes it and succeeds.
func TestIdleSuspendExitAndWake(t *testing.T) {
	e := newEnv(t, time.Second, true)
	db := e.sqlServer(42)
	exec(t, db, "CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(20))")
	for i := 0; i < 50; i++ {
		exec(t, db, "INSERT INTO t VALUES (?, 'x')", i)
	}
	if e.liveProcs() != 1 {
		t.Fatalf("want one KV process, have %d", e.liveProcs())
	}
	waitFor(t, "the idle KV process to exit", 10*time.Second, func() bool { return e.liveProcs() == 0 })
	routes, _ := e.dir.Partitions()
	if routes[42].Node != "" {
		t.Fatalf("the partition should be released: %+v", routes[42])
	}

	start := time.Now()
	if n := count(t, db, "SELECT COUNT(*) FROM t"); n != 50 {
		t.Fatalf("rows after wake: %d", n)
	}
	t.Logf("wake + first query: %v", time.Since(start))
	if e.liveProcs() != 1 {
		t.Fatalf("the query should have woken one process, have %d", e.liveProcs())
	}
	// The woken owner found the checkpoint in the host's cache.
	e.mu.Lock()
	woken := e.procs[len(e.procs)-1]
	e.mu.Unlock()
	if p := woken.node.Get(42); p == nil || p.CacheHits() == 0 {
		t.Fatal("expected the wake to use the local SST cache")
	}
	exec(t, db, "INSERT INTO t VALUES (100, 'after wake')")
	if n := count(t, db, "SELECT COUNT(*) FROM t"); n != 51 {
		t.Fatalf("rows: %d", n)
	}
}

// A partition with an open transaction is never suspended under it.
func TestNoSuspendWithOpenTransaction(t *testing.T) {
	e := newEnv(t, time.Second, true)
	db := e.sqlServer(7)
	exec(t, db, "CREATE TABLE t (id INT PRIMARY KEY)")
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("INSERT INTO t VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(3 * time.Second) // well past the idle time
	if e.liveProcs() != 1 {
		t.Fatal("suspended with an open transaction")
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit after idle period: %v", err)
	}
	waitFor(t, "suspend after commit", 10*time.Second, func() bool { return e.liveProcs() == 0 })
	if n := count(t, db, "SELECT COUNT(*) FROM t"); n != 1 {
		t.Fatalf("rows: %d", n)
	}
}

// Queries keep working while the partition suspends and wakes repeatedly.
func TestQueriesAcrossManyWakes(t *testing.T) {
	e := newEnv(t, 500*time.Millisecond, true)
	db := e.sqlServer(9)
	exec(t, db, "CREATE TABLE t (id INT PRIMARY KEY)")
	for i := 0; i < 4; i++ {
		exec(t, db, "INSERT INTO t VALUES (?)", i)
		waitFor(t, fmt.Sprint("suspend #", i), 10*time.Second, func() bool { return e.liveProcs() == 0 })
	}
	if n := count(t, db, "SELECT COUNT(*) FROM t"); n != 4 {
		t.Fatalf("rows: %d", n)
	}
}

// Pool mode: when the node serving a partition dies, the query engine
// re-resolves and the directory moves the partition to a live node.
func TestDeadNodeIsReplaced(t *testing.T) {
	e := newEnv(t, 0, false)
	a := e.startNode("a", 0, false)
	db := e.sqlServer(5)
	exec(t, db, "CREATE TABLE t (id INT PRIMARY KEY)")
	exec(t, db, "INSERT INTO t VALUES (1)")
	routes, _ := e.dir.Partitions()
	if routes[5].Node != "a" {
		t.Fatalf("route: %+v", routes[5])
	}
	e.startNode("b", 0, false)
	a.kill()
	exec(t, db, "INSERT INTO t VALUES (2)")
	if n := count(t, db, "SELECT COUNT(*) FROM t"); n != 2 {
		t.Fatalf("rows: %d", n)
	}
	routes, _ = e.dir.Partitions()
	if routes[5].Node != "b" {
		t.Fatalf("partition should have moved to b: %+v", routes[5])
	}
}

// Queries that arrive around the idle time race with suspends and wakes;
// every one must succeed, and no process is left behind.
func TestQueriesAtTheSuspendBoundary(t *testing.T) {
	e := newEnv(t, 500*time.Millisecond, true)
	db := e.sqlServer(11)
	exec(t, db, "CREATE TABLE t (id INT PRIMARY KEY)")
	for i := 0; i < 25; i++ {
		time.Sleep(time.Duration(300+i*37%900) * time.Millisecond)
		exec(t, db, "INSERT INTO t VALUES (?)", i)
	}
	if n := count(t, db, "SELECT COUNT(*) FROM t"); n != 25 {
		t.Fatalf("rows: %d", n)
	}
	waitFor(t, "every process to exit", 10*time.Second, func() bool { return e.liveProcs() == 0 })
}

// Background ANALYZE never keeps a tenant awake (slice 8).
func TestBackgroundAnalyzeDoesNotKeepTenantAwake(t *testing.T) {
	e := newEnv(t, time.Second, true)
	db := e.sqlServer(21)
	exec(t, db, "CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(100))")
	q := "INSERT INTO t VALUES "
	for i := 0; i < 1500; i++ {
		if i > 0 {
			q += ","
		}
		q += fmt.Sprintf("(%d, 'some value %d')", i, i)
	}
	exec(t, db, q) // past the auto-analyze threshold
	start := time.Now()
	waitFor(t, "the tenant to suspend", 10*time.Second, func() bool { return e.liveProcs() == 0 })
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("suspend took %v", d)
	}
	if n := count(t, db, "SELECT COUNT(*) FROM t"); n != 1500 {
		t.Fatalf("rows: %d", n)
	}
}
