package proxy

import (
	"context"
	"crypto/tls"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	gomysql "github.com/go-sql-driver/mysql"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/salesstair-01/001/internal/directory"
	"github.com/salesstair-01/001/internal/kv"
	"github.com/salesstair-01/001/internal/kvrpc"
	"github.com/salesstair-01/001/internal/objstore"
	"github.com/salesstair-01/001/internal/sqlengine"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// stack runs every layer in-process: a directory that starts a dedicated KV
// node per woken tenant, a pool of generic query engines, and the proxy.
type stack struct {
	t       *testing.T
	store   objstore.Store
	cache   string
	dir     *directory.Directory
	dirAddr string
	pool    *Pool
	proxy   *Proxy

	mu      sync.Mutex
	kvLive  int
	kvStops []func()
	qeLive  int
}

type stackOpts struct {
	kvIdle, backendIdle, engineIdle time.Duration
	warm                            int
	sharedKV                        bool     // one KV node serves every tenant
	tenants                         []Tenant // default: acme only
}

func newStack(t *testing.T, o stackOpts) *stack {
	s := &stack{t: t, store: objstore.NewMem(), cache: filepath.Join(t.TempDir(), "cache")}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s.dirAddr = lis.Addr().String()
	s.dir, err = directory.Open(directory.Config{
		DBPath: filepath.Join(t.TempDir(), "dir.db"), Dial: kvrpc.Dial, Logger: quiet, DedicatedNodes: !o.sharedKV,
		Spawn: func(_ context.Context, id string) (<-chan struct{}, error) { return s.startKV(id, o.kvIdle), nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	g := grpc.NewServer(kvrpc.ServerOption())
	directory.Register(g, s.dir)
	go g.Serve(lis)

	s.pool = NewPool(PoolConfig{Spawn: s.startQE, Warm: o.warm, Idle: o.engineIdle, Logger: quiet})
	if o.tenants == nil {
		o.tenants = []Tenant{{Name: "acme", Partition: 42, Users: map[string]string{"app": "s3cret"}}}
	}
	s.proxy, err = New("127.0.0.1:0", Config{
		Tenants: NewTenants("db.test", o.tenants...),
		Pool:    s.pool, TenantUser: sqlengine.TenantUser, BackendIdle: o.backendIdle, Logger: quiet,
		TLS: mustTLS(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		s.proxy.Close()
		s.pool.Close()
		s.mu.Lock()
		stops := s.kvStops
		s.mu.Unlock()
		for _, f := range stops {
			f()
		}
		g.Stop()
		s.dir.Close()
	})
	return s
}

func mustTLS(t *testing.T) *tls.Config {
	c, err := SelfSignedTLS("localhost", "*.db.test")
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// startKV is an in-process stand-in for a spawned per-tenant kvd.
func (s *stack) startKV(id string, idle time.Duration) <-chan struct{} {
	lis, _ := net.Listen("tcp", "127.0.0.1:0")
	cc, _ := grpc.NewClient(s.dirAddr, append(kvrpc.DialOptions(), grpc.WithTransportCredentials(insecure.NewCredentials()))...)
	exited := make(chan struct{})
	var once sync.Once
	var node *kv.Node
	g := grpc.NewServer(kvrpc.ServerOption())
	stop := func() {
		once.Do(func() {
			g.Stop()
			node.Close()
			cc.Close()
			s.mu.Lock()
			s.kvLive--
			s.mu.Unlock()
			close(exited)
		})
	}
	agent := &directory.Agent{Dir: directory.NewClient(cc), NodeID: id, Addr: lis.Addr().String(), ExitWhenIdle: true, Logger: quiet,
		Exit: func() { go stop() }}
	node = kv.NewNode(kv.Config{Store: s.store, DataDir: s.t.TempDir(), CacheDir: s.cache, OwnerID: id, Logger: quiet,
		IdleSuspend: idle, OnSuspend: agent.OnSuspend, OnEmpty: agent.OnEmpty})
	agent.Attach(node)
	kvrpc.Register(g, &kvrpc.Server{Node: node, AutoOpen: true})
	go g.Serve(lis)
	s.mu.Lock()
	s.kvLive++
	s.kvStops = append(s.kvStops, stop)
	s.mu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	go func() { <-exited; cancel() }()
	agent.Dir.RegisterNode(ctx, id, agent.Addr)
	go agent.Run(ctx, 0)
	return exited
}

// startQE is an in-process stand-in for a spawned generic sqld.
func (s *stack) startQE(ctx context.Context) (*Engine, error) {
	cc, err := grpc.NewClient(s.dirAddr, append(kvrpc.DialOptions(), grpc.WithTransportCredentials(insecure.NewCredentials()))...)
	if err != nil {
		return nil, err
	}
	eng := sqlengine.NewGenericEngine(&directory.Router{Dir: directory.NewClient(cc), Dial: kvrpc.Dial}, "")
	lis, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := lis.Addr().String()
	lis.Close()
	srv, err := eng.NewServer(addr)
	if err != nil {
		return nil, err
	}
	go srv.Start()
	if err := WaitReady(ctx, addr); err != nil {
		return nil, err
	}
	exited := make(chan struct{})
	var once sync.Once
	s.mu.Lock()
	s.qeLive++
	s.mu.Unlock()
	return &Engine{Addr: addr, Exited: exited, Stop: func() {
		once.Do(func() {
			srv.Close()
			cc.Close()
			s.mu.Lock()
			s.qeLive--
			s.mu.Unlock()
			close(exited)
		})
	}}, nil
}

func (s *stack) live() (kvs, qes int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.kvLive, s.qeLive
}

func (s *stack) waitZero(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		k, q := s.live()
		if k == 0 && q == 0 && s.proxy.Backends() == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("compute did not reach zero: %d KV, %d query engines, %d backends", k, q, s.proxy.Backends())
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (s *stack) db(t *testing.T, user string, extra string) *sql.DB {
	db, err := sql.Open("mysql", fmt.Sprintf("%s:s3cret@tcp(%s)/?%s", user, s.proxy.Addr(), extra))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func mustExec(t *testing.T, db interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}, q string, args ...any) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(), q, args...); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
}

func scalar(t *testing.T, db interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, q string, args ...any) string {
	t.Helper()
	var v sql.NullString
	if err := db.QueryRowContext(context.Background(), q, args...).Scan(&v); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return v.String
}

func TestAuthAndTenantRouting(t *testing.T) {
	s := newStack(t, stackOpts{backendIdle: time.Minute, engineIdle: time.Minute})
	good := s.db(t, "app@acme", "")
	if err := good.Ping(); err != nil {
		t.Fatalf("caching_sha2_password login: %v", err)
	}
	bad, _ := sql.Open("mysql", fmt.Sprintf("app@acme:wrong@tcp(%s)/", s.proxy.Addr()))
	defer bad.Close()
	if err := bad.Ping(); err == nil {
		t.Fatal("wrong password accepted")
	}
	other, _ := sql.Open("mysql", fmt.Sprintf("app@nobody:s3cret@tcp(%s)/", s.proxy.Addr()))
	defer other.Close()
	if err := other.Ping(); err == nil {
		t.Fatal("unknown tenant accepted")
	}
	// Native password and TLS with the tenant in the hostname (bare user name).
	native := s.db(t, "app@acme", "allowNativePasswords=true")
	if err := native.Ping(); err != nil {
		t.Fatal(err)
	}
	gomysql.RegisterTLSConfig("acme-sni", &tls.Config{ServerName: "acme.db.test", InsecureSkipVerify: true})
	sni := s.db(t, "app", "tls=acme-sni")
	mustExec(t, sni, "CREATE DATABASE app")
	mustExec(t, sni, "CREATE TABLE app.t (id INT PRIMARY KEY)")
	mustExec(t, good, "INSERT INTO app.t VALUES (1)")
	if got := scalar(t, sni, "SELECT COUNT(*) FROM app.t"); got != "1" {
		t.Fatalf("both logins should reach tenant acme: %s", got)
	}
}

// Connecting, pinging and pool health checks never wake compute.
func TestPingsDoNotWakeCompute(t *testing.T) {
	s := newStack(t, stackOpts{backendIdle: time.Minute, engineIdle: time.Minute})
	db := s.db(t, "app@acme", "")
	for i := 0; i < 5; i++ {
		if err := db.Ping(); err != nil {
			t.Fatal(err)
		}
		if got := scalar(t, db, "SELECT 1"); got != "1" {
			t.Fatal(got)
		}
		mustExec(t, db, "SET NAMES utf8mb4")
	}
	if k, q := s.live(); k != 0 || q != 0 {
		t.Fatalf("pings woke compute: %d KV, %d query engines", k, q)
	}
}

// Slice 7 done-when: an app with a connection pool works across a period
// where query engines and KV owners are all at zero; cold starts measured.
func TestPooledAppAcrossZero(t *testing.T) {
	s := newStack(t, stackOpts{kvIdle: 500 * time.Millisecond, backendIdle: 300 * time.Millisecond, engineIdle: 300 * time.Millisecond})
	db := s.db(t, "app@acme", "")
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(4)
	db.SetConnMaxIdleTime(time.Hour)
	mustExec(t, db, "CREATE DATABASE app")
	mustExec(t, db, "CREATE TABLE app.items (id INT PRIMARY KEY, name VARCHAR(20))")
	mustExec(t, db, "INSERT INTO app.items VALUES (1, 'a'), (2, 'b')")

	var cold []time.Duration
	for round := 0; round < 8; round++ {
		s.waitZero(t)
		if err := db.Ping(); err != nil { // the pool's health check, answered by the proxy
			t.Fatal(err)
		}
		start := time.Now()
		if got := scalar(t, db, "SELECT COUNT(*) FROM app.items"); got != fmt.Sprint(2+round) {
			t.Fatalf("round %d: %s rows", round, got)
		}
		cold = append(cold, time.Since(start))
		mustExec(t, db, "INSERT INTO app.items VALUES (?, 'x')", 3+round)
	}
	sort.Slice(cold, func(i, j int) bool { return cold[i] < cold[j] })
	t.Logf("cold start from all-zero: p50 %v, max %v (n=%d)", cold[len(cold)/2], cold[len(cold)-1], len(cold))
}

// Session variables, the current database and prepared statements survive
// the session moving to a new query engine.
func TestSessionReplayAndPreparedStatements(t *testing.T) {
	s := newStack(t, stackOpts{backendIdle: 200 * time.Millisecond, engineIdle: 200 * time.Millisecond})
	setup := s.db(t, "app@acme", "")
	mustExec(t, setup, "CREATE DATABASE app")
	mustExec(t, setup, "CREATE TABLE app.kv (id INT PRIMARY KEY, v VARCHAR(10))")
	mustExec(t, setup, "INSERT INTO app.kv VALUES (1, 'one'), (2, 'two')")

	db := s.db(t, "app@acme", "")
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mustExec(t, conn, "USE app")
	mustExec(t, conn, "SET @greeting = 'hello'")
	mustExec(t, conn, "SET SESSION innodb_lock_wait_timeout = 7")
	stmt, err := conn.PrepareContext(context.Background(), "SELECT v FROM kv WHERE id = ?")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	var v string
	if err := stmt.QueryRow(1).Scan(&v); err != nil || v != "one" {
		t.Fatalf("prepared: %q %v", v, err)
	}
	waitFor(t, "the backend to be released", func() bool { return s.proxy.Backends() == 0 })
	waitFor(t, "the query engine to stop", func() bool { _, q := s.live(); return q == 0 })

	if got := scalar(t, conn, "SELECT @greeting"); got != "hello" {
		t.Fatalf("user variable after replay: %q", got)
	}
	if got := scalar(t, conn, "SELECT @@innodb_lock_wait_timeout"); got != "7" {
		t.Fatalf("session variable after replay: %q", got)
	}
	if got := scalar(t, conn, "SELECT COUNT(*) FROM kv"); got != "2" {
		t.Fatalf("current database after replay: %q", got)
	}
	if err := stmt.QueryRow(2).Scan(&v); err != nil || v != "two" {
		t.Fatalf("prepared statement after reconnect: %q %v", v, err)
	}
}

// A session in a transaction, or holding GET_LOCK, keeps its query engine.
func TestTransactionsAndLocksPin(t *testing.T) {
	s := newStack(t, stackOpts{backendIdle: 200 * time.Millisecond, engineIdle: 200 * time.Millisecond})
	db := s.db(t, "app@acme", "")
	mustExec(t, db, "CREATE DATABASE app")
	mustExec(t, db, "CREATE TABLE app.t (id INT PRIMARY KEY)")
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, tx, "INSERT INTO app.t VALUES (1)")
	time.Sleep(time.Second) // well past the backend idle time
	if s.proxy.Backends() == 0 {
		t.Fatal("a session in a transaction lost its backend")
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "release after commit", func() bool { return s.proxy.Backends() == 0 })

	conn, _ := db.Conn(context.Background())
	defer conn.Close()
	if got := scalar(t, conn, "SELECT GET_LOCK('job', 1)"); got != "1" {
		t.Fatalf("GET_LOCK: %s", got)
	}
	time.Sleep(time.Second)
	if s.proxy.Backends() != 1 {
		t.Fatal("a session holding GET_LOCK lost its backend")
	}
	if got := scalar(t, conn, "SELECT IS_USED_LOCK('job') IS NOT NULL"); got != "1" {
		t.Fatal("the lock is gone")
	}
}

// With warm engines, a cold tenant doesn't wait for a query engine to start.
func TestWarmPool(t *testing.T) {
	s := newStack(t, stackOpts{kvIdle: 500 * time.Millisecond, backendIdle: 200 * time.Millisecond, engineIdle: 200 * time.Millisecond, warm: 1})
	waitFor(t, "a warm engine", func() bool { _, q := s.live(); return q == 1 })
	db := s.db(t, "app@acme", "")
	mustExec(t, db, "CREATE DATABASE app")
	waitFor(t, "a spare warm engine", func() bool { _, q := s.live(); return q >= 1 && s.pool.Running() >= 1 })
	time.Sleep(2 * time.Second)
	if _, q := s.live(); q != 1 {
		t.Fatalf("idle engines beyond the warm one should stop: %d running", q)
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
