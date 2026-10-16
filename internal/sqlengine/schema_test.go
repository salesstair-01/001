package sqlengine

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gmssql "github.com/dolthub/go-mysql-server/sql"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/salesstair-01/001/internal/encoding"
	"github.com/salesstair-01/001/internal/kv"
	"github.com/salesstair-01/001/internal/kvrpc"
)

// secondEngine starts another query engine on the same tenant, with its own
// catalog cache, as a second sqld would be.
func (c *cluster) secondEngine(t *testing.T) (*Engine, *sql.DB) {
	t.Helper()
	cc, err := grpc.NewClient(c.kvAddr, append(kvrpc.DialOptions(), grpc.WithTransportCredentials(insecure.NewCredentials()))...)
	if err != nil {
		t.Fatal(err)
	}
	eng := NewEngine(NewStore(kvrpc.NewClient(cc), 1))
	lis, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := lis.Addr().String()
	lis.Close()
	srv, err := eng.NewServer(addr)
	if err != nil {
		t.Fatal(err)
	}
	go srv.Start()
	db, err := sql.Open("mysql", fmt.Sprintf("root@tcp(%s)/app", addr))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close(); srv.Close(); eng.Store.Close(); cc.Close() })
	for i := 0; i < 100 && db.Ping() != nil; i++ {
		time.Sleep(20 * time.Millisecond)
	}
	return eng, db
}

// retry runs a statement until it succeeds, retrying 1213 as an ORM would.
func retry(db *sql.DB, q string, args ...any) error {
	for i := 0; ; i++ {
		_, err := db.Exec(q, args...)
		if err == nil || mysqlCode(err) != 1213 || i > 50 {
			return err
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// rawRows reads a table's stored rows straight from the KV.
func rawRows(t *testing.T, e *Engine, ti *TableInfo) map[string][]byte {
	t.Helper()
	txn := e.Store.NewTxn(kv.ReadCommitted, true)
	defer txn.Rollback(context.Background())
	prefix := encoding.Prefix(1, ti.ID, encoding.PrimaryIndexID)
	return scanAll(t, txn, prefix)
}

func scanAll(t *testing.T, txn *Txn, prefix []byte) map[string][]byte {
	out := map[string][]byte{}
	start, end := prefix, encoding.PrefixEnd(prefix)
	for {
		resp, err := txn.Scan(context.Background(), start, end, 1000, kvrpc.ReadOpts{Stmt: 1})
		if err != nil {
			t.Fatal(err)
		}
		for i, k := range resp.Keys {
			out[string(k)] = resp.Values[i]
		}
		if len(resp.Resume) == 0 {
			return out
		}
		start = resp.Resume
	}
}

func tableInfo(t *testing.T, e *Engine, name string) *TableInfo {
	t.Helper()
	e.Store.catalog.Invalidate()
	ti, err := e.Store.catalog.Table(context.Background(), "app", name)
	if err != nil || ti == nil {
		t.Fatalf("table %s: %v", name, err)
	}
	return ti
}

// checkIndex verifies that an index holds exactly one entry per row.
func checkIndex(t *testing.T, e *Engine, table, index string) int {
	t.Helper()
	ti := tableInfo(t, e, table)
	tbl, err := newTable(newCtx(), e.Store.catalog, ti)
	if err != nil {
		t.Fatal(err)
	}
	var ix *secIndex
	for _, x := range tbl.indexes {
		if x.info.Name == index {
			ix = x
		}
	}
	if ix == nil || ix.info.State != "" {
		t.Fatalf("index %s missing or not public", index)
	}
	txn := e.Store.NewTxn(kv.ReadCommitted, true)
	defer txn.Rollback(context.Background())
	rows := scanAll(t, txn, tbl.prefix())
	want := map[string][]byte{}
	for pk, v := range rows {
		row, err := tbl.decodeRow(context.Background(), v)
		if err != nil {
			t.Fatal(err)
		}
		k, val, _, err := ix.entry(context.Background(), row, []byte(pk))
		if err != nil {
			t.Fatal(err)
		}
		want[string(k)] = val
	}
	got := scanAll(t, txn, ix.keyPrefix())
	for k, v := range want {
		if g, ok := got[k]; !ok || !bytes.Equal(g, v) {
			t.Fatalf("index %s: a row has no matching entry", index)
		}
	}
	for k := range got {
		if _, ok := want[k]; !ok {
			t.Fatalf("index %s: an entry has no row (stale write)", index)
		}
	}
	return len(rows)
}

func newCtx() *gmssql.Context { return gmssql.NewEmptyContext() }

// Slice 8 done-when, part 1: ALTER TABLE ADD COLUMN runs on one query engine
// while another keeps writing, and no write uses a stale schema.
func TestAddColumnWhileAnotherEngineWrites(t *testing.T) {
	c := startCluster(t)
	a := c.db(t)
	mustExec(t, a, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
	engB, b := c.secondEngine(t)

	var next atomic.Int64
	var mu sync.Mutex
	started := map[int64]time.Time{}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < 3; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				id := next.Add(1)
				mu.Lock()
				started[id] = time.Now()
				mu.Unlock()
				if err := retry(b, "INSERT INTO t (id, v) VALUES (?, ?)", id, id); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	time.Sleep(300 * time.Millisecond)
	mustExec(t, a, "ALTER TABLE t ADD COLUMN c INT NOT NULL DEFAULT 7")
	altered := time.Now()
	time.Sleep(CatalogLease + 500*time.Millisecond) // long enough for B's lease to run out
	close(stop)
	wg.Wait()

	ti := tableInfo(t, engB, "t")
	var cID uint32
	for _, col := range ti.Columns {
		if col.Name == "c" {
			cID = col.ID
		}
	}
	tbl, _ := newTable(newCtx(), engB.Store.catalog, ti)
	after, stale := 0, 0
	for pk, v := range rawRows(t, engB, ti) {
		r, err := encoding.DecodeRow(v)
		if err != nil {
			t.Fatal(err)
		}
		row, _ := tbl.decodeRow(context.Background(), v)
		id := row[0].(int32)
		mu.Lock()
		s := started[int64(id)]
		mu.Unlock()
		if s.After(altered) {
			after++
			if st, _, _ := r.Lookup(cID); st == encoding.ColMissing {
				stale++
			}
		}
		_ = pk
	}
	if after == 0 {
		t.Fatal("no writes after the ALTER")
	}
	if stale != 0 {
		t.Fatalf("%d of %d rows written after the ALTER used the stale schema", stale, after)
	}
	if got := queryStrings(t, a, "SELECT COUNT(*), MIN(c), MAX(c) FROM t"); fmt.Sprint(got) != fmt.Sprintf("[%d,7,7]", next.Load()) {
		t.Fatalf("got %v (every row reads c = 7, old ones through the original default)", got)
	}
}

// Slice 8 done-when, part 1: CREATE INDEX on one engine while another writes.
func TestCreateIndexWhileAnotherEngineWrites(t *testing.T) {
	c := startCluster(t)
	a := c.db(t)
	mustExec(t, a, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
	for i := 0; i < 2000; i += 100 {
		q := "INSERT INTO t VALUES "
		for j := i; j < i+100; j++ {
			if j > i {
				q += ","
			}
			q += fmt.Sprintf("(%d, %d)", j, j%97)
		}
		mustExec(t, a, q)
	}
	engB, b := c.secondEngine(t)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var writes atomic.Int64
	for w := 0; w < 3; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				id := (w*7919 + i*31) % 2500
				var err error
				switch i % 3 {
				case 0:
					err = retry(b, "UPDATE t SET v = v + 1 WHERE id = ?", id)
				case 1:
					err = retry(b, "DELETE FROM t WHERE id = ?", id)
				default:
					err = retry(b, "REPLACE INTO t VALUES (?, ?)", id, i%97)
				}
				if err != nil {
					t.Error(err)
					return
				}
				writes.Add(1)
			}
		}(w)
	}
	time.Sleep(200 * time.Millisecond)
	mustExec(t, a, "CREATE INDEX v_idx ON t (v)")
	time.Sleep(CatalogLease + 500*time.Millisecond)
	close(stop)
	wg.Wait()
	n := checkIndex(t, engB, "t", "v_idx")
	t.Logf("index consistent over %d rows after %d concurrent writes", n, writes.Load())
	if got := queryStrings(t, b, "SELECT COUNT(*) FROM t WHERE v = 5"); len(got) != 1 {
		t.Fatal(got)
	}
}

// Slice 8 done-when, part 2: an index build survives a KV suspend.
func TestIndexBuildSurvivesKVRestart(t *testing.T) {
	c := startCluster(t)
	db := c.db(t)
	mustExec(t, db, "CREATE TABLE big (id INT PRIMARY KEY, v VARCHAR(20))")
	for i := 0; i < 5000; i += 250 {
		q := "INSERT INTO big VALUES "
		for j := i; j < i+250; j++ {
			if j > i {
				q += ","
			}
			q += fmt.Sprintf("(%d, 'v%d')", j, j%300)
		}
		mustExec(t, db, q)
	}
	done := make(chan error, 1)
	go func() { _, err := db.Exec("CREATE INDEX v_idx ON big (v)"); done <- err }()
	// Wait for the backfill to be part-way, then kill the KV owner.
	ti := tableInfo(t, c.engine, "big")
	waitProgress(t, c.engine, ti.ID)
	c.restartKV()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("index build after KV restart: %v", err)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("index build did not finish")
	}
	if n := checkIndex(t, c.engine, "big", "v_idx"); n != 5000 {
		t.Fatalf("rows %d", n)
	}
}

func waitProgress(t *testing.T, e *Engine, table uint32) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		txn := e.Store.NewTxn(kv.ReadCommitted, true)
		vs, err := txn.Get(context.Background(), [][]byte{e.Store.jobKey(table, 1)}, kvrpc.ReadOpts{Stmt: 1})
		txn.Rollback(context.Background())
		if err == nil && vs[0] != nil {
			var j jobState
			json.Unmarshal(vs[0], &j)
			if j.Rows > 0 {
				return
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("no backfill progress")
}

// A build whose query engine died is resumed by another engine.
func TestIndexBuildResumedByAnotherEngine(t *testing.T) {
	c := startCluster(t)
	db := c.db(t)
	mustExec(t, db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
	mustExec(t, db, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
	// Leave the index write-only with a job owned by a dead engine.
	ti := tableInfo(t, c.engine, "t")
	ctx := newCtx()
	err := c.engine.Store.catalog.AlterTable(ctx, ti, func(txn *Txn, nt *TableInfo) error {
		nt.NextIndexID = 2
		nt.Indexes = append(nt.Indexes, IndexInfo{ID: 1, Name: "v_idx", Columns: []IndexColumn{{ColID: 2}}, State: IndexWriteOnly})
		b, _ := json.Marshal(jobState{Owner: "dead", Heartbeat: time.Now().Add(-time.Minute).UnixNano()})
		return txn.Mutate(ctx, 1, []kv.Mutation{{Op: kv.OpPut, Key: c.engine.Store.jobKey(ti.ID, 1), Value: b}}, 0)
	})
	if err != nil {
		t.Fatal(err)
	}
	// It was loaded with the job in place; wait for this or another engine to finish it.
	engB, _ := c.secondEngine(t)
	deadline := time.Now().Add(30 * time.Second)
	for {
		ti := tableInfo(t, engB, "t")
		if len(ti.Indexes) == 1 && ti.Indexes[0].State == "" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the build was not resumed")
		}
		time.Sleep(50 * time.Millisecond)
	}
	checkIndex(t, engB, "t", "v_idx")
}

func TestInstantColumnChanges(t *testing.T) {
	c := startCluster(t)
	db := c.db(t)
	mustExec(t, db, "CREATE TABLE p (id INT PRIMARY KEY, name VARCHAR(10))")
	mustExec(t, db, "INSERT INTO p VALUES (1, 'a'), (2, 'b')")
	mustExec(t, db, "ALTER TABLE p ADD COLUMN score INT DEFAULT 5 AFTER id")
	mustExec(t, db, "ALTER TABLE p ALTER COLUMN score SET DEFAULT 9") // must not change old rows (D3)
	mustExec(t, db, "INSERT INTO p (id, name) VALUES (3, 'c')")
	if got := queryStrings(t, db, "SELECT * FROM p ORDER BY id"); fmt.Sprint(got) != "[1,5,a 2,5,b 3,9,c]" {
		t.Fatalf("after ADD COLUMN: %v", got)
	}
	mustExec(t, db, "ALTER TABLE p ADD COLUMN created DATETIME NULL")
	mustExec(t, db, "ALTER TABLE p RENAME COLUMN name TO label")
	mustExec(t, db, "ALTER TABLE p MODIFY COLUMN label VARCHAR(50)")
	mustExec(t, db, "ALTER TABLE p DROP COLUMN created")
	if got := queryStrings(t, db, "SELECT id, score, label FROM p WHERE label = 'b'"); fmt.Sprint(got) != "[2,5,b]" {
		t.Fatalf("after rename and modify: %v", got)
	}
	mustExec(t, db, "ALTER TABLE p DROP COLUMN score")
	mustExec(t, db, "ALTER TABLE p ADD COLUMN score INT")
	if got := queryStrings(t, db, "SELECT id, label, score FROM p ORDER BY id"); fmt.Sprint(got) != "[1,a,NULL 2,b,NULL 3,c,NULL]" {
		t.Fatalf("a re-added column must not see the dropped one's values: %v", got)
	}
	if _, err := db.Exec("ALTER TABLE p MODIFY COLUMN label INT"); err == nil {
		t.Fatal("a rewriting type change must be refused")
	}
	if _, err := db.Exec("ALTER TABLE p DROP COLUMN id"); err == nil {
		t.Fatal("dropping the primary key must be refused")
	}
}

func TestAnalyzeStoresStatistics(t *testing.T) {
	c := startCluster(t)
	db := c.db(t)
	mustExec(t, db, "CREATE TABLE s (id INT PRIMARY KEY)")
	for i := 0; i < 50; i++ {
		mustExec(t, db, "INSERT INTO s VALUES (?)", i)
	}
	mustExec(t, db, "ANALYZE TABLE s")
	ti := tableInfo(t, c.engine, "s")
	st, ok := c.engine.Store.catalog.Stats(ti.ID)
	if !ok || st.Rows != 50 {
		t.Fatalf("stats %+v %v", st, ok)
	}
	// Stats survive a restart: they live in system table 2.
	c.restartKV()
	c.engine.Store.catalog.Invalidate()
	_ = queryStrings(t, db, "SELECT COUNT(*) FROM s")
	if st, ok := c.engine.Store.catalog.Stats(ti.ID); !ok || st.Rows != 50 {
		t.Fatalf("stats after restart %+v", st)
	}
}
