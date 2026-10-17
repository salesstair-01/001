package sqlengine

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/planbuilder"
	"github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/salesstair-01/001/internal/encoding"
	"github.com/salesstair-01/001/internal/kv"
	"github.com/salesstair-01/001/internal/kvrpc"
)

// The catalog is stored as rows in system table 1 (D2). Table and column ids
// are never reused, so the database name never needs to be in a data key.
//
//	("db", lower(name))                  -> DBInfo
//	("table", table_id)                  -> TableInfo
//	("tname", lower(db) "." lower(name)) -> table_id, so names are unique
//	("seq", "table_id")                  -> next table id
//	("seq", "catalog_version")           -> bumped by every DDL, for lease refresh
//	("schemaver", table_id)              -> the table's schema version (u64), the commit guard
//	("job", table_id, index_id)          -> progress of an index backfill
//
// Table statistics live in system table 2: (table_id) -> TableStats.
type DBInfo struct {
	Name      string `json:"name"`
	Collation string `json:"collation,omitempty"`
}

// ColumnInfo is one column. Type and Default are SQL text, re-parsed on load.
type ColumnInfo struct {
	ID            uint32 `json:"id"`
	Name          string `json:"name"`
	Type          string `json:"type"`
	Nullable      bool   `json:"nullable"`
	Default       string `json:"default,omitempty"`
	OnUpdate      string `json:"on_update,omitempty"` // ON UPDATE expression
	Generated     string `json:"generated,omitempty"` // generated column expression
	Virtual       bool   `json:"virtual,omitempty"`   // VIRTUAL rather than STORED
	AutoIncrement bool   `json:"auto_increment,omitempty"`
	Comment       string `json:"comment,omitempty"`
	// OriginalDefault is the encoded value a row without this column's id
	// means: the default at the moment the column was added (D3). Nil = NULL.
	OriginalDefault []byte `json:"original_default,omitempty"`
}

// IndexColumn is one column of a secondary index; Prefix > 0 is col(N).
type IndexColumn struct {
	ColID  uint32 `json:"col"`
	Prefix int    `json:"prefix,omitempty"`
}

// IndexInfo is a secondary index. Its id (1, 2, ...) is the index field of
// its keys' prefix and is never reused within the table.
type IndexInfo struct {
	ID      uint32        `json:"id"`
	Name    string        `json:"name"`
	Columns []IndexColumn `json:"columns"`
	Unique  bool          `json:"unique,omitempty"`
	Comment string        `json:"comment,omitempty"`
	// State is "" (public), or IndexWriteOnly while it is built or dropped:
	// writers maintain it but readers don't use it (the F1/TiDB states).
	State string `json:"state,omitempty"`
	// Dropping marks a write-only index on its way out; Since is when it
	// stopped being readable (unix nanos).
	Dropping bool  `json:"dropping,omitempty"`
	Since    int64 `json:"since,omitempty"`
}

// IndexWriteOnly is the state of an index being backfilled or dropped.
const IndexWriteOnly = "write_only"

// TableInfo is one table.
type TableInfo struct {
	ID           uint32       `json:"id"`
	DB           string       `json:"db"`
	Name         string       `json:"name"`
	Columns      []ColumnInfo `json:"columns"`
	PK           []uint32     `json:"pk"` // column ids, in key order
	NextColumnID uint32       `json:"next_column_id"`
	Indexes      []IndexInfo  `json:"indexes,omitempty"`
	NextIndexID  uint32       `json:"next_index_id,omitempty"`
	// Version changes with every schema change; writers guard their commit on
	// it, so no write commits with a stale schema (slice 8).
	Version   uint64 `json:"version,omitempty"`
	Collation string `json:"collation,omitempty"`
	Comment   string `json:"comment,omitempty"`
	// ForeignKeys are the constraints this table declares (fk.go).
	ForeignKeys []ForeignKeyInfo `json:"foreign_keys,omitempty"`
}

// clone deep-copies a table so the cached original is never mutated.
func (t *TableInfo) clone() *TableInfo {
	b, _ := json.Marshal(t)
	var out TableInfo
	_ = json.Unmarshal(b, &out)
	return &out
}

// The five collations v1 supports; all others are rejected (D2).
var supportedCollations = map[sql.CollationID]bool{
	sql.Collation_utf8mb4_0900_ai_ci: true,
	sql.Collation_utf8mb4_general_ci: true,
	sql.Collation_utf8mb4_0900_bin:   true,
	sql.Collation_utf8mb4_bin:        true,
	sql.Collation_binary:             true,
}

// collationSupported checks by name: go-mysql-server has several ids for
// one collation (such as the unspecified default).
func collationSupported(c sql.CollationID) bool {
	for id := range supportedCollations {
		if id.Name() == c.Name() {
			return true
		}
	}
	return false
}

func (s *Store) catalogPrefix() []byte {
	return encoding.Prefix(s.partition, encoding.TableCatalog, 0)
}

func (s *Store) dbKey(name string) []byte {
	k := encoding.AppendBytes(s.catalogPrefix(), []byte("db"), false)
	return encoding.AppendBytes(k, []byte(strings.ToLower(name)), false)
}

func (s *Store) tableKey(id uint32) []byte {
	k := encoding.AppendBytes(s.catalogPrefix(), []byte("table"), false)
	return encoding.AppendUint(k, uint64(id), false)
}

func (s *Store) tableNameKey(db, name string) []byte {
	k := encoding.AppendBytes(s.catalogPrefix(), []byte("tname"), false)
	return encoding.AppendBytes(k, []byte(strings.ToLower(db)+"."+strings.ToLower(name)), false)
}

func (s *Store) schemaVerKey(table uint32) []byte {
	k := encoding.AppendBytes(s.catalogPrefix(), []byte("schemaver"), false)
	return encoding.AppendUint(k, uint64(table), false)
}

func (s *Store) jobKey(table, index uint32) []byte {
	k := encoding.AppendBytes(s.catalogPrefix(), []byte("job"), false)
	k = encoding.AppendUint(k, uint64(table), false)
	return encoding.AppendUint(k, uint64(index), false)
}

func (s *Store) statsPrefix() []byte {
	return encoding.Prefix(s.partition, encoding.TableStatistics, 0)
}

func (s *Store) statsKey(table uint32) []byte {
	return encoding.AppendUint(s.statsPrefix(), uint64(table), false)
}

func versionBytes(v uint64) []byte { return binary.BigEndian.AppendUint64(nil, v) }

func (s *Store) seqKey(name string) []byte {
	k := encoding.AppendBytes(s.catalogPrefix(), []byte("seq"), false)
	return encoding.AppendBytes(k, []byte(name), false)
}

// CatalogLease is how long a query engine trusts its cached catalog before
// checking the catalog version again. Several query engines can serve one
// tenant: writes are exact (commit guards on the table's schema version),
// and reads see a schema at most one lease old.
var CatalogLease = 2 * time.Second

// TableStats are a table's statistics (system table 2).
type TableStats struct {
	Rows     uint64 `json:"rows"`
	Bytes    uint64 `json:"bytes"`
	Analyzed int64  `json:"analyzed"` // unix seconds
}

// Catalog caches the catalog rows under a lease.
type Catalog struct {
	store *Store

	mu      sync.RWMutex
	loaded  bool
	version uint64             // catalog version the cache reflects
	checked time.Time          // when the version was last confirmed
	dbs     map[string]*DBInfo // lower-case name
	tables  map[uint32]*TableInfo
	views   map[string]*ViewInfo // "db.name", lower-case
	stats   map[uint32]TableStats
}

func NewCatalog(s *Store) *Catalog {
	return &Catalog{store: s}
}

// load reads the whole catalog in its own transaction.
func (c *Catalog) load(ctx context.Context) error {
	txn := c.store.NewTxn(kv.ReadCommitted, true)
	defer txn.Rollback(ctx)
	dbs := map[string]*DBInfo{}
	tables := map[uint32]*TableInfo{}
	views := map[string]*ViewInfo{}
	var version uint64
	start, end := c.store.catalogPrefix(), encoding.PrefixEnd(c.store.catalogPrefix())
	for {
		resp, err := txn.Scan(ctx, start, end, 1000, kvrpc.ReadOpts{Stmt: 1})
		if err != nil {
			return err
		}
		for i, k := range resp.Keys {
			kind, _, err := encoding.DecodeBytes(k[encoding.PrefixLen:], false)
			if err != nil {
				return err
			}
			switch string(kind) {
			case "db":
				var d DBInfo
				if err := json.Unmarshal(resp.Values[i], &d); err != nil {
					return err
				}
				dbs[strings.ToLower(d.Name)] = &d
			case "table":
				var t TableInfo
				if err := json.Unmarshal(resp.Values[i], &t); err != nil {
					return err
				}
				tables[t.ID] = &t
			case "view":
				v, err := decodeView(resp.Values[i])
				if err != nil {
					return err
				}
				views[viewMapKey(v.DB, v.Name)] = v
			case "seq":
				if bytes.Equal(k, c.store.seqKey("catalog_version")) {
					_ = json.Unmarshal(resp.Values[i], &version)
				}
			}
		}
		if len(resp.Resume) == 0 {
			break
		}
		start = resp.Resume
	}
	stats := map[uint32]TableStats{}
	resp, err := txn.Scan(ctx, c.store.statsPrefix(), encoding.PrefixEnd(c.store.statsPrefix()), 0, kvrpc.ReadOpts{Stmt: 1})
	if err != nil {
		return err
	}
	for i, k := range resp.Keys {
		id, _, err := encoding.DecodeUint(k[encoding.PrefixLen:], false)
		var st TableStats
		if err == nil && json.Unmarshal(resp.Values[i], &st) == nil {
			stats[uint32(id)] = st
		}
	}
	c.mu.Lock()
	c.dbs, c.tables, c.views, c.stats, c.version, c.checked, c.loaded = dbs, tables, views, stats, version, time.Now(), true
	c.mu.Unlock()
	c.store.resumeJobs(tables)
	return nil
}

// ensure loads the catalog, or once the lease has run out, checks the
// catalog version and reloads if another query engine changed the schema.
func (c *Catalog) ensure(ctx context.Context) error {
	c.mu.RLock()
	loaded, fresh, version := c.loaded, time.Since(c.checked) < CatalogLease, c.version
	c.mu.RUnlock()
	if loaded && fresh {
		return nil
	}
	if loaded {
		txn := c.store.NewTxn(kv.ReadCommitted, true)
		vs, err := txn.Get(ctx, [][]byte{c.store.seqKey("catalog_version")}, kvrpc.ReadOpts{Stmt: 1})
		_ = txn.Rollback(ctx)
		if err != nil {
			return err
		}
		var cur uint64
		if vs[0] != nil {
			_ = json.Unmarshal(vs[0], &cur)
		}
		if cur == version {
			c.mu.Lock()
			c.checked = time.Now()
			c.mu.Unlock()
			return nil
		}
	}
	return c.load(ctx)
}

// Invalidate makes the next use reload the catalog (after a commit failed
// on a schema guard).
func (c *Catalog) Invalidate() {
	c.mu.Lock()
	c.loaded = false
	c.mu.Unlock()
}

// Stats returns a table's statistics, if it has been analyzed.
func (c *Catalog) Stats(id uint32) (TableStats, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	st, ok := c.stats[id]
	return st, ok
}

// Database returns a database by name (case-insensitive).
func (c *Catalog) Database(ctx context.Context, name string) (*DBInfo, error) {
	if err := c.ensure(ctx); err != nil {
		return nil, err
	}
	c.mu.RLock()
	d := c.dbs[strings.ToLower(name)]
	c.mu.RUnlock()
	return d, nil
}

// Databases returns every database, sorted by name.
func (c *Catalog) Databases(ctx context.Context) ([]*DBInfo, error) {
	if err := c.ensure(ctx); err != nil {
		return nil, err
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]*DBInfo, 0, len(c.dbs))
	for _, d := range c.dbs {
		out = append(out, d)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// Tables returns the tables of a database, sorted by name.
func (c *Catalog) Tables(ctx context.Context, db string) ([]*TableInfo, error) {
	if err := c.ensure(ctx); err != nil {
		return nil, err
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	var out []*TableInfo
	for _, t := range c.tables {
		if strings.EqualFold(t.DB, db) {
			out = append(out, t)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// Table finds a table by database and name (case-insensitive).
func (c *Catalog) Table(ctx context.Context, db, name string) (*TableInfo, error) {
	ts, err := c.Tables(ctx, db)
	if err != nil {
		return nil, err
	}
	for _, t := range ts {
		if strings.EqualFold(t.Name, name) {
			return t, nil
		}
	}
	return nil, nil
}

// ddl runs f in its own transaction and commits it, the way MySQL DDL is
// atomic and commits on its own. On a write conflict it reloads and retries.
func (c *Catalog) ddl(ctx context.Context, f func(txn *Txn) error) error {
	var err error
	for attempt := 0; attempt < 5; attempt++ {
		txn := c.store.NewTxn(kv.RepeatableRead, false)
		if err = f(txn); err == nil {
			err = c.bumpVersion(ctx, txn)
		}
		if err == nil {
			err = txn.Commit(ctx)
		} else {
			_ = txn.Rollback(ctx)
		}
		if !errIsKind(err, kv.KindDeadlock) && !errIsKind(err, kv.KindLockTimeout) && !errIsKind(err, kv.KindSchemaChanged) {
			break
		}
		if lerr := c.load(ctx); lerr != nil {
			return lerr
		}
	}
	if err != nil {
		_ = c.load(ctx) // the cache may be stale after a failure
		return err
	}
	return c.load(ctx)
}

// bumpVersion increments the catalog version; its lock also serialises DDL.
func (c *Catalog) bumpVersion(ctx context.Context, txn *Txn) error {
	key := c.store.seqKey("catalog_version")
	vs, err := txn.Get(ctx, [][]byte{key}, kvrpc.ReadOpts{Stmt: 1, IncludeStmt: true, Lock: kv.LockExclusive})
	if err != nil {
		return err
	}
	var v uint64
	if vs[0] != nil {
		_ = json.Unmarshal(vs[0], &v)
	}
	return put(txn, ctx, kv.OpPut, key, v+1)
}

// setSchemaVersion writes a table's commit-guard version in txn.
func (c *Catalog) setSchemaVersion(ctx context.Context, txn *Txn, t *TableInfo) error {
	return txn.Mutate(ctx, 1, []kv.Mutation{{Op: kv.OpPut, Key: c.store.schemaVerKey(t.ID), Value: versionBytes(t.Version)}}, 0)
}

func put(txn *Txn, ctx context.Context, op kv.MutationOp, key []byte, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return txn.Mutate(ctx, 1, []kv.Mutation{{Op: op, Key: key, Value: b}}, 0)
}

// CreateDatabase adds a database.
func (c *Catalog) CreateDatabase(ctx context.Context, name string, collation sql.CollationID) error {
	return c.ddl(ctx, func(txn *Txn) error {
		err := put(txn, ctx, kv.OpInsertOnly, c.store.dbKey(name), &DBInfo{Name: name, Collation: collation.Name()})
		if errIsKind(err, kv.KindDuplicateKey) {
			return sql.ErrDatabaseExists.New(name)
		}
		return err
	})
}

// DropDatabase removes a database and its tables from the catalog. Table data
// is left behind; its table ids are never reused.
func (c *Catalog) DropDatabase(ctx context.Context, name string) error {
	tables, err := c.Tables(ctx, name)
	if err != nil {
		return err
	}
	views, err := c.Views(ctx, name)
	if err != nil {
		return err
	}
	return c.ddl(ctx, func(txn *Txn) error {
		muts := []kv.Mutation{{Op: kv.OpDelete, Key: c.store.dbKey(name)}}
		for _, v := range views {
			muts = append(muts, kv.Mutation{Op: kv.OpDelete, Key: c.store.viewKey(v.DB, v.Name)})
		}
		for _, t := range tables {
			muts = append(muts,
				kv.Mutation{Op: kv.OpDelete, Key: c.store.tableKey(t.ID)},
				kv.Mutation{Op: kv.OpDelete, Key: c.store.tableNameKey(t.DB, t.Name)},
				kv.Mutation{Op: kv.OpDelete, Key: c.store.schemaVerKey(t.ID)})
		}
		return txn.Mutate(ctx, 1, muts, 0)
	})
}

// nextTableID allocates a table id inside txn. The locking read serialises
// concurrent allocations, so an id is never handed out twice.
func (c *Catalog) nextTableID(ctx context.Context, txn *Txn) (uint32, error) {
	vs, err := txn.Get(ctx, [][]byte{c.store.seqKey("table_id")}, kvrpc.ReadOpts{Stmt: 1, IncludeStmt: true, Lock: kv.LockExclusive})
	if err != nil {
		return 0, err
	}
	id := encoding.FirstUserTableID
	if vs[0] != nil {
		if err := json.Unmarshal(vs[0], &id); err != nil {
			return 0, err
		}
	}
	if err := put(txn, ctx, kv.OpPut, c.store.seqKey("table_id"), id+1); err != nil {
		return 0, err
	}
	return id, nil
}

// CreateTable allocates a table id and stores the table.
func (c *Catalog) CreateTable(ctx context.Context, t *TableInfo) error {
	return c.ddl(ctx, func(txn *Txn) error {
		if existing, err := c.Table(ctx, t.DB, t.Name); err != nil {
			return err
		} else if existing != nil {
			return sql.ErrTableAlreadyExists.New(t.Name)
		}
		if v, err := c.View(ctx, t.DB, t.Name); err != nil {
			return err
		} else if v != nil {
			return sql.ErrTableAlreadyExists.New(t.Name)
		}
		id, err := c.nextTableID(ctx, txn)
		if err != nil {
			return err
		}
		t.ID, t.Version = id, 1
		if err := c.setSchemaVersion(ctx, txn, t); err != nil {
			return err
		}
		err = put(txn, ctx, kv.OpInsertOnly, c.store.tableNameKey(t.DB, t.Name), id)
		if errIsKind(err, kv.KindDuplicateKey) {
			return sql.ErrTableAlreadyExists.New(t.Name)
		}
		if err != nil {
			return err
		}
		return put(txn, ctx, kv.OpInsertOnly, c.store.tableKey(id), t)
	})
}

// RenameTable renames a table, keeping its id.
func (c *Catalog) RenameTable(ctx context.Context, t *TableInfo, newName string) error {
	return c.ddl(ctx, func(txn *Txn) error {
		updated := *t.clone()
		updated.Name = newName
		updated.Version++
		if err := c.setSchemaVersion(ctx, txn, &updated); err != nil {
			return err
		}
		if !strings.EqualFold(t.Name, newName) {
			err := put(txn, ctx, kv.OpInsertOnly, c.store.tableNameKey(t.DB, newName), t.ID)
			if errIsKind(err, kv.KindDuplicateKey) {
				return sql.ErrTableAlreadyExists.New(newName)
			}
			if err != nil {
				return err
			}
			if err := txn.Mutate(ctx, 1, []kv.Mutation{{Op: kv.OpDelete, Key: c.store.tableNameKey(t.DB, t.Name)}}, 0); err != nil {
				return err
			}
		}
		return put(txn, ctx, kv.OpPut, c.store.tableKey(t.ID), &updated)
	})
}

// AlterTable rewrites a table's catalog row in one DDL transaction; f
// changes the copy it is given and may write other rows (an index backfill).
func (c *Catalog) AlterTable(ctx context.Context, t *TableInfo, f func(txn *Txn, t *TableInfo) error) error {
	return c.ddl(ctx, func(txn *Txn) error {
		// Start from the latest row, read under an exclusive lock: the caller's
		// copy may be stale (go-mysql-server creates a new table's indexes one
		// by one on the same table object).
		vs, err := txn.Get(ctx, [][]byte{c.store.tableKey(t.ID)}, kvrpc.ReadOpts{Stmt: 1, IncludeStmt: true, Lock: kv.LockExclusive})
		if err != nil {
			return err
		}
		if vs[0] == nil {
			return sql.ErrTableNotFound.New(t.Name)
		}
		var updated TableInfo
		if err := json.Unmarshal(vs[0], &updated); err != nil {
			return err
		}
		if err := f(txn, &updated); err != nil {
			return err
		}
		updated.Version++
		if err := c.setSchemaVersion(ctx, txn, &updated); err != nil {
			return err
		}
		return put(txn, ctx, kv.OpPut, c.store.tableKey(t.ID), &updated)
	})
}

// DropTable removes a table from the catalog.
func (c *Catalog) DropTable(ctx context.Context, t *TableInfo) error {
	return c.ddl(ctx, func(txn *Txn) error {
		return txn.Mutate(ctx, 1, []kv.Mutation{
			{Op: kv.OpDelete, Key: c.store.tableKey(t.ID)},
			{Op: kv.OpDelete, Key: c.store.tableNameKey(t.DB, t.Name)},
			{Op: kv.OpDelete, Key: c.store.schemaVerKey(t.ID)}, // stale writers' guards now fail
		}, 0)
	})
}

// ---- schema conversion -------------------------------------------------------

// columnInfo converts a go-mysql-server column for storage.
func columnInfo(col *sql.Column, id uint32) (ColumnInfo, error) {
	if tc, ok := col.Type.(sql.TypeWithCollation); ok && !collationSupported(tc.Collation()) {
		return ColumnInfo{}, fmt.Errorf("collation %s is not supported; v1 supports utf8mb4_0900_ai_ci, utf8mb4_general_ci, utf8mb4_0900_bin, utf8mb4_bin and binary", tc.Collation().Name())
	}
	typ := col.Type.String()
	if tc, ok := col.Type.(sql.TypeWithCollation); ok {
		// String() leaves out go-mysql-server's default collation
		// (utf8mb4_0900_bin), which would read back as unspecified.
		typ = tc.StringWithTableCollation(sql.Collation_Unspecified)
	}
	ci := ColumnInfo{
		ID: id, Name: col.Name, Type: typ, Nullable: col.Nullable,
		AutoIncrement: col.AutoIncrement, Comment: col.Comment,
	}
	if col.OnUpdate != nil {
		ci.OnUpdate = col.OnUpdate.String()
	}
	if col.Generated != nil {
		ci.Generated, ci.Virtual = col.Generated.String(), col.Virtual
	}
	if col.Default != nil {
		ci.Default = col.Default.String()
	}
	return ci, nil
}

// functionCatalog resolves function names (now(), CURRENT_TIMESTAMP) in
// stored column expressions; it has no databases.
var functionCatalog = analyzer.NewCatalog(NewProvider(nil))

// parseColumnExpr parses a stored DEFAULT, ON UPDATE or generated
// expression. It is planbuilder.StringToColumnDefaultValue, except that the
// second parse gets the original text: go-mysql-server 0.20 formats the
// parsed AST with %s there, which is not SQL for function calls
// (CURRENT_TIMESTAMP, now()) and fails to parse.
func parseColumnExpr(ctx *sql.Context, text string) (*sql.ColumnDefaultValue, error) {
	stmt, err := sqlparser.Parse("SELECT " + text)
	if err != nil {
		return nil, err
	}
	sel, ok := stmt.(*sqlparser.Select)
	if !ok || len(sel.SelectExprs) != 1 {
		return nil, fmt.Errorf("not a single expression")
	}
	ae, ok := sel.SelectExprs[0].(*sqlparser.AliasedExpr)
	if !ok {
		return nil, fmt.Errorf("not an expression")
	}
	proj, _, err := planbuilder.Parse(ctx, functionCatalog, "SELECT "+text)
	if err != nil {
		return nil, err
	}
	p, ok := proj.(*plan.Project)
	if !ok || len(p.Projections) != 1 {
		return nil, fmt.Errorf("not a single expression")
	}
	e := p.Projections[0]
	if a, ok := e.(*expression.Alias); ok {
		e = a.Child
	}
	_, paren := ae.Expr.(*sqlparser.ParenExpr)
	var literal bool
	switch x := e.(type) {
	case *expression.UnaryMinus:
		_, literal = x.Child.(*expression.Literal)
	case *expression.UnresolvedFunction:
	default:
		literal = len(e.Children()) == 0 && !strings.HasPrefix(text, "(")
	}
	return planbuilder.ExpressionToColumnDefaultValue(e, literal, paren), nil
}

// sqlColumn converts a stored column back.
func sqlColumn(ctx *sql.Context, t *TableInfo, ci *ColumnInfo, pk bool) (*sql.Column, error) {
	typ, err := planbuilder.ParseColumnTypeString(ci.Type)
	if err != nil {
		return nil, fmt.Errorf("column %s.%s: type %q: %w", t.Name, ci.Name, ci.Type, err)
	}
	if tc, ok := typ.(sql.TypeWithCollation); ok && tc.Collation() == sql.Collation_Unspecified {
		// Stored without a collation: it was go-mysql-server's default. An
		// unspecified collation makes LIKE false and COLLATE fail.
		if typ, err = tc.WithNewCollation(sql.Collation_Default); err != nil {
			return nil, fmt.Errorf("column %s.%s: %w", t.Name, ci.Name, err)
		}
	}
	col := &sql.Column{
		Name: ci.Name, Type: typ, Nullable: ci.Nullable, Source: t.Name, DatabaseSource: t.DB,
		PrimaryKey: pk, AutoIncrement: ci.AutoIncrement, Comment: ci.Comment,
	}
	for _, d := range []struct {
		text string
		dst  **sql.ColumnDefaultValue
		what string
	}{{ci.Default, &col.Default, "default"}, {ci.OnUpdate, &col.OnUpdate, "on update"}, {ci.Generated, &col.Generated, "generated"}} {
		if d.text == "" {
			continue
		}
		if d.dst == &col.Generated {
			// It names other columns: go-mysql-server resolves it when it
			// plans each statement, against the table.
			*d.dst = sql.NewUnresolvedColumnDefaultValue(d.text)
			continue
		}
		def, err := parseColumnExpr(ctx, d.text)
		if err == nil {
			def, err = sql.NewColumnDefaultValue(def.Expr, typ, def.IsLiteral(), def.IsParenthesized(), ci.Nullable)
		}
		if err != nil {
			*d.dst = sql.NewUnresolvedColumnDefaultValue(d.text)
			continue
		}
		*d.dst = def
	}
	col.Virtual = ci.Virtual
	return col, nil
}
