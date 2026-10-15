package sqlengine

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/planbuilder"

	"github.com/salesstair-01/001/internal/encoding"
	"github.com/salesstair-01/001/internal/kv"
)

// The catalog is stored as rows in system table 1 (D2). Table and column ids
// are never reused, so the database name never needs to be in a data key.
//
//	("db", lower(name))       -> DBInfo
//	("table", table_id)       -> TableInfo
//	("seq", "table_id")       -> next table id
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
	AutoIncrement bool   `json:"auto_increment,omitempty"`
	Comment       string `json:"comment,omitempty"`
	// OriginalDefault is the encoded value a row without this column's id
	// means: the default at the moment the column was added (D3). Nil = NULL.
	OriginalDefault []byte `json:"original_default,omitempty"`
}

// TableInfo is one table.
type TableInfo struct {
	ID           uint32       `json:"id"`
	DB           string       `json:"db"`
	Name         string       `json:"name"`
	Columns      []ColumnInfo `json:"columns"`
	PK           []uint32     `json:"pk"` // column ids, in key order
	NextColumnID uint32       `json:"next_column_id"`
	Collation    string       `json:"collation,omitempty"`
	Comment      string       `json:"comment,omitempty"`
}

// The five collations v1 supports; all others are rejected (D2).
var supportedCollations = map[sql.CollationID]bool{
	sql.Collation_utf8mb4_0900_ai_ci: true,
	sql.Collation_utf8mb4_general_ci: true,
	sql.Collation_utf8mb4_0900_bin:   true,
	sql.Collation_utf8mb4_bin:        true,
	sql.Collation_binary:             true,
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

func (s *Store) seqKey(name string) []byte {
	k := encoding.AppendBytes(s.catalogPrefix(), []byte("seq"), false)
	return encoding.AppendBytes(k, []byte(name), false)
}

// Catalog caches the catalog rows. With one query engine per tenant this is
// exact; schema leases across query engines are slice 8.
type Catalog struct {
	store *Store

	mu     sync.RWMutex
	loaded bool
	dbs    map[string]*DBInfo // lower-case name
	tables map[uint32]*TableInfo
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
	start, end := c.store.catalogPrefix(), encoding.PrefixEnd(c.store.catalogPrefix())
	for {
		resp, err := txn.Scan(ctx, 1, start, end, 1000)
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
			}
		}
		if !resp.More || len(resp.Keys) == 0 {
			break
		}
		start = append(append([]byte(nil), resp.Keys[len(resp.Keys)-1]...), 0)
	}
	c.mu.Lock()
	c.dbs, c.tables, c.loaded = dbs, tables, true
	c.mu.Unlock()
	return nil
}

func (c *Catalog) ensure(ctx context.Context) error {
	c.mu.RLock()
	loaded := c.loaded
	c.mu.RUnlock()
	if loaded {
		return nil
	}
	return c.load(ctx)
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
			err = txn.Commit(ctx)
		} else {
			_ = txn.Rollback(ctx)
		}
		if !errIsKind(err, kv.KindConflict) && !errIsKind(err, kv.KindDuplicateKey) {
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

func put(txn *Txn, ctx context.Context, op kv.MutationOp, key []byte, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return txn.Mutate(ctx, 1, []kv.Mutation{{Op: op, Key: key, Value: b}})
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
	return c.ddl(ctx, func(txn *Txn) error {
		muts := []kv.Mutation{{Op: kv.OpDelete, Key: c.store.dbKey(name)}}
		for _, t := range tables {
			muts = append(muts, kv.Mutation{Op: kv.OpDelete, Key: c.store.tableKey(t.ID)})
		}
		return txn.Mutate(ctx, 1, muts)
	})
}

// nextTableID allocates a table id inside txn.
func (c *Catalog) nextTableID(ctx context.Context, txn *Txn) (uint32, error) {
	vs, err := txn.Get(ctx, 1, [][]byte{c.store.seqKey("table_id")}, true)
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
		id, err := c.nextTableID(ctx, txn)
		if err != nil {
			return err
		}
		t.ID = id
		return put(txn, ctx, kv.OpInsertOnly, c.store.tableKey(id), t)
	})
}

// UpdateTable rewrites a table's catalog row (rename, ALTER).
func (c *Catalog) UpdateTable(ctx context.Context, t *TableInfo) error {
	return c.ddl(ctx, func(txn *Txn) error {
		return put(txn, ctx, kv.OpPut, c.store.tableKey(t.ID), t)
	})
}

// DropTable removes a table from the catalog.
func (c *Catalog) DropTable(ctx context.Context, t *TableInfo) error {
	return c.ddl(ctx, func(txn *Txn) error {
		return txn.Mutate(ctx, 1, []kv.Mutation{{Op: kv.OpDelete, Key: c.store.tableKey(t.ID)}})
	})
}

// ---- schema conversion -------------------------------------------------------

// columnInfo converts a go-mysql-server column for storage.
func columnInfo(col *sql.Column, id uint32) (ColumnInfo, error) {
	if tc, ok := col.Type.(sql.TypeWithCollation); ok && !supportedCollations[tc.Collation()] {
		return ColumnInfo{}, fmt.Errorf("collation %s is not supported; v1 supports utf8mb4_0900_ai_ci, utf8mb4_general_ci, utf8mb4_0900_bin, utf8mb4_bin and binary", tc.Collation().Name())
	}
	ci := ColumnInfo{
		ID: id, Name: col.Name, Type: col.Type.String(), Nullable: col.Nullable,
		AutoIncrement: col.AutoIncrement, Comment: col.Comment,
	}
	if col.Default != nil {
		ci.Default = col.Default.String()
	}
	return ci, nil
}

// sqlColumn converts a stored column back.
func sqlColumn(ctx *sql.Context, t *TableInfo, ci *ColumnInfo, pk bool) (*sql.Column, error) {
	typ, err := planbuilder.ParseColumnTypeString(ci.Type)
	if err != nil {
		return nil, fmt.Errorf("column %s.%s: type %q: %w", t.Name, ci.Name, ci.Type, err)
	}
	col := &sql.Column{
		Name: ci.Name, Type: typ, Nullable: ci.Nullable, Source: t.Name, DatabaseSource: t.DB,
		PrimaryKey: pk, AutoIncrement: ci.AutoIncrement, Comment: ci.Comment,
	}
	if ci.Default != "" {
		def, err := planbuilder.StringToColumnDefaultValue(ctx, ci.Default)
		if err == nil {
			def, err = sql.NewColumnDefaultValue(def.Expr, typ, def.IsLiteral(), def.IsParenthesized(), ci.Nullable)
		}
		if err != nil {
			return nil, fmt.Errorf("column %s.%s: default %q: %w", t.Name, ci.Name, ci.Default, err)
		}
		col.Default = def
	}
	return col, nil
}
