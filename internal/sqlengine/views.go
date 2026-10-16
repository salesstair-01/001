package sqlengine

import (
	"context"
	"encoding/json"
	"sort"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/salesstair-01/001/internal/encoding"
	"github.com/salesstair-01/001/internal/kv"
	"github.com/salesstair-01/001/internal/kvrpc"
)

// ViewInfo is a view: catalog row ("view", "db.name") -> ViewInfo.
type ViewInfo struct {
	DB     string `json:"db"`
	Name   string `json:"name"`
	Select string `json:"select"` // the SELECT text
	Create string `json:"create"` // the CREATE VIEW statement
}

func (s *Store) viewKey(db, name string) []byte {
	k := encoding.AppendBytes(s.catalogPrefix(), []byte("view"), false)
	return encoding.AppendBytes(k, []byte(strings.ToLower(db)+"."+strings.ToLower(name)), false)
}

func viewMapKey(db, name string) string { return strings.ToLower(db) + "." + strings.ToLower(name) }

// Views returns the views of a database, sorted by name.
func (c *Catalog) Views(ctx context.Context, db string) ([]*ViewInfo, error) {
	if err := c.ensure(ctx); err != nil {
		return nil, err
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	var out []*ViewInfo
	for _, v := range c.views {
		if strings.EqualFold(v.DB, db) {
			out = append(out, v)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// View finds a view by database and name (case-insensitive).
func (c *Catalog) View(ctx context.Context, db, name string) (*ViewInfo, error) {
	if err := c.ensure(ctx); err != nil {
		return nil, err
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.views[viewMapKey(db, name)], nil
}

// CreateView stores a view; tables and views share one namespace.
func (c *Catalog) CreateView(ctx context.Context, v *ViewInfo) error {
	return c.ddl(ctx, func(txn *Txn) error {
		if t, err := c.Table(ctx, v.DB, v.Name); err != nil {
			return err
		} else if t != nil {
			return sql.ErrTableAlreadyExists.New(v.Name)
		}
		err := put(txn, ctx, kv.OpInsertOnly, c.store.viewKey(v.DB, v.Name), v)
		if errIsKind(err, kv.KindDuplicateKey) {
			return sql.ErrExistingView.New(v.DB, v.Name)
		}
		return err
	})
}

// DropView removes a view.
func (c *Catalog) DropView(ctx context.Context, db, name string) error {
	return c.ddl(ctx, func(txn *Txn) error {
		key := c.store.viewKey(db, name)
		vs, err := txn.Get(ctx, [][]byte{key}, kvrpc.ReadOpts{Stmt: 1, IncludeStmt: true, Lock: kv.LockExclusive})
		if err != nil {
			return err
		}
		if vs[0] == nil {
			return sql.ErrViewDoesNotExist.New(db, name)
		}
		return txn.Mutate(ctx, 1, []kv.Mutation{{Op: kv.OpDelete, Key: key}}, 0)
	})
}

func decodeView(b []byte) (*ViewInfo, error) {
	var v ViewInfo
	if err := json.Unmarshal(b, &v); err != nil {
		return nil, err
	}
	return &v, nil
}

// ---- sql.ViewDatabase -------------------------------------------------------------

var _ sql.ViewDatabase = (*Database)(nil)

func (d *Database) CreateView(ctx *sql.Context, name, selectStatement, createViewStmt string) error {
	return d.cat.CreateView(ctx, &ViewInfo{DB: d.name, Name: name, Select: selectStatement, Create: qualifyView(createViewStmt, d.name)})
}

// qualifyView names the view's database in its CREATE VIEW statement, which
// go-mysql-server re-parses on every use: sessions with another current
// database (or none) bind it the same way.
func qualifyView(create, db string) string {
	stmt, err := sqlparser.Parse(create)
	if err != nil {
		return create
	}
	ddl, ok := stmt.(*sqlparser.DDL)
	if !ok || ddl.ViewSpec == nil || !ddl.ViewSpec.ViewName.DbQualifier.IsEmpty() {
		return create
	}
	ddl.ViewSpec.ViewName.DbQualifier = sqlparser.NewTableIdent(db)
	return sqlparser.String(ddl)
}

func (d *Database) DropView(ctx *sql.Context, name string) error {
	return d.cat.DropView(ctx, d.name, name)
}

func (d *Database) GetViewDefinition(ctx *sql.Context, name string) (sql.ViewDefinition, bool, error) {
	v, err := d.cat.View(ctx, d.name, name)
	if err != nil || v == nil {
		return sql.ViewDefinition{}, false, err
	}
	if s, ok := ctx.Session.(*Session); ok && ctx.GetCurrentDatabase() == "" {
		// go-mysql-server 0.20 binds a stored view only with a current
		// database; lend it the view's until the command ends.
		s.lendDatabase(ctx, d.name)
	}
	return viewDefinition(v), true, nil
}

func (d *Database) AllViews(ctx *sql.Context) ([]sql.ViewDefinition, error) {
	vs, err := d.cat.Views(ctx, d.name)
	if err != nil {
		return nil, err
	}
	out := make([]sql.ViewDefinition, len(vs))
	for i, v := range vs {
		out[i] = viewDefinition(v)
	}
	return out, nil
}

func viewDefinition(v *ViewInfo) sql.ViewDefinition {
	return sql.ViewDefinition{Name: v.Name, TextDefinition: v.Select, CreateViewStatement: v.Create}
}

// ---- TRUNCATE ---------------------------------------------------------------------

var _ sql.TruncateableTable = (*Table)(nil)

// Truncate empties the table at once, as InnoDB does, by giving it a new
// table id: the old rows are left behind like a dropped table's, and the
// AUTO_INCREMENT counter (keyed by table id) starts over.
func (t *Table) Truncate(ctx *sql.Context) (int, error) {
	return 0, t.cat.TruncateTable(ctx, t.info)
}

// TruncateTable moves a table to a new id in one DDL transaction.
func (c *Catalog) TruncateTable(ctx context.Context, t *TableInfo) error {
	return c.ddl(ctx, func(txn *Txn) error {
		vs, err := txn.Get(ctx, [][]byte{c.store.tableKey(t.ID)}, kvrpc.ReadOpts{Stmt: 1, IncludeStmt: true, Lock: kv.LockExclusive})
		if err != nil {
			return err
		}
		if vs[0] == nil {
			return sql.ErrTableNotFound.New(t.Name)
		}
		var cur TableInfo
		if err := json.Unmarshal(vs[0], &cur); err != nil {
			return err
		}
		for _, ix := range cur.Indexes {
			if ix.State != "" {
				return sql.ErrInvalidArgument.New("TRUNCATE while index " + ix.Name + " is being built or dropped")
			}
		}
		id, err := c.nextTableID(ctx, txn)
		if err != nil {
			return err
		}
		old := cur.ID
		cur.ID = id
		cur.Version++
		if err := txn.Mutate(ctx, 1, []kv.Mutation{
			{Op: kv.OpDelete, Key: c.store.tableKey(old)},
			{Op: kv.OpDelete, Key: c.store.schemaVerKey(old)}, // stale writers' guards now fail
			{Op: kv.OpDelete, Key: c.store.statsKey(old)},
		}, 0); err != nil {
			return err
		}
		if err := c.setSchemaVersion(ctx, txn, &cur); err != nil {
			return err
		}
		if err := put(txn, ctx, kv.OpPut, c.store.tableNameKey(cur.DB, cur.Name), id); err != nil {
			return err
		}
		return put(txn, ctx, kv.OpInsertOnly, c.store.tableKey(id), &cur)
	})
}
