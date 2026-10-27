package sqlengine

import (
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/salesstair-01/001/internal/kv"
)

// Foreign keys are declared on the child table's catalog row and enforced
// by go-mysql-server, which checks the parent on child writes and runs the
// ON DELETE / ON UPDATE actions through each table's ForeignKeyEditor. Its
// reference checks read with a shared row lock (and see the statement's own
// writes), as InnoDB's do: a parent row being checked cannot be deleted by
// another transaction until this one ends, so no orphan can commit.

// ForeignKeyInfo is one declared foreign key.
type ForeignKeyInfo struct {
	Name          string   `json:"name"`
	Columns       []string `json:"columns"`
	ParentDB      string   `json:"parent_db"`
	ParentTable   string   `json:"parent_table"`
	ParentColumns []string `json:"parent_columns"`
	OnUpdate      string   `json:"on_update,omitempty"`
	OnDelete      string   `json:"on_delete,omitempty"`
	Resolved      bool     `json:"resolved,omitempty"`
}

var (
	_ sql.ForeignKeyTable  = (*Table)(nil)
	_ sql.ReferenceChecker = (*indexedTable)(nil)
)

func fkConstraint(t *TableInfo, f ForeignKeyInfo) sql.ForeignKeyConstraint {
	return sql.ForeignKeyConstraint{
		Name: f.Name, Database: t.DB, Table: t.Name, Columns: append([]string(nil), f.Columns...),
		ParentDatabase: f.ParentDB, ParentTable: f.ParentTable, ParentColumns: append([]string(nil), f.ParentColumns...),
		OnUpdate: sql.ForeignKeyReferentialAction(f.OnUpdate), OnDelete: sql.ForeignKeyReferentialAction(f.OnDelete),
		IsResolved: f.Resolved,
	}
}

func fkInfo(fk sql.ForeignKeyConstraint) ForeignKeyInfo {
	return ForeignKeyInfo{
		Name: fk.Name, Columns: fk.Columns, ParentDB: fk.ParentDatabase, ParentTable: fk.ParentTable,
		ParentColumns: fk.ParentColumns, OnUpdate: string(fk.OnUpdate), OnDelete: string(fk.OnDelete), Resolved: fk.IsResolved,
	}
}

func (t *Table) GetDeclaredForeignKeys(*sql.Context) ([]sql.ForeignKeyConstraint, error) {
	out := make([]sql.ForeignKeyConstraint, len(t.info.ForeignKeys))
	for i, f := range t.info.ForeignKeys {
		out[i] = fkConstraint(t.info, f)
	}
	return out, nil
}

// GetReferencedForeignKeys finds the keys of every table that name this one
// as their parent.
func (t *Table) GetReferencedForeignKeys(ctx *sql.Context) ([]sql.ForeignKeyConstraint, error) {
	var out []sql.ForeignKeyConstraint
	err := t.cat.eachTable(ctx, func(ti *TableInfo) {
		for _, f := range ti.ForeignKeys {
			if strings.EqualFold(f.ParentDB, t.info.DB) && strings.EqualFold(f.ParentTable, t.info.Name) {
				out = append(out, fkConstraint(ti, f))
			}
		}
	})
	return out, err
}

func (t *Table) AddForeignKey(ctx *sql.Context, fk sql.ForeignKeyConstraint) error {
	// Constraint names are unique within a database.
	var dup bool
	err := t.cat.eachTable(ctx, func(ti *TableInfo) {
		if !strings.EqualFold(ti.DB, t.info.DB) {
			return
		}
		for _, f := range ti.ForeignKeys {
			dup = dup || strings.EqualFold(f.Name, fk.Name)
		}
	})
	if err != nil {
		return err
	}
	if dup {
		return sql.ErrForeignKeyDuplicateName.New(fk.Name)
	}
	return t.alter(ctx, func(ti *TableInfo) error {
		ti.ForeignKeys = append(ti.ForeignKeys, fkInfo(fk))
		return nil
	})
}

func (t *Table) DropForeignKey(ctx *sql.Context, name string) error {
	return t.alter(ctx, func(ti *TableInfo) error {
		for i, f := range ti.ForeignKeys {
			if strings.EqualFold(f.Name, name) {
				ti.ForeignKeys = append(ti.ForeignKeys[:i], ti.ForeignKeys[i+1:]...)
				return nil
			}
		}
		return sql.ErrForeignKeyNotFound.New(name, t.info.Name)
	})
}

func (t *Table) UpdateForeignKey(ctx *sql.Context, name string, fk sql.ForeignKeyConstraint) error {
	return t.alter(ctx, func(ti *TableInfo) error {
		for i, f := range ti.ForeignKeys {
			if strings.EqualFold(f.Name, name) {
				ti.ForeignKeys[i] = fkInfo(fk)
				return nil
			}
		}
		return sql.ErrForeignKeyNotFound.New(name, t.info.Name)
	})
}

// CreateIndexForForeignKey adds the index MySQL creates for a foreign key's
// columns when none covers them.
func (t *Table) CreateIndexForForeignKey(ctx *sql.Context, def sql.IndexDef) error {
	if err := t.CreateIndex(ctx, def); err != nil {
		return err
	}
	return t.reload(ctx)
}

// alter changes the table's catalog row and reloads this table object, which
// go-mysql-server goes on using within the statement.
func (t *Table) alter(ctx *sql.Context, f func(ti *TableInfo) error) error {
	if err := t.cat.AlterTable(ctx, t.info, func(_ *Txn, ti *TableInfo) error { return f(ti) }); err != nil {
		return err
	}
	return t.reload(ctx)
}

func (t *Table) reload(ctx *sql.Context) error {
	ti, err := t.cat.Table(ctx, t.info.DB, t.info.Name)
	if err != nil || ti == nil {
		return err
	}
	nt, err := newTable(ctx, t.cat, ti)
	if err != nil {
		return err
	}
	*t = *nt
	return nil
}

func (t *Table) GetForeignKeyEditor(ctx *sql.Context) sql.ForeignKeyEditor {
	return &fkEditor{editor: t.editor(ctx), t: t}
}

// fkEditor is the table's editor, addressable by index for the reference
// checks and cascades go-mysql-server runs.
type fkEditor struct {
	*editor
	t *Table
}

var _ sql.ForeignKeyEditor = (*fkEditor)(nil)

// IndexedAccess first sends the statement's buffered writes, so the lookup
// sees them (a child inserted with its parent in one statement).
func (e *fkEditor) IndexedAccess(ctx *sql.Context, lookup sql.IndexLookup) sql.IndexedTable {
	if err := e.flush(ctx); err != nil {
		ctx.GetLogger().Warnf("foreign key lookup: flush: %v", err)
	}
	it := e.t.IndexedAccess(ctx, lookup)
	if isFulltextTable(e.t.info.Name) {
		// the fulltext editor reads the counts it wrote for earlier rows
		// of the statement (fulltext.go)
		if x, ok := it.(*indexedTable); ok {
			_ = x.SetReferenceCheck()
		}
	}
	return it
}

func (e *fkEditor) GetIndexes(ctx *sql.Context) ([]sql.Index, error) { return e.t.GetIndexes(ctx) }
func (e *fkEditor) PreciseMatch() bool                               { return e.t.PreciseMatch() }

// SetReferenceCheck makes this lookup a reference check: shared row locks on
// the latest versions, including the statement's own writes.
func (it *indexedTable) SetReferenceCheck() error {
	t := *it.Table
	t.refCheck = true
	it.Table = &t
	return nil
}

// eachTable calls f for every table of the tenant.
func (c *Catalog) eachTable(ctx *sql.Context, f func(*TableInfo)) error {
	if err := c.ensure(ctx); err != nil {
		return err
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, ti := range c.tables {
		f(ti)
	}
	return nil
}

// refCheckLock is the lock a reference check takes.
const refCheckLock = kv.LockShared
