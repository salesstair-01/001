package sqlengine

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/salesstair-01/001/internal/kv"
	goerrors "gopkg.in/src-d/go-errors.v1"
)

// ---- ALTER TABLE by rewriting ------------------------------------------------------
//
// A change that stored rows or keys cannot follow in place (a new or
// dropped primary key, a column type whose encoding changes, a column made
// NOT NULL, dropping a column that is part of a key, adding an
// AUTO_INCREMENT, stored generated or expression-default column) rewrites the
// table, as MySQL's ALTER TABLE ... ALGORITHM=COPY does. go-mysql-server
// streams the rows, in the new schema, into RewriteInserter. They are
// written under a new table id, in a transaction of the rewrite's own;
// Close then points the table's name at the new id in that same
// transaction and commits, so the new table appears whole or not at all.
// The old table's rows are left behind, as DROP TABLE leaves them.
//
// While the rows are copied the old table is marked (RewriteUntil):
// marking it changes its schema version, so a writer that started before
// fails its commit, and a writer that starts after is refused with 1412 —
// MySQL would make it wait for the ALTER's metadata lock. The mark expires
// on its own if this server stops mid-copy.

var (
	_ sql.RewritableTable          = (*Table)(nil)
	_ sql.RewriteDecidingTable     = (*Table)(nil)
	_ sql.PrimaryKeyAlterableTable = (*Table)(nil)
)

// rewriteMarkTTL bounds how long a table stays marked if its rewrite never
// finishes.
const rewriteMarkTTL = 10 * time.Minute

func (t *Table) DecidesRewrite() {}

// CreatePrimaryKey and DropPrimaryKey are not called: go-mysql-server
// rewrites a RewritableTable for a primary-key change.
func (t *Table) CreatePrimaryKey(*sql.Context, []sql.IndexColumn) error {
	return fmt.Errorf("primary keys are changed by rewriting the table")
}
func (t *Table) DropPrimaryKey(*sql.Context) error {
	return fmt.Errorf("primary keys are changed by rewriting the table")
}

// errBeingRewritten refuses a write to a table an ALTER TABLE is copying.
func errBeingRewritten() error {
	return mysql.NewSQLError(1412, "HY000", "Table definition has changed, please retry transaction")
}

// beingRewritten is whether an ALTER TABLE is copying the table.
func (t *Table) beingRewritten() bool {
	return t.info.RewriteUntil > time.Now().UnixNano()
}

func (t *Table) ShouldRewriteTable(ctx *sql.Context, oldSchema, newSchema sql.PrimaryKeySchema, oldColumn, newColumn *sql.Column) bool {
	if !samePK(oldSchema, newSchema) {
		return true
	}
	switch {
	case oldColumn != nil && newColumn != nil: // MODIFY / CHANGE
		return !compatibleType(oldColumn.Type, newColumn.Type) ||
			(oldColumn.Nullable && !newColumn.Nullable) ||
			(newColumn.AutoIncrement && !oldColumn.AutoIncrement) ||
			generatedChanged(oldColumn, newColumn)
	case newColumn != nil: // ADD
		return newColumn.PrimaryKey || newColumn.AutoIncrement ||
			(newColumn.Generated != nil && !newColumn.Virtual) ||
			(newColumn.Default != nil && !newColumn.Default.IsLiteral())
	case oldColumn != nil: // DROP
		ord := t.ordinal(oldColumn.Name)
		if ord < 0 {
			return false
		}
		id := t.codecs[ord].info.ID
		for _, pk := range t.info.PK {
			if pk == id {
				return true
			}
		}
		for _, ix := range t.info.Indexes {
			for _, c := range ix.Columns {
				if c.ColID == id {
					return true
				}
			}
		}
	}
	return false
}

func generatedChanged(a, b *sql.Column) bool {
	if (a.Generated == nil) != (b.Generated == nil) {
		return true
	}
	return a.Generated != nil && (a.Generated.String() != b.Generated.String() || a.Virtual != b.Virtual)
}

func samePK(a, b sql.PrimaryKeySchema) bool {
	if len(a.PkOrdinals) != len(b.PkOrdinals) {
		return false
	}
	for i := range a.PkOrdinals {
		if !strings.EqualFold(a.Schema[a.PkOrdinals[i]].Name, b.Schema[b.PkOrdinals[i]].Name) {
			return false
		}
	}
	return true
}

func (t *Table) RewriteInserter(ctx *sql.Context, oldSchema, newSchema sql.PrimaryKeySchema, oldColumn, newColumn *sql.Column, _ []sql.IndexColumn) (sql.RowInserter, error) {
	if len(t.fulltext) > 0 {
		return nil, fmt.Errorf("this ALTER TABLE rewrites table %s, which has a FULLTEXT index; drop the index first", t.info.Name)
	}
	for _, ord := range newSchema.PkOrdinals {
		if col := newSchema.Schema[ord]; !keyable(col.Type) {
			return nil, fmt.Errorf("column %s: type %s cannot be part of a primary key in v1", col.Name, col.Type)
		}
	}
	// Mark the table; the copy reads the marked version.
	until := time.Now().Add(rewriteMarkTTL).UnixNano()
	if err := t.cat.AlterTable(ctx, t.info, func(_ *Txn, ti *TableInfo) error {
		ti.RewriteUntil = until
		return nil
	}); err != nil {
		return nil, err
	}
	old, err := t.cat.Table(ctx, t.info.DB, t.info.Name)
	if err != nil {
		return nil, err
	}
	if old == nil || old.ID != t.info.ID {
		return nil, sql.ErrTableNotFound.New(t.info.Name)
	}
	old = old.clone()
	r := &rewriter{t: t, old: old}
	// MODIFY a ... AUTO_INCREMENT PRIMARY KEY is a MODIFY and then an ADD
	// PRIMARY KEY, each a rewrite: the second resequences too.
	r.resequence = (newColumn != nil && newColumn.AutoIncrement && (oldColumn == nil || !oldColumn.AutoIncrement)) ||
		(readsRewritten(ctx, t.info.ID) && newSchema.HasAutoIncrement())
	fail := func(err error) (sql.RowInserter, error) {
		r.unmark(ctx)
		return nil, err
	}
	info, err := rewrittenInfo(old, newSchema, oldColumn, newColumn)
	if err != nil {
		return fail(err)
	}
	// The id comes from a transaction of its own, so the copy does not hold
	// the id sequence's lock (a failed rewrite skips an id).
	idTxn := t.store.NewTxn(kv.RepeatableRead, false)
	if info.ID, err = t.cat.nextTableID(ctx, idTxn); err != nil {
		_ = idTxn.Rollback(ctx)
		return fail(err)
	}
	if err := idTxn.Commit(ctx); err != nil {
		return fail(err)
	}
	r.txn = t.store.NewTxn(kv.RepeatableRead, false)
	r.info = info
	nt, err := newTable(ctx, t.cat, info)
	if err != nil {
		_ = r.txn.Rollback(ctx)
		return fail(err)
	}
	r.ed = &editor{t: nt, pending: map[string]int{}, own: r.txn}
	if sess, ok := ctx.Session.(*Session); ok {
		sess.rewriteMu.Lock()
		sess.openRewrites = append(sess.openRewrites, r)
		sess.rewriteMu.Unlock()
	}
	return r, nil
}

// rewrittenInfo is the catalog row of the rewritten table: old's, with
// newSchema's columns. A column keeps its id (renamed when newColumn
// replaces oldColumn); a new column gets the next one. Secondary indexes
// lose dropped columns, and an index left with none is dropped, as in
// MySQL.
func rewrittenInfo(old *TableInfo, newSchema sql.PrimaryKeySchema, oldColumn, newColumn *sql.Column) (*TableInfo, error) {
	info := &TableInfo{
		DB: old.DB, Name: old.Name, Collation: old.Collation, Comment: old.Comment,
		NextColumnID: old.NextColumnID, NextIndexID: old.NextIndexID,
		ForeignKeys: old.ForeignKeys, Checks: old.Checks, Version: 1,
	}
	oldID := map[string]uint32{}
	for _, c := range old.Columns {
		oldID[strings.ToLower(c.Name)] = c.ID
	}
	kept := map[uint32]bool{}
	for _, col := range newSchema.Schema {
		name := strings.ToLower(col.Name)
		if newColumn != nil && strings.EqualFold(col.Name, newColumn.Name) {
			name = ""
			if oldColumn != nil {
				name = strings.ToLower(oldColumn.Name) // MODIFY / CHANGE keeps the id
			}
		}
		id, ok := oldID[name]
		if !ok || name == "" {
			id = info.NextColumnID
			info.NextColumnID++
		}
		ci, err := columnInfo(col, id)
		if err != nil {
			return nil, err
		}
		kept[id] = true
		info.Columns = append(info.Columns, ci)
	}
	for _, ord := range newSchema.PkOrdinals {
		info.PK = append(info.PK, info.Columns[ord].ID)
	}
	for _, ix := range old.Indexes {
		if ix.Dropping {
			continue
		}
		ix.State, ix.Since = "", 0
		var cols []IndexColumn
		for _, c := range ix.Columns {
			if kept[c.ColID] {
				cols = append(cols, c)
			}
		}
		if len(cols) == 0 {
			continue
		}
		ix.Columns = cols
		info.Indexes = append(info.Indexes, ix)
	}
	return info, nil
}

// rewriter is the RowInserter of a rewrite.
type rewriter struct {
	t         *Table
	old, info *TableInfo
	txn       *Txn
	ed        *editor
	maxAuto   uint64
	discarded bool
	closed    bool
	// resequence: the ALTER makes a column AUTO_INCREMENT, and MySQL
	// words a duplicate key as caused by the resequencing.
	resequence bool
}

func (r *rewriter) StatementBegin(*sql.Context) {}

func (r *rewriter) DiscardChanges(ctx *sql.Context, _ error) error {
	r.discarded = true
	return nil
}

func (r *rewriter) StatementComplete(*sql.Context) error { return nil }

func (r *rewriter) Insert(ctx *sql.Context, row sql.Row) error {
	if o := r.ed.t.autoOrd; o >= 0 && row[o] != nil {
		if n, err := toUint64(ctx, r.ed.t.codecs[o].typ, row[o]); err == nil && n > r.maxAuto {
			r.maxAuto = n
		}
	}
	err := r.ed.Insert(ctx, row)
	if r.resequence && (sql.ErrPrimaryKeyViolation.Is(err) || sql.ErrUniqueKeyViolation.Is(err)) {
		if msg, ok := strings.CutPrefix(uniqueKeyText(err), "Duplicate entry "); ok {
			return mysql.NewSQLError(1062, "23000", "ALTER TABLE causes auto_increment resequencing, resulting in duplicate entry %s", msg)
		}
	}
	return err
}

// unmark clears the old table's mark after a failed rewrite.
func (r *rewriter) unmark(ctx context.Context) {
	_ = r.t.cat.AlterTable(ctx, r.t.info, func(_ *Txn, ti *TableInfo) error {
		ti.RewriteUntil = 0
		return nil
	})
}

func (r *rewriter) Close(ctx *sql.Context) error {
	if r.closed {
		return nil
	}
	r.closed = true
	if sess, ok := ctx.Session.(*Session); ok {
		sess.rewriteMu.Lock()
		for i, o := range sess.openRewrites {
			if o == r {
				sess.openRewrites = append(sess.openRewrites[:i], sess.openRewrites[i+1:]...)
				break
			}
		}
		sess.rewriteMu.Unlock()
	}
	if r.discarded {
		_ = r.txn.Rollback(ctx)
		r.unmark(ctx)
		return nil
	}
	if err := r.swap(ctx); err != nil {
		_ = r.txn.Rollback(ctx)
		r.unmark(ctx)
		return err
	}
	if err := r.t.cat.load(ctx); err != nil {
		return err
	}
	// go-mysql-server goes on using this table object (foreign keys,
	// checks): make it the new table.
	nt, err := newTable(ctx, r.t.cat, r.info.clone())
	if err != nil {
		return err
	}
	*r.t = *nt
	if sess, ok := ctx.Session.(*Session); ok {
		sess.noteRewritten(ctx, r.info.ID)
	}
	return nil
}

// swap commits the copied rows with the catalog change that makes them the
// table.
func (r *rewriter) swap(ctx *sql.Context) error {
	if err := r.ed.flush(ctx); err != nil {
		return err
	}
	txn, c := r.txn, r.t.cat
	// The counter continues from the old table's, or from the largest value
	// copied (a column made AUTO_INCREMENT).
	if r.ed.t.autoOrd >= 0 {
		next, err := r.t.PeekNextAutoIncrementValue(ctx)
		if err != nil {
			return err
		}
		next = max(next, r.maxAuto+1, 1)
		if _, err := txn.AutoIncrement(ctx, r.info.ID, 0, next); err != nil {
			return mysqlError(err)
		}
	}
	// Nothing else changed the old table since it was marked.
	txn.Guard(c.store.schemaVerKey(r.old.ID), versionBytes(r.old.Version))
	if err := txn.Mutate(ctx, 1, []kv.Mutation{
		{Op: kv.OpDelete, Key: c.store.tableKey(r.old.ID)},
		{Op: kv.OpDelete, Key: c.store.schemaVerKey(r.old.ID)}, // stale writers' guards now fail
	}, 0); err != nil {
		return err
	}
	if err := c.setSchemaVersion(ctx, txn, r.info); err != nil {
		return err
	}
	if err := put(txn, ctx, kv.OpPut, c.store.tableNameKey(r.info.DB, r.info.Name), r.info.ID); err != nil {
		return err
	}
	if err := put(txn, ctx, kv.OpInsertOnly, c.store.tableKey(r.info.ID), r.info); err != nil {
		return err
	}
	if err := c.bumpVersion(ctx, txn); err != nil {
		return err
	}
	return mysqlError(txn.Commit(ctx))
}

// noteRewritten records that a rewrite made table id in the session's
// current transaction. Its rows were committed after the transaction's
// snapshot, so until the transaction ends the session reads them as
// committed (a second rewrite in the same ALTER TABLE reads them).
func (s *Session) noteRewritten(ctx *sql.Context, id uint32) {
	txn, _ := ctx.GetTransaction().(*Txn)
	s.rewriteMu.Lock()
	defer s.rewriteMu.Unlock()
	if s.rewrittenIn != txn {
		s.rewrittenIn, s.rewritten = txn, map[uint32]bool{}
	}
	s.rewritten[id] = true
}

// readsRewritten is whether table id was made by a rewrite in the
// session's current transaction.
func readsRewritten(ctx *sql.Context, id uint32) bool {
	s, ok := ctx.Session.(*Session)
	if !ok {
		return false
	}
	txn, _ := ctx.GetTransaction().(*Txn)
	s.rewriteMu.Lock()
	defer s.rewriteMu.Unlock()
	return s.rewrittenIn == txn && s.rewritten[id]
}

// abandonRewrites ends the rewrites of a statement that stopped without
// closing them (a panic): their copies are rolled back and their tables
// unmarked.
func (s *Session) abandonRewrites() {
	s.rewriteMu.Lock()
	open := s.openRewrites
	s.openRewrites = nil
	s.rewriteMu.Unlock()
	for _, r := range open {
		if !r.closed {
			r.closed = true
			_ = r.txn.Rollback(context.Background())
			r.unmark(context.Background())
		}
	}
}

// uniqueKeyText is a duplicate-key error's message ("Duplicate entry ...").
func uniqueKeyText(err error) string {
	if e, ok := err.(*goerrors.Error); ok {
		if ue, ok := e.Cause().(sql.UniqueKeyError); ok {
			return ue.Error()
		}
	}
	return ""
}
