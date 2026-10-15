package sqlengine

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/salesstair-01/001/internal/encoding"
	"github.com/salesstair-01/001/internal/kv"
	"github.com/salesstair-01/001/internal/kvrpc"
)

// scanPage is how many rows one Scan call returns.
const scanPage = 256

// mutateBatch is the most row mutations one Mutate call carries (D7).
const mutateBatch = 256

// Table is a KV-backed table, clustered by its primary key.
type Table struct {
	store  *Store
	info   *TableInfo
	schema sql.PrimaryKeySchema
	codecs []colCodec // by schema ordinal
	pkOrd  []int      // schema ordinals of the primary-key columns, in key order
}

var (
	_ sql.Table            = (*Table)(nil)
	_ sql.PrimaryKeyTable  = (*Table)(nil)
	_ sql.CommentedTable   = (*Table)(nil)
	_ sql.InsertableTable  = (*Table)(nil)
	_ sql.UpdatableTable   = (*Table)(nil)
	_ sql.DeletableTable   = (*Table)(nil)
	_ sql.ReplaceableTable = (*Table)(nil)
	_ sql.IndexAddressable = (*Table)(nil)
	_ sql.Databaseable     = (*Table)(nil)
)

func newTable(ctx *sql.Context, store *Store, info *TableInfo) (*Table, error) {
	t := &Table{store: store, info: info}
	isPK := map[uint32]int{}
	for i, id := range info.PK {
		isPK[id] = i
	}
	t.pkOrd = make([]int, len(info.PK))
	var cols sql.Schema
	for i := range info.Columns {
		ci := &info.Columns[i]
		_, pk := isPK[ci.ID]
		col, err := sqlColumn(ctx, info, ci, pk)
		if err != nil {
			return nil, err
		}
		cols = append(cols, col)
		t.codecs = append(t.codecs, colCodec{info: ci, typ: col.Type})
		if pk {
			t.pkOrd[isPK[ci.ID]] = i
		}
	}
	t.schema = sql.NewPrimaryKeySchema(cols, t.pkOrd...)
	return t, nil
}

func (t *Table) Name() string                           { return t.info.Name }
func (t *Table) String() string                         { return t.info.Name }
func (t *Table) Database() string                       { return t.info.DB }
func (t *Table) Schema() sql.Schema                     { return t.schema.Schema }
func (t *Table) PrimaryKeySchema() sql.PrimaryKeySchema { return t.schema }
func (t *Table) Comment() string                        { return t.info.Comment }

func (t *Table) Collation() sql.CollationID {
	if c, err := sql.ParseCollation("", t.info.Collation, false); err == nil {
		return c
	}
	return sql.Collation_Default
}

func (t *Table) prefix() []byte {
	return encoding.Prefix(t.store.partition, t.info.ID, encoding.PrimaryIndexID)
}

// pkKey returns the primary-key user key of a row.
func (t *Table) pkKey(ctx context.Context, row sql.Row) ([]byte, error) {
	k := t.prefix()
	var err error
	for _, ord := range t.pkOrd {
		if k, err = t.codecs[ord].appendKey(ctx, k, row[ord]); err != nil {
			return nil, err
		}
	}
	return k, nil
}

func (t *Table) pkString(row sql.Row) string {
	parts := make([]string, len(t.pkOrd))
	for i, ord := range t.pkOrd {
		parts[i] = fmt.Sprint(row[ord])
	}
	return "[" + strings.Join(parts, ",") + "]"
}

// encodeRow encodes a row in row format v2, every column included (D3).
func (t *Table) encodeRow(ctx context.Context, row sql.Row) ([]byte, error) {
	vals := make([]encoding.RowValue, len(t.codecs))
	for i := range t.codecs {
		c := &t.codecs[i]
		vals[i].ID = c.info.ID
		if row[i] == nil {
			vals[i].Null = true
			continue
		}
		b, err := c.encodeValue(ctx, row[i])
		if err != nil {
			return nil, fmt.Errorf("column %s: %w", c.info.Name, err)
		}
		vals[i].Bytes = b
	}
	b, err := encoding.EncodeRow(vals)
	if err == encoding.ErrRowTooLarge {
		return nil, fmt.Errorf("row size exceeds the v1 limit of 1 MiB")
	}
	return b, err
}

// decodeRow decodes a stored row; a missing column id takes the column's
// original default (D3).
func (t *Table) decodeRow(ctx context.Context, b []byte) (sql.Row, error) {
	r, err := encoding.DecodeRow(b)
	if err != nil {
		return nil, err
	}
	row := make(sql.Row, len(t.codecs))
	for i := range t.codecs {
		c := &t.codecs[i]
		st, v, err := r.Lookup(c.info.ID)
		if err != nil {
			return nil, err
		}
		switch st {
		case encoding.ColNull:
			continue
		case encoding.ColMissing:
			if c.info.OriginalDefault == nil {
				continue
			}
			v = c.info.OriginalDefault
		}
		if row[i], err = c.decodeValue(ctx, v); err != nil {
			return nil, fmt.Errorf("column %s: %w", c.info.Name, err)
		}
	}
	return row, nil
}

// ---- reads ---------------------------------------------------------------------

// keySpan is a range [start, end) of user keys, or one exact key.
type keySpan struct {
	start, end []byte
	point      bool
}

type kvPartition struct {
	spans []keySpan
	key   []byte
}

func (p *kvPartition) Key() []byte { return p.key }

type partitionIter struct {
	parts []*kvPartition
	i     int
}

func (it *partitionIter) Next(*sql.Context) (sql.Partition, error) {
	if it.i >= len(it.parts) {
		return nil, io.EOF
	}
	it.i++
	return it.parts[it.i-1], nil
}

func (it *partitionIter) Close(*sql.Context) error { return nil }

func (t *Table) Partitions(*sql.Context) (sql.PartitionIter, error) {
	p := t.prefix()
	return &partitionIter{parts: []*kvPartition{{spans: []keySpan{{start: p, end: encoding.PrefixEnd(p)}}, key: p}}}, nil
}

func (t *Table) PartitionRows(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	p, ok := part.(*kvPartition)
	if !ok {
		return nil, fmt.Errorf("unexpected partition %T", part)
	}
	txn, done := sessionTxn(ctx, t.store)
	return &rowIter{t: t, txn: txn, opts: readOpts(ctx, t.info.Name), done: done, spans: p.spans}, nil
}

type rowIter struct {
	t     *Table
	txn   *Txn
	opts  kvrpc.ReadOpts
	done  func()
	spans []keySpan

	page   [][]byte
	next   []byte // resume key of the current span; nil = span not started
	more   bool
	closed bool
}

func (it *rowIter) Next(ctx *sql.Context) (sql.Row, error) {
	for {
		if len(it.page) > 0 {
			v := it.page[0]
			it.page = it.page[1:]
			return it.t.decodeRow(ctx, v)
		}
		if it.next != nil && it.more {
			if err := it.fetch(ctx, it.spans[0]); err != nil {
				return nil, err
			}
			continue
		}
		if it.next != nil {
			it.spans, it.next = it.spans[1:], nil
		}
		if len(it.spans) == 0 {
			return nil, io.EOF
		}
		sp := it.spans[0]
		if sp.point {
			vs, err := it.txn.Get(ctx, [][]byte{sp.start}, it.opts)
			if err != nil {
				return nil, mysqlError(err)
			}
			it.spans = it.spans[1:]
			if vs[0] != nil {
				return it.t.decodeRow(ctx, vs[0])
			}
			continue
		}
		it.next = sp.start
		if err := it.fetch(ctx, sp); err != nil {
			return nil, err
		}
	}
}

func (it *rowIter) fetch(ctx *sql.Context, sp keySpan) error {
	resp, err := it.txn.Scan(ctx, it.next, sp.end, scanPage, it.opts)
	if err != nil {
		return mysqlError(err)
	}
	it.page, it.more = resp.Values, len(resp.Resume) > 0
	if it.more {
		it.next = resp.Resume
	}
	return nil
}

func (it *rowIter) Close(*sql.Context) error {
	if !it.closed {
		it.closed = true
		it.done()
	}
	return nil
}

// ---- writes --------------------------------------------------------------------

func (t *Table) Inserter(ctx *sql.Context) sql.RowInserter { return t.editor(ctx) }
func (t *Table) Updater(ctx *sql.Context) sql.RowUpdater   { return t.editor(ctx) }
func (t *Table) Deleter(ctx *sql.Context) sql.RowDeleter   { return t.editor(ctx) }
func (t *Table) Replacer(ctx *sql.Context) sql.RowReplacer { return t.editor(ctx) }

func (t *Table) editor(*sql.Context) *editor {
	return &editor{t: t, pending: map[string]int{}}
}

// editor buffers one statement's row mutations and sends them in batches.
type editor struct {
	t       *Table
	buf     []kv.Mutation
	pending map[string]int // key -> index in buf
}

var _ sql.TableEditor = (*editor)(nil)

func (e *editor) txn(ctx *sql.Context) (*Txn, uint64, error) {
	t, ok := ctx.GetTransaction().(*Txn)
	if !ok || t == nil {
		return nil, 0, fmt.Errorf("write outside a transaction")
	}
	stmt := uint64(1)
	if s, ok := ctx.Session.(*Session); ok {
		stmt = s.Stmt()
	}
	return t, stmt, nil
}

func (e *editor) StatementBegin(*sql.Context) {}

func (e *editor) DiscardChanges(ctx *sql.Context, _ error) error {
	e.buf, e.pending = nil, map[string]int{}
	txn, stmt, err := e.txn(ctx)
	if err != nil {
		return err
	}
	return mysqlError(txn.RollbackStatement(ctx, stmt))
}

func (e *editor) StatementComplete(ctx *sql.Context) error { return e.flush(ctx) }
func (e *editor) Close(ctx *sql.Context) error             { return e.flush(ctx) }

func (e *editor) flush(ctx *sql.Context) error {
	if len(e.buf) == 0 {
		return nil
	}
	txn, stmt, err := e.txn(ctx)
	if err != nil {
		return err
	}
	buf := e.buf
	e.buf, e.pending = nil, map[string]int{}
	if txn.IsReadOnly() {
		return sql.ErrReadOnlyTransaction.New()
	}
	return mysqlError(txn.Mutate(ctx, stmt, buf, lockWait(ctx)))
}

// add buffers a mutation; a later write to the same key in the statement
// replaces the earlier one.
func (e *editor) add(ctx *sql.Context, m kv.Mutation) error {
	if i, ok := e.pending[string(m.Key)]; ok {
		prev := e.buf[i]
		switch {
		case prev.Op == kv.OpDelete && m.Op == kv.OpInsertOnly:
			m.Op = kv.OpPut // re-insert of a row deleted in this statement
		case prev.Op == kv.OpInsertOnly && m.Op == kv.OpPut:
			m.Op = kv.OpInsertOnly // still must not exist outside this statement
		}
		e.buf[i] = m
		return nil
	}
	e.pending[string(m.Key)] = len(e.buf)
	e.buf = append(e.buf, m)
	if len(e.buf) >= mutateBatch {
		return e.flush(ctx)
	}
	return nil
}

// existing returns the live row at key as this statement sees it, if any.
func (e *editor) existing(ctx *sql.Context, key []byte) (sql.Row, error) {
	if i, ok := e.pending[string(key)]; ok {
		m := e.buf[i]
		if m.Op == kv.OpDelete {
			return nil, nil
		}
		return e.t.decodeRow(ctx, m.Value)
	}
	txn, stmt, err := e.txn(ctx)
	if err != nil {
		return nil, err
	}
	// A locking read of the key: holding its lock keeps the duplicate check
	// true until commit (D5).
	vs, err := txn.Get(ctx, [][]byte{key}, kvrpc.ReadOpts{Stmt: stmt, IncludeStmt: true, Lock: kv.LockExclusive, LockWaitMs: lockWait(ctx).Milliseconds()})
	if err != nil {
		return nil, mysqlError(err)
	}
	if vs[0] == nil {
		return nil, nil
	}
	return e.t.decodeRow(ctx, vs[0])
}

func (e *editor) Insert(ctx *sql.Context, row sql.Row) error {
	key, err := e.t.pkKey(ctx, row)
	if err != nil {
		return err
	}
	old, err := e.existing(ctx, key)
	if err != nil {
		return err
	}
	if old != nil {
		return sql.NewUniqueKeyErr(e.t.pkString(row), true, old)
	}
	v, err := e.t.encodeRow(ctx, row)
	if err != nil {
		return err
	}
	return e.add(ctx, kv.Mutation{Op: kv.OpInsertOnly, Key: key, Value: v})
}

func (e *editor) Update(ctx *sql.Context, old, new sql.Row) error {
	oldKey, err := e.t.pkKey(ctx, old)
	if err != nil {
		return err
	}
	newKey, err := e.t.pkKey(ctx, new)
	if err != nil {
		return err
	}
	v, err := e.t.encodeRow(ctx, new)
	if err != nil {
		return err
	}
	if bytes.Equal(oldKey, newKey) {
		return e.add(ctx, kv.Mutation{Op: kv.OpPut, Key: newKey, Value: v})
	}
	// The primary key changed: delete the old row, insert the new one.
	if other, err := e.existing(ctx, newKey); err != nil {
		return err
	} else if other != nil {
		return sql.NewUniqueKeyErr(e.t.pkString(new), true, other)
	}
	if err := e.add(ctx, kv.Mutation{Op: kv.OpDelete, Key: oldKey}); err != nil {
		return err
	}
	return e.add(ctx, kv.Mutation{Op: kv.OpInsertOnly, Key: newKey, Value: v})
}

func (e *editor) Delete(ctx *sql.Context, row sql.Row) error {
	key, err := e.t.pkKey(ctx, row)
	if err != nil {
		return err
	}
	return e.add(ctx, kv.Mutation{Op: kv.OpDelete, Key: key})
}
