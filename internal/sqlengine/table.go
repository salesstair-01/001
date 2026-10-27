package sqlengine

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/mysql"

	"github.com/salesstair-01/001/internal/encoding"
	"github.com/salesstair-01/001/internal/kv"
	"github.com/salesstair-01/001/internal/kvrpc"
)

// scanPage is how many rows one Scan call returns.
const scanPage = 256

// mutateBatch is the most row mutations one Mutate call carries (D7).
const mutateBatch = 256

// Table is a KV-backed table, clustered by its primary key as in InnoDB.
type Table struct {
	cat     *Catalog
	store   *Store
	info    *TableInfo
	schema  sql.PrimaryKeySchema
	codecs  []colCodec // by schema ordinal
	pkOrd   []int      // schema ordinals of the primary-key columns, in key order
	colOrd  map[uint32]int
	indexes []*secIndex
	// fulltext are the FULLTEXT indexes (fulltext.go), which writers
	// maintain through go-mysql-server's tables, not as KV entries.
	fulltext []*fulltextIndex
	autoOrd int // ordinal of the AUTO_INCREMENT column, or -1
	// instantAdd marks the table object of an ADD COLUMN statement after the
	// column was added: it reads no rows and writes nothing (see AddColumn).
	instantAdd bool
	// keyless tables have no primary key and are keyed by a hidden rowid.
	keyless bool
	// refCheck marks a foreign-key reference check (fk.go).
	refCheck bool
	// cmpSchema, when set, is what Schema() returns: the schema with text
	// columns in the binary collation, for an UPDATE (exactupdate.go).
	cmpSchema sql.Schema
}

var (
	_ sql.Table               = (*Table)(nil)
	_ sql.PrimaryKeyTable     = (*Table)(nil)
	_ sql.CommentedTable      = (*Table)(nil)
	_ sql.InsertableTable     = (*Table)(nil)
	_ sql.UpdatableTable      = (*Table)(nil)
	_ sql.DeletableTable      = (*Table)(nil)
	_ sql.ReplaceableTable    = (*Table)(nil)
	_ sql.IndexAddressable    = (*Table)(nil)
	_ sql.IndexAlterableTable = (*Table)(nil)
	_ sql.AutoIncrementTable  = (*Table)(nil)
	_ sql.Databaseable        = (*Table)(nil)
)

func newTable(ctx *sql.Context, cat *Catalog, info *TableInfo) (*Table, error) {
	t := &Table{cat: cat, store: cat.store, info: info, colOrd: map[uint32]int{}, autoOrd: -1}
	isPK := map[uint32]int{}
	for i, id := range info.PK {
		isPK[id] = i
	}
	t.pkOrd = make([]int, len(info.PK))
	t.keyless = len(info.PK) == 0
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
		t.colOrd[ci.ID] = i
		if pk {
			t.pkOrd[isPK[ci.ID]] = i
		}
		if ci.AutoIncrement {
			t.autoOrd = i
		}
	}
	t.schema = sql.NewPrimaryKeySchema(cols, t.pkOrd...)
	for i := range info.Indexes {
		if info.Indexes[i].Fulltext != nil {
			ft, err := newFulltextIndex(t, &info.Indexes[i])
			if err != nil {
				return nil, err
			}
			t.fulltext = append(t.fulltext, ft)
			continue
		}
		ix := &secIndex{t: t, info: &info.Indexes[i]}
		for _, c := range ix.info.Columns {
			ord, ok := t.colOrd[c.ColID]
			if !ok {
				return nil, fmt.Errorf("index %s: unknown column id %d", ix.info.Name, c.ColID)
			}
			ix.ords = append(ix.ords, ord)
			ix.prefix = append(ix.prefix, c.Prefix)
		}
		t.indexes = append(t.indexes, ix)
	}
	return t, nil
}

func (t *Table) Name() string     { return t.info.Name }
func (t *Table) String() string   { return t.info.Name }
func (t *Table) Database() string { return t.info.DB }
func (t *Table) Schema() sql.Schema {
	if t.cmpSchema != nil {
		return t.cmpSchema
	}
	return t.schema.Schema
}
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

func keyString(row sql.Row, ords []int) string {
	parts := make([]string, len(ords))
	for i, ord := range ords {
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
			if se, ok := err.(*mysql.SQLError); ok {
				return nil, se // already names the column, with MySQL's code
			}
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

// guard makes txn's commit fail if this table's schema changed since this
// query engine loaded it: no write commits with a stale schema (slice 8).
func (t *Table) guard(txn *Txn) {
	var v []byte
	if t.info.Version != 0 {
		v = versionBytes(t.info.Version)
	}
	txn.Guard(t.store.schemaVerKey(t.info.ID), v)
}

// ---- secondary indexes (D2) ----------------------------------------------------

// secIndex is one secondary index. Entries are
//
//	non-unique:                index cols + pk cols -> empty
//	unique:                    index cols           -> encoded pk
//	unique, any column NULL:   index cols + pk cols -> empty
//
// The NULL rule is there because MySQL allows several NULLs in a unique index.
type secIndex struct {
	t      *Table
	info   *IndexInfo
	ords   []int
	prefix []int
}

func (ix *secIndex) keyPrefix() []byte {
	return encoding.Prefix(ix.t.store.partition, ix.t.info.ID, ix.info.ID)
}

// entry returns the index entry for a row whose primary key is pk. unique
// reports whether the entry enforces uniqueness (unique index, no NULLs).
func (ix *secIndex) entry(ctx context.Context, row sql.Row, pk []byte) (key, value []byte, unique bool, err error) {
	k := ix.keyPrefix()
	hasNull := false
	for i, ord := range ix.ords {
		v := row[ord]
		if v == nil {
			hasNull = true
			k = encoding.AppendNull(k, false)
			continue
		}
		c := &ix.t.codecs[ord]
		if k, err = c.appendKey(ctx, k, truncate(c.typ, v, ix.prefix[i])); err != nil {
			return nil, nil, false, err
		}
	}
	suffix := pk[encoding.PrefixLen:]
	if ix.info.Unique && !hasNull {
		return k, append([]byte(nil), suffix...), true, nil
	}
	return append(k, suffix...), []byte{}, false, nil
}

// pkOf recovers the primary key from an index entry.
func (ix *secIndex) pkOf(key, value []byte) ([]byte, error) {
	if len(value) > 0 {
		return append(ix.t.prefix(), value...), nil
	}
	rest := key[encoding.PrefixLen:]
	for _, ord := range ix.ords {
		n, err := encoding.SkipColumn(rest, ix.t.codecs[ord].keyKind(), false)
		if err != nil {
			return nil, err
		}
		rest = rest[n:]
	}
	return append(ix.t.prefix(), rest...), nil
}

func (t *Table) uniqueIndexes() []*secIndex {
	var out []*secIndex
	for _, ix := range t.indexes {
		if ix.info.Unique {
			out = append(out, ix)
		}
	}
	return out
}

// ---- reads ---------------------------------------------------------------------

// keySpan is a range [start, end) of user keys, or one exact key.
type keySpan struct {
	start, end []byte
	point      bool
}

type kvPartition struct {
	spans []keySpan
	index *secIndex // nil = primary key
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
	if t.instantAdd {
		return &partitionIter{}, nil
	}
	p := t.prefix()
	return &partitionIter{parts: []*kvPartition{{spans: []keySpan{{start: p, end: encoding.PrefixEnd(p)}}, key: p}}}, nil
}

func (t *Table) PartitionRows(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	p, ok := part.(*kvPartition)
	if !ok {
		return nil, fmt.Errorf("unexpected partition %T", part)
	}
	txn, done := sessionTxn(ctx, t.store)
	opts := readOpts(ctx, t.info.Name)
	if t.refCheck {
		opts.IncludeStmt = true
		if opts.Lock == kv.LockNone {
			opts.Lock, opts.LockWaitMs = refCheckLock, lockWait(ctx).Milliseconds()
		}
	}
	it := &rowIter{t: t, txn: txn, opts: opts, done: done, spans: p.spans, index: p.index}
	if c, stmt, ok := cacheable(ctx, it); ok {
		key := cacheKey(t, p.index, p.spans)
		if rows, hit := c.get(stmt, key); hit {
			it.page, it.spans, it.fromCache = rows, nil, true
		} else {
			it.cache, it.cacheStmt, it.cacheKey = c, stmt, key
		}
	}
	return it, nil
}

// rowIter reads rows span by span: primary spans directly, secondary spans
// as index entries followed by a batched Get of their rows.
type rowIter struct {
	t     *Table
	txn   *Txn
	opts  kvrpc.ReadOpts
	done  func()
	spans []keySpan
	index *secIndex

	page   [][]byte // row values ready to decode
	keys   [][]byte // their keys, when noteKeys
	next   []byte   // resume key of the current span; nil = span not started
	more   bool
	closed bool

	// Statement read cache (readcache.go): rows read so far, kept on EOF.
	cache     *readCache
	cacheStmt uint64
	cacheKey  string
	seen      [][]byte
	seenBytes int
	fromCache bool

	polled uint32 // rows since the context was last checked
}

func (it *rowIter) Next(ctx *sql.Context) (sql.Row, error) {
	if err := pollCancel(ctx, &it.polled); err != nil {
		return nil, err
	}
	for {
		if len(it.page) > 0 {
			v := it.page[0]
			it.page = it.page[1:]
			it.remember(v)
			var k []byte
			if it.noteKeys() {
				k, it.keys = it.keys[0], it.keys[1:]
			}
			return it.row(ctx, v, k)
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
			if it.cache != nil {
				it.cache.put(it.cacheStmt, it.cacheKey, it.seen, it.seenBytes)
				it.cache = nil
			}
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
				it.remember(vs[0])
				return it.row(ctx, vs[0], sp.start)
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
	it.more = len(resp.Resume) > 0
	if it.more {
		it.next = resp.Resume
	}
	if it.index == nil {
		it.page, it.keys = resp.Values, resp.Keys
		return nil
	}
	if len(resp.Keys) == 0 {
		it.page, it.keys = nil, nil
		return nil
	}
	pks := make([][]byte, len(resp.Keys))
	for i := range resp.Keys {
		if pks[i], err = it.index.pkOf(resp.Keys[i], resp.Values[i]); err != nil {
			return err
		}
	}
	rows, err := it.txn.Get(ctx, pks, it.opts)
	if err != nil {
		return mysqlError(err)
	}
	it.page, it.keys = it.page[:0], it.keys[:0]
	for i, r := range rows {
		if r != nil {
			it.page = append(it.page, r)
			it.keys = append(it.keys, pks[i])
		}
	}
	return nil
}

// remember keeps a row value for the statement read cache.
func (it *rowIter) remember(v []byte) {
	if it.cache == nil {
		return
	}
	it.seenBytes += len(v) + 24
	if it.seenBytes > readCacheEntryBytes {
		it.cache, it.seen = nil, nil // too big to keep
		return
	}
	it.seen = append(it.seen, v)
}

// noteKeys says whether rows read are noted with their keys, for the
// UPDATE or DELETE of a keyless table (see rowid.go).
func (it *rowIter) noteKeys() bool {
	return it.t.keyless && it.opts.Lock != kv.LockNone
}

func (it *rowIter) row(ctx *sql.Context, v, key []byte) (sql.Row, error) {
	row, err := it.t.decodeRow(ctx, v)
	if err == nil && it.noteKeys() {
		err = it.t.noteRowKey(ctx, row, key)
	}
	return row, err
}

func (it *rowIter) Close(*sql.Context) error {
	if !it.closed {
		it.closed = true
		it.done()
	}
	return nil
}

// ---- writes --------------------------------------------------------------------

func (t *Table) Inserter(ctx *sql.Context) sql.RowInserter { return t.writer(ctx) }
func (t *Table) Updater(ctx *sql.Context) sql.RowUpdater   { return t.writer(ctx) }
func (t *Table) Deleter(ctx *sql.Context) sql.RowDeleter   { return t.writer(ctx) }
func (t *Table) Replacer(ctx *sql.Context) sql.RowReplacer { return t.writer(ctx) }

// writer is the table's editor, and its FULLTEXT indexes' (fulltext.go).
func (t *Table) writer(ctx *sql.Context) sql.TableEditor {
	if isFulltextTable(t.info.Name) {
		// go-mysql-server's fulltext editor looks rows of its tables up
		// by key, its own writes included
		return &fkEditor{editor: t.editor(ctx), t: t}
	}
	if len(t.fulltext) == 0 {
		return t.editor(ctx)
	}
	return t.fulltextEditor(ctx, &ftParentEditor{
		fkEditor:            &fkEditor{editor: t.editor(ctx), t: t},
		AutoIncrementSetter: t.AutoIncrementSetter(ctx),
	})
}

func (t *Table) editor(*sql.Context) *editor {
	return &editor{t: t, pending: map[string]int{}}
}

// editor buffers one statement's row and index mutations and sends them in batches.
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
	e.t.guard(txn)
	e.t.store.noteWrites(e.t.info, len(buf))
	return mysqlError(txn.Mutate(ctx, stmt, buf, lockWait(ctx)))
}

// add buffers a mutation; a later write to the same key in the statement
// replaces the earlier one.
func (e *editor) add(ctx *sql.Context, m kv.Mutation) error {
	if i, ok := e.pending[string(m.Key)]; ok {
		prev := e.buf[i]
		switch {
		case prev.Op == kv.OpDelete && m.Op == kv.OpInsertOnly:
			m.Op = kv.OpPut // re-insert of a key deleted in this statement
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

// lookup returns the live value at key as this statement sees it: from the
// buffer, or a locking read of the latest version, whose lock keeps the
// answer true until commit (D5). nil = absent.
func (e *editor) lookup(ctx *sql.Context, key []byte) ([]byte, error) {
	if i, ok := e.pending[string(key)]; ok {
		if m := e.buf[i]; m.Op != kv.OpDelete {
			return m.Value, nil
		}
		return nil, nil
	}
	txn, stmt, err := e.txn(ctx)
	if err != nil {
		return nil, err
	}
	vs, err := txn.Get(ctx, [][]byte{key}, kvrpc.ReadOpts{Stmt: stmt, IncludeStmt: true, Lock: kv.LockExclusive, LockWaitMs: lockWait(ctx).Milliseconds()})
	if err != nil {
		return nil, mysqlError(err)
	}
	return vs[0], nil
}

// existing returns the live row with primary key pk, if any.
func (e *editor) existing(ctx *sql.Context, pk []byte) (sql.Row, error) {
	v, err := e.lookup(ctx, pk)
	if err != nil || v == nil {
		return nil, err
	}
	return e.t.decodeRow(ctx, v)
}

// uniqueOwner returns the row that holds a unique index entry, if any.
func (e *editor) uniqueOwner(ctx *sql.Context, ix *secIndex, key []byte) (sql.Row, error) {
	v, err := e.lookup(ctx, key)
	if err != nil || v == nil {
		return nil, err
	}
	pk, err := ix.pkOf(key, v)
	if err != nil {
		return nil, err
	}
	return e.existing(ctx, pk)
}

func (e *editor) uniqueErr(ix *secIndex, row, existing sql.Row) error {
	return sql.NewUniqueKeyErr(fmt.Sprintf("%s for key '%s.%s'", keyString(row, ix.ords), e.t.info.Name, ix.info.Name), false, existing)
}

func entryOp(unique bool) kv.MutationOp {
	if unique {
		return kv.OpInsertOnly
	}
	return kv.OpPut
}

func (e *editor) Insert(ctx *sql.Context, row sql.Row) error {
	var pk []byte
	var err error
	if e.t.keyless {
		if pk, err = e.newRowid(ctx); err != nil {
			return err
		}
	} else if pk, err = e.t.pkKey(ctx, row); err != nil {
		return err
	} else if old, err := e.existing(ctx, pk); err != nil {
		return err
	} else if old != nil {
		return sql.NewUniqueKeyErr(keyString(row, e.t.pkOrd), true, old)
	}
	type ent struct {
		key, value []byte
		unique     bool
	}
	ents := make([]ent, 0, len(e.t.indexes))
	for _, ix := range e.t.indexes {
		k, v, uniq, err := ix.entry(ctx, row, pk)
		if err != nil {
			return err
		}
		if uniq {
			if other, err := e.uniqueOwner(ctx, ix, k); err != nil {
				return err
			} else if other != nil {
				return e.uniqueErr(ix, row, other)
			}
		}
		ents = append(ents, ent{k, v, uniq})
	}
	v, err := e.t.encodeRow(ctx, row)
	if err != nil {
		return err
	}
	if err := e.add(ctx, kv.Mutation{Op: kv.OpInsertOnly, Key: pk, Value: v}); err != nil {
		return err
	}
	for _, en := range ents {
		if err := e.add(ctx, kv.Mutation{Op: entryOp(en.unique), Key: en.key, Value: en.value}); err != nil {
			return err
		}
	}
	return nil
}

func (e *editor) Update(ctx *sql.Context, old, new sql.Row) error {
	oldPK, newPK, err := e.updateKeys(ctx, old, new)
	if err != nil {
		return err
	}
	pkMoved := !bytes.Equal(oldPK, newPK)
	if pkMoved {
		if other, err := e.existing(ctx, newPK); err != nil {
			return err
		} else if other != nil {
			return sql.NewUniqueKeyErr(keyString(new, e.t.pkOrd), true, other)
		}
	}
	type change struct {
		oldKey, newKey, newValue []byte
		unique                   bool
	}
	var changes []change
	for _, ix := range e.t.indexes {
		ok, ov, _, err := ix.entry(ctx, old, oldPK)
		if err != nil {
			return err
		}
		nk, nv, uniq, err := ix.entry(ctx, new, newPK)
		if err != nil {
			return err
		}
		if bytes.Equal(ok, nk) && bytes.Equal(ov, nv) {
			continue
		}
		if uniq && !bytes.Equal(ok, nk) {
			if other, err := e.uniqueOwner(ctx, ix, nk); err != nil {
				return err
			} else if other != nil {
				return e.uniqueErr(ix, new, other)
			}
		}
		changes = append(changes, change{ok, nk, nv, uniq})
	}
	v, err := e.t.encodeRow(ctx, new)
	if err != nil {
		return err
	}
	if pkMoved {
		if err := e.add(ctx, kv.Mutation{Op: kv.OpDelete, Key: oldPK}); err != nil {
			return err
		}
		if err := e.add(ctx, kv.Mutation{Op: kv.OpInsertOnly, Key: newPK, Value: v}); err != nil {
			return err
		}
	} else if err := e.add(ctx, kv.Mutation{Op: kv.OpPut, Key: newPK, Value: v}); err != nil {
		return err
	}
	for _, c := range changes {
		op := kv.OpPut // same key, new primary key: rewrite this row's own entry
		if !bytes.Equal(c.oldKey, c.newKey) {
			if err := e.add(ctx, kv.Mutation{Op: kv.OpDelete, Key: c.oldKey}); err != nil {
				return err
			}
			op = entryOp(c.unique)
		}
		if err := e.add(ctx, kv.Mutation{Op: op, Key: c.newKey, Value: c.newValue}); err != nil {
			return err
		}
	}
	return nil
}

// updateKeys returns the keys of a row before and after an update; a keyless
// row keeps its rowid.
func (e *editor) updateKeys(ctx *sql.Context, old, new sql.Row) ([]byte, []byte, error) {
	if e.t.keyless {
		k, err := e.keyOf(ctx, old)
		return k, k, err
	}
	oldPK, err := e.t.pkKey(ctx, old)
	if err != nil {
		return nil, nil, err
	}
	newPK, err := e.t.pkKey(ctx, new)
	return oldPK, newPK, err
}

func (e *editor) Delete(ctx *sql.Context, row sql.Row) error {
	var pk []byte
	var err error
	if e.t.keyless {
		pk, err = e.keyOf(ctx, row)
	} else {
		pk, err = e.t.pkKey(ctx, row)
	}
	if err != nil {
		return err
	}
	if err := e.add(ctx, kv.Mutation{Op: kv.OpDelete, Key: pk}); err != nil {
		return err
	}
	for _, ix := range e.t.indexes {
		k, _, _, err := ix.entry(ctx, row, pk)
		if err != nil {
			return err
		}
		if err := e.add(ctx, kv.Mutation{Op: kv.OpDelete, Key: k}); err != nil {
			return err
		}
	}
	return nil
}

// ---- AUTO_INCREMENT (D7) ------------------------------------------------------------

func (t *Table) PeekNextAutoIncrementValue(ctx *sql.Context) (uint64, error) {
	h, c, err := t.store.header(ctx)
	if err != nil {
		return 0, mysqlError(err)
	}
	n, err := c.AutoIncrement(ctx, &kvrpc.AutoIncReq{H: h, Table: t.info.ID})
	return n, mysqlError(err)
}

// GetNextAutoIncrementValue hands out the next value, or raises the counter
// past an explicitly inserted one. Values within a statement come from one
// growing reservation, so they are consecutive.
func (t *Table) GetNextAutoIncrementValue(ctx *sql.Context, insertVal interface{}) (uint64, error) {
	txn, ok := ctx.GetTransaction().(*Txn)
	if !ok || txn == nil {
		return 0, fmt.Errorf("AUTO_INCREMENT outside a transaction")
	}
	s, _ := ctx.Session.(*Session)
	if insertVal != nil {
		n, err := toUint64(ctx, t.codecs[t.autoOrd].typ, insertVal)
		if err != nil {
			return 0, err
		}
		if s != nil {
			s.dropAutoBatch(t.info.ID)
		}
		_, err = txn.AutoIncrement(ctx, t.info.ID, 0, n+1)
		return n, mysqlError(err)
	}
	if s == nil {
		n, err := txn.AutoIncrement(ctx, t.info.ID, 1, 0)
		return n, mysqlError(err)
	}
	return s.nextAutoValue(t.info.ID, func(count uint64) (uint64, error) {
		n, err := txn.AutoIncrement(ctx, t.info.ID, count, 0)
		return n, mysqlError(err)
	})
}

func (t *Table) AutoIncrementSetter(*sql.Context) sql.AutoIncrementSetter {
	return &autoIncSetter{t: t}
}

type autoIncSetter struct{ t *Table }

// SetAutoIncrementValue is ALTER TABLE ... AUTO_INCREMENT = v; the counter never goes down.
func (a *autoIncSetter) SetAutoIncrementValue(ctx *sql.Context, v uint64) error {
	txn := a.t.store.NewTxn(kv.RepeatableRead, false)
	if _, err := txn.AutoIncrement(ctx, a.t.info.ID, 0, v); err != nil {
		_ = txn.Rollback(ctx)
		return mysqlError(err)
	}
	return mysqlError(txn.Commit(ctx))
}

func (a *autoIncSetter) AcquireAutoIncrementLock(*sql.Context) (func(), error) {
	return func() {}, nil
}

func (a *autoIncSetter) Close(*sql.Context) error { return nil }
