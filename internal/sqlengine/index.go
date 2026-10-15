package sqlengine

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/salesstair-01/001/internal/encoding"
	"github.com/salesstair-01/001/internal/kv"
	"github.com/salesstair-01/001/internal/kvrpc"
)

// tableIndex exposes the primary key (sec == nil) or a secondary index to the
// planner, so key lookups and ranges become KV Get and Scan calls (pushdown is
// key ranges only, D7).
type tableIndex struct {
	t   *Table
	sec *secIndex
}

var _ sql.Index = (*tableIndex)(nil)

func (i *tableIndex) ords() []int {
	if i.sec == nil {
		return i.t.pkOrd
	}
	return i.sec.ords
}

func (i *tableIndex) ID() string {
	if i.sec == nil {
		return "PRIMARY"
	}
	return i.sec.info.Name
}
func (i *tableIndex) Database() string { return i.t.info.DB }
func (i *tableIndex) Table() string    { return i.t.info.Name }
func (i *tableIndex) Expressions() []string {
	out := make([]string, len(i.ords()))
	for n, ord := range i.ords() {
		out[n] = strings.ToLower(i.t.info.Name) + "." + strings.ToLower(i.t.schema.Schema[ord].Name)
	}
	return out
}
func (i *tableIndex) IsUnique() bool                        { return i.sec == nil || i.sec.info.Unique }
func (i *tableIndex) IsSpatial() bool                       { return false }
func (i *tableIndex) IsFullText() bool                      { return false }
func (i *tableIndex) IsVector() bool                        { return false }
func (i *tableIndex) IndexType() string                     { return "BTREE" }
func (i *tableIndex) IsGenerated() bool                     { return false }
func (i *tableIndex) CanSupportOrderBy(sql.Expression) bool { return false }

func (i *tableIndex) Comment() string {
	if i.sec == nil {
		return ""
	}
	return i.sec.info.Comment
}

func (i *tableIndex) PrefixLengths() []uint16 {
	if i.sec == nil {
		return nil
	}
	out := make([]uint16, len(i.sec.prefix))
	any := false
	for n, p := range i.sec.prefix {
		out[n] = uint16(p)
		any = any || p > 0
	}
	if !any {
		return nil
	}
	return out
}

func (i *tableIndex) ColumnExpressionTypes() []sql.ColumnExpressionType {
	exprs := i.Expressions()
	out := make([]sql.ColumnExpressionType, len(exprs))
	for n, ord := range i.ords() {
		out[n] = sql.ColumnExpressionType{Expression: exprs[n], Type: i.t.schema.Schema[ord].Type}
	}
	return out
}

func (i *tableIndex) CanSupport(_ *sql.Context, ranges ...sql.Range) bool {
	for _, r := range ranges {
		if _, ok := r.(sql.MySQLRange); !ok {
			return false
		}
	}
	return true
}

func (t *Table) GetIndexes(*sql.Context) ([]sql.Index, error) {
	out := []sql.Index{&tableIndex{t: t}}
	for _, ix := range t.indexes {
		out = append(out, &tableIndex{t: t, sec: ix})
	}
	return out, nil
}

// PreciseMatch is false: key spans may be a superset of the lookup (values
// are converted to the column type, prefixes truncated), so filters stay.
func (t *Table) PreciseMatch() bool { return false }

func (t *Table) IndexedAccess(_ *sql.Context, lookup sql.IndexLookup) sql.IndexedTable {
	if _, ok := lookup.Index.(*tableIndex); !ok {
		return nil
	}
	return &indexedTable{Table: t}
}

type indexedTable struct {
	*Table
}

var _ sql.IndexedTable = (*indexedTable)(nil)

func (it *indexedTable) LookupPartitions(ctx *sql.Context, lookup sql.IndexLookup) (sql.PartitionIter, error) {
	idx := lookup.Index.(*tableIndex)
	ranges, ok := lookup.Ranges.(sql.MySQLRangeCollection)
	if !ok {
		return nil, fmt.Errorf("unsupported range collection %T", lookup.Ranges)
	}
	part := &kvPartition{key: []byte("lookup"), index: idx.sec}
	if lookup.IsEmptyRange {
		return &partitionIter{parts: []*kvPartition{part}}, nil
	}
	for _, r := range ranges {
		sp, empty := it.span(ctx, idx, r)
		if !empty {
			part.spans = append(part.spans, sp)
		}
	}
	return &partitionIter{parts: []*kvPartition{part}}, nil
}

// span turns one range into a key span: equal-valued leading columns extend
// the prefix, the first column with a real range sets the bounds, and later
// columns are left to the filters. Bounds are inclusive at the converted (and
// prefix-truncated) value, which keeps the span a superset of the range.
func (t *Table) span(ctx *sql.Context, idx *tableIndex, r sql.MySQLRange) (keySpan, bool) {
	var prefix []byte
	var prefixLens []int
	if idx.sec == nil {
		prefix = t.prefix()
	} else {
		prefix, prefixLens = idx.sec.keyPrefix(), idx.sec.prefix
	}
	ords := idx.ords()
	enc := func(n, ord int, v any) ([]byte, bool) {
		c := &t.codecs[ord]
		if prefixLens != nil {
			v = truncate(c.typ, v, prefixLens[n])
		}
		b, err := c.appendKey(ctx, nil, v)
		return b, err == nil
	}
	for n, ord := range ords {
		if n >= len(r) {
			break
		}
		ce := r[n]
		if lo, ok := ce.LowerBound.(sql.Below); ok {
			if hi, ok := ce.UpperBound.(sql.Above); ok {
				kl, ok1 := enc(n, ord, lo.Key)
				kh, ok2 := enc(n, ord, hi.Key)
				if ok1 && ok2 && bytes.Equal(kl, kh) {
					prefix = append(prefix, kl...)
					continue
				}
			}
		}
		// IS NULL on this column: the NULL marker extends the prefix.
		if _, ok := ce.LowerBound.(sql.BelowNull); ok {
			if _, ok := ce.UpperBound.(sql.AboveNull); ok {
				prefix = encoding.AppendNull(prefix, false)
				continue
			}
		}
		start, end := prefix, encoding.PrefixEnd(prefix)
		switch lo := ce.LowerBound.(type) {
		case sql.Below:
			if k, ok := enc(n, ord, lo.Key); ok {
				start = append(append([]byte(nil), prefix...), k...)
			}
		case sql.Above:
			if k, ok := enc(n, ord, lo.Key); ok {
				start = append(append([]byte(nil), prefix...), k...)
			}
		case sql.AboveNull:
			start = append(append([]byte(nil), prefix...), encoding.MarkerValue)
		case sql.AboveAll:
			return keySpan{}, true
		}
		switch hi := ce.UpperBound.(type) {
		case sql.Below:
			if k, ok := enc(n, ord, hi.Key); ok {
				end = encoding.PrefixEnd(append(append([]byte(nil), prefix...), k...))
			}
		case sql.Above:
			if k, ok := enc(n, ord, hi.Key); ok {
				end = encoding.PrefixEnd(append(append([]byte(nil), prefix...), k...))
			}
		case sql.AboveNull:
			end = append(append([]byte(nil), prefix...), encoding.MarkerValue)
		case sql.BelowNull:
			return keySpan{}, true
		}
		if bytes.Compare(start, end) >= 0 {
			return keySpan{}, true
		}
		return keySpan{start: start, end: end}, false
	}
	// A full primary key is one row. Secondary entries may carry the primary
	// key after the index columns (non-unique, or NULL), so they are scanned.
	if idx.sec == nil && len(r) >= len(ords) {
		return keySpan{start: prefix, point: true}, false
	}
	return keySpan{start: prefix, end: encoding.PrefixEnd(prefix)}, false
}

// ---- CREATE / DROP / RENAME INDEX --------------------------------------------------

func (t *Table) findIndex(name string) int {
	for i := range t.info.Indexes {
		if strings.EqualFold(t.info.Indexes[i].Name, name) {
			return i
		}
	}
	return -1
}

// CreateIndex adds a secondary index and backfills it in the same DDL
// transaction (bounded by the 64 MiB transaction cap until the resumable
// backfill job of slice 8).
func (t *Table) CreateIndex(ctx *sql.Context, def sql.IndexDef) error {
	switch def.Constraint {
	case sql.IndexConstraint_Fulltext, sql.IndexConstraint_Spatial, sql.IndexConstraint_Vector:
		return fmt.Errorf("FULLTEXT, SPATIAL and VECTOR indexes are not supported in v1")
	case sql.IndexConstraint_Primary:
		return fmt.Errorf("changing the primary key is not supported in v1")
	}
	info := IndexInfo{Name: def.Name, Unique: def.IsUnique(), Comment: def.Comment}
	for _, c := range def.Columns {
		ord := -1
		for i, col := range t.schema.Schema {
			if strings.EqualFold(col.Name, c.Name) {
				ord = i
			}
		}
		if ord < 0 {
			return sql.ErrKeyColumnDoesNotExist.New(c.Name)
		}
		col := t.schema.Schema[ord]
		if !keyable(col.Type) {
			return fmt.Errorf("column %s: type %s cannot be indexed in v1", col.Name, col.Type)
		}
		if c.Length > 0 && !(types.IsText(col.Type) || types.IsBinaryType(col.Type)) {
			return sql.ErrInvalidIndexPrefix.New(col.Name)
		}
		if c.Length == 0 && types.IsTextBlob(col.Type) {
			return sql.ErrInvalidBlobTextKey.New(col.Name)
		}
		info.Columns = append(info.Columns, IndexColumn{ColID: t.codecs[ord].info.ID, Prefix: int(c.Length)})
	}
	if info.Name == "" {
		info.Name = t.indexName(def.Columns[0].Name)
	}
	if strings.EqualFold(info.Name, "PRIMARY") || t.findIndex(info.Name) >= 0 {
		return sql.ErrDuplicateKey.New(info.Name)
	}
	return t.cat.AlterTable(ctx, t.info, func(txn *Txn, ti *TableInfo) error {
		if ti.NextIndexID == 0 {
			ti.NextIndexID = 1
		}
		info.ID = ti.NextIndexID
		ti.NextIndexID++
		ti.Indexes = append(ti.Indexes, info)
		nt, err := newTable(ctx, t.cat, ti)
		if err != nil {
			return err
		}
		return nt.backfill(ctx, txn, nt.indexes[len(nt.indexes)-1])
	})
}

// indexName picks MySQL's default name for an index: its first column, then col_2, col_3, ...
func (t *Table) indexName(col string) string {
	name := col
	for n := 2; t.findIndex(name) >= 0 || strings.EqualFold(name, "PRIMARY"); n++ {
		name = fmt.Sprintf("%s_%d", col, n)
	}
	return name
}

// backfill writes the index entries of every existing row.
func (t *Table) backfill(ctx *sql.Context, txn *Txn, ix *secIndex) error {
	start, end := t.prefix(), encoding.PrefixEnd(t.prefix())
	for {
		resp, err := txn.Scan(ctx, start, end, scanPage, kvrpc.ReadOpts{Stmt: 1})
		if err != nil {
			return err
		}
		var muts []kv.Mutation
		for i, v := range resp.Values {
			row, err := t.decodeRow(ctx, v)
			if err != nil {
				return err
			}
			k, val, uniq, err := ix.entry(ctx, row, resp.Keys[i])
			if err != nil {
				return err
			}
			muts = append(muts, kv.Mutation{Op: entryOp(uniq), Key: k, Value: val})
		}
		if err := txn.Mutate(ctx, 2, muts, lockWait(ctx)); err != nil {
			if errIsKind(err, kv.KindDuplicateKey) {
				return sql.ErrDuplicateEntry.New(ix.info.Name)
			}
			return err
		}
		if len(resp.Resume) == 0 {
			return nil
		}
		start = resp.Resume
	}
}

// DropIndex removes an index from the catalog. Its entries are left behind;
// the index id is never reused.
func (t *Table) DropIndex(ctx *sql.Context, name string) error {
	if t.findIndex(name) < 0 {
		return sql.ErrIndexNotFound.New(name)
	}
	return t.cat.AlterTable(ctx, t.info, func(_ *Txn, ti *TableInfo) error {
		i := (&Table{info: ti}).findIndex(name)
		ti.Indexes = append(ti.Indexes[:i], ti.Indexes[i+1:]...)
		return nil
	})
}

func (t *Table) RenameIndex(ctx *sql.Context, from, to string) error {
	if t.findIndex(from) < 0 {
		return sql.ErrIndexNotFound.New(from)
	}
	if t.findIndex(to) >= 0 && !strings.EqualFold(from, to) {
		return sql.ErrDuplicateKey.New(to)
	}
	return t.cat.AlterTable(ctx, t.info, func(_ *Txn, ti *TableInfo) error {
		ti.Indexes[(&Table{info: ti}).findIndex(from)].Name = to
		return nil
	})
}
