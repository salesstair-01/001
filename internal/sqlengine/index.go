package sqlengine

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/salesstair-01/001/internal/encoding"
)

// tableIndex exposes the primary key (sec == nil) or a secondary index to the
// planner, so key lookups and ranges become KV Get and Scan calls (pushdown is
// key ranges only, D7).
type tableIndex struct {
	t   *Table
	sec *secIndex
}

var _ sql.OrderedIndex = (*tableIndex)(nil)

// Order is ascending: keys are memcomparable in the column's collation, NULL
// first, and lookups read their (sorted, merged) spans in key order.
func (i *tableIndex) Order() sql.IndexOrder { return sql.IndexOrderAsc }

// Reversible is false: KV scans run forward only. go-mysql-server treats an
// index without OrderedIndex as reversible and would drop the sort of an
// ORDER BY ... DESC.
func (i *tableIndex) Reversible() bool { return false }

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

// GetIndexes lists the primary key and the public secondary indexes; indexes
// being built or dropped are maintained by writers but never read.
func (t *Table) GetIndexes(*sql.Context) ([]sql.Index, error) {
	var out []sql.Index
	if !t.keyless {
		out = append(out, &tableIndex{t: t})
	}
	for _, ix := range t.indexes {
		if ix.info.State == "" {
			out = append(out, &tableIndex{t: t, sec: ix})
		}
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
	part.spans = mergeSpans(part.spans)
	return &partitionIter{parts: []*kvPartition{part}}, nil
}

// mergeSpans sorts spans and merges the overlapping ones. Disjoint ranges
// can give overlapping spans, since each span is widened to a superset; read
// separately, a row in the overlap would be returned twice.
func mergeSpans(spans []keySpan) []keySpan {
	if len(spans) < 2 {
		return spans
	}
	end := func(s keySpan) []byte {
		if s.point {
			// Just past the key: keys are prefix-free (D2), so no other key
			// lies in [key, PrefixEnd(key)). key+"\x00" would not do: the
			// owner compares MVCC keys bytewise, and key@ts sorts after it.
			return encoding.PrefixEnd(s.start)
		}
		return s.end
	}
	// less reports a < b for span ends, where nil is +infinity.
	less := func(a, b []byte) bool {
		if a == nil {
			return false
		}
		return b == nil || bytes.Compare(a, b) < 0
	}
	sort.Slice(spans, func(i, j int) bool { return bytes.Compare(spans[i].start, spans[j].start) < 0 })
	out := []keySpan{spans[0]}
	for _, s := range spans[1:] {
		cur := &out[len(out)-1]
		ce := end(*cur)
		if ce != nil && bytes.Compare(s.start, ce) >= 0 {
			out = append(out, s)
			continue
		}
		if cur.point && s.point && bytes.Equal(cur.start, s.start) {
			continue // the same row twice
		}
		e := end(s)
		if less(ce, e) {
			ce = e
		}
		*cur = keySpan{start: cur.start, end: ce}
	}
	return out
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
