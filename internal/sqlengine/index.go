package sqlengine

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/salesstair-01/001/internal/encoding"
)

// pkIndex exposes the clustered primary key to the planner, so key lookups
// and key ranges become KV Get and Scan calls (pushdown is key ranges only, D7).
type pkIndex struct {
	t *Table
}

var _ sql.Index = (*pkIndex)(nil)

func (i *pkIndex) ID() string       { return "PRIMARY" }
func (i *pkIndex) Database() string { return i.t.info.DB }
func (i *pkIndex) Table() string    { return i.t.info.Name }
func (i *pkIndex) Expressions() []string {
	out := make([]string, len(i.t.pkOrd))
	for n, ord := range i.t.pkOrd {
		out[n] = strings.ToLower(i.t.info.Name) + "." + strings.ToLower(i.t.schema.Schema[ord].Name)
	}
	return out
}
func (i *pkIndex) IsUnique() bool                        { return true }
func (i *pkIndex) IsSpatial() bool                       { return false }
func (i *pkIndex) IsFullText() bool                      { return false }
func (i *pkIndex) IsVector() bool                        { return false }
func (i *pkIndex) Comment() string                       { return "" }
func (i *pkIndex) IndexType() string                     { return "BTREE" }
func (i *pkIndex) IsGenerated() bool                     { return false }
func (i *pkIndex) PrefixLengths() []uint16               { return nil }
func (i *pkIndex) CanSupportOrderBy(sql.Expression) bool { return false }

func (i *pkIndex) ColumnExpressionTypes() []sql.ColumnExpressionType {
	exprs := i.Expressions()
	out := make([]sql.ColumnExpressionType, len(exprs))
	for n, ord := range i.t.pkOrd {
		out[n] = sql.ColumnExpressionType{Expression: exprs[n], Type: i.t.schema.Schema[ord].Type}
	}
	return out
}

func (i *pkIndex) CanSupport(_ *sql.Context, ranges ...sql.Range) bool {
	for _, r := range ranges {
		if _, ok := r.(sql.MySQLRange); !ok {
			return false
		}
	}
	return true
}

func (t *Table) GetIndexes(*sql.Context) ([]sql.Index, error) {
	return []sql.Index{&pkIndex{t: t}}, nil
}

// PreciseMatch is false: key spans may be a superset of the lookup (values
// are converted to the column type), so the planner keeps its filters.
func (t *Table) PreciseMatch() bool { return false }

func (t *Table) IndexedAccess(_ *sql.Context, lookup sql.IndexLookup) sql.IndexedTable {
	if _, ok := lookup.Index.(*pkIndex); !ok {
		return nil
	}
	return &indexedTable{Table: t}
}

type indexedTable struct {
	*Table
}

var _ sql.IndexedTable = (*indexedTable)(nil)

func (it *indexedTable) LookupPartitions(ctx *sql.Context, lookup sql.IndexLookup) (sql.PartitionIter, error) {
	ranges, ok := lookup.Ranges.(sql.MySQLRangeCollection)
	if !ok {
		return nil, fmt.Errorf("unsupported range collection %T", lookup.Ranges)
	}
	part := &kvPartition{key: []byte("lookup")}
	if lookup.IsEmptyRange {
		return &partitionIter{parts: []*kvPartition{part}}, nil
	}
	for _, r := range ranges {
		sp, empty := it.span(ctx, r)
		if !empty {
			part.spans = append(part.spans, sp)
		}
	}
	return &partitionIter{parts: []*kvPartition{part}}, nil
}

// span turns one range into a key span: equal-valued leading columns extend
// the prefix, the first column with a real range sets the bounds, and later
// columns are left to the filters. Bounds are inclusive at the converted
// value, which keeps the span a superset whenever conversion rounds.
func (t *Table) span(ctx *sql.Context, r sql.MySQLRange) (keySpan, bool) {
	prefix := t.prefix()
	enc := func(ord int, v any) ([]byte, bool) {
		b, err := t.codecs[ord].appendKey(ctx, nil, v)
		return b, err == nil
	}
	for n, ord := range t.pkOrd {
		if n >= len(r) {
			break
		}
		ce := r[n]
		if lo, ok := ce.LowerBound.(sql.Below); ok {
			if hi, ok := ce.UpperBound.(sql.Above); ok {
				kl, ok1 := enc(ord, lo.Key)
				kh, ok2 := enc(ord, hi.Key)
				if ok1 && ok2 && bytes.Equal(kl, kh) {
					prefix = append(prefix, kl...)
					continue
				}
			}
		}
		start, end := prefix, encoding.PrefixEnd(prefix)
		switch lo := ce.LowerBound.(type) {
		case sql.Below:
			if k, ok := enc(ord, lo.Key); ok {
				start = append(append([]byte(nil), prefix...), k...)
			}
		case sql.Above:
			if k, ok := enc(ord, lo.Key); ok {
				start = append(append([]byte(nil), prefix...), k...)
			}
		case sql.AboveNull:
			start = append(append([]byte(nil), prefix...), encoding.MarkerValue)
		case sql.AboveAll:
			return keySpan{}, true
		}
		switch hi := ce.UpperBound.(type) {
		case sql.Below:
			if k, ok := enc(ord, hi.Key); ok {
				end = encoding.PrefixEnd(append(append([]byte(nil), prefix...), k...))
			}
		case sql.Above:
			if k, ok := enc(ord, hi.Key); ok {
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
	if len(r) >= len(t.pkOrd) {
		return keySpan{start: prefix, point: true}, false
	}
	return keySpan{start: prefix, end: encoding.PrefixEnd(prefix)}, false
}
