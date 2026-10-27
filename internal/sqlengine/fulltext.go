package sqlengine

import (
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/fulltext"
)

// FULLTEXT indexes and MATCH ... AGAINST. go-mysql-server implements them
// over tables of its own per index (the words, their positions and counts)
// and a config table per parent table, which it creates through the
// database and fills on CREATE INDEX; the table's writers keep them up to
// date through its fulltext editor. sqld names those tables s2z_ft_..., keeps
// them out of table listings, and records the index in the parent's catalog
// row. A FULLTEXT index has no KV entries of its own. Not covered: rows
// changed by a foreign key's cascade, and a multi-table UPDATE of a table
// without a primary key.

const fulltextTablePrefix = "s2z_ft_"

// FulltextInfo is where a FULLTEXT index lives: go-mysql-server's tables and
// the parent's key columns it identifies rows by.
type FulltextInfo struct {
	Tables       fulltext.IndexTableNames `json:"tables"`
	KeyType      fulltext.KeyType         `json:"key_type"`
	KeyName      string                   `json:"key_name,omitempty"`
	KeyPositions []int                    `json:"key_positions,omitempty"`
}

func isFulltextTable(name string) bool {
	return strings.HasPrefix(strings.ToLower(name), fulltextTablePrefix)
}

var (
	_ fulltext.Database            = (*Database)(nil)
	_ fulltext.IndexAlterableTable = (*Table)(nil)
	_ fulltext.Index               = (*fulltextIndex)(nil)
)

// CreateFulltextTableNames names the tables of a new FULLTEXT index.
func (d *Database) CreateFulltextTableNames(ctx *sql.Context, parent, index string) (fulltext.IndexTableNames, error) {
	parent, index = strings.ToLower(parent), strings.ToLower(index)
	var prefix string
	for n := 0; ; n++ {
		prefix = fmt.Sprintf("%s%s_%s_%d", fulltextTablePrefix, parent, index, n)
		info, err := d.cat.Table(ctx, d.name, prefix+"_position")
		if err != nil {
			return fulltext.IndexTableNames{}, err
		}
		if info == nil {
			break
		}
	}
	return fulltext.IndexTableNames{
		Config:      fulltextTablePrefix + parent + "_config",
		Position:    prefix + "_position",
		DocCount:    prefix + "_doc_count",
		GlobalCount: prefix + "_global_count",
		RowCount:    prefix + "_row_count",
	}, nil
}

// CreateFulltextIndex records the index; go-mysql-server has created and
// fills its tables.
func (t *Table) CreateFulltextIndex(ctx *sql.Context, def sql.IndexDef, keyCols fulltext.KeyColumns, names fulltext.IndexTableNames) error {
	info := IndexInfo{Name: def.Name, Comment: def.Comment, Fulltext: &FulltextInfo{
		Tables: names, KeyType: keyCols.Type, KeyName: keyCols.Name, KeyPositions: keyCols.Positions,
	}}
	for _, c := range def.Columns {
		ord := t.ordinal(c.Name)
		if ord < 0 {
			return sql.ErrKeyColumnDoesNotExist.New(c.Name)
		}
		info.Columns = append(info.Columns, IndexColumn{ColID: t.codecs[ord].info.ID})
	}
	if info.Name == "" {
		info.Name = t.indexName(def.Columns[0].Name)
	}
	if strings.EqualFold(info.Name, "PRIMARY") || t.findIndex(info.Name) >= 0 {
		return sql.ErrDuplicateKey.New(info.Name)
	}
	err := t.cat.AlterTable(ctx, t.info, func(_ *Txn, ti *TableInfo) error {
		if ti.NextIndexID == 0 {
			ti.NextIndexID = 1
		}
		info.ID = ti.NextIndexID
		ti.NextIndexID++
		ti.Indexes = append(ti.Indexes, info)
		return nil
	})
	if err != nil {
		return err
	}
	// go-mysql-server fills the index's tables through this table object
	return t.reloadIndexes(append(append([]IndexInfo(nil), t.info.Indexes...), info))
}

func (t *Table) dropFulltextIndex(ctx *sql.Context, id uint32) error {
	err := t.cat.AlterTable(ctx, t.info, func(_ *Txn, ti *TableInfo) error {
		kept := ti.Indexes[:0]
		for _, ix := range ti.Indexes {
			if ix.ID != id {
				kept = append(kept, ix)
			}
		}
		ti.Indexes = kept
		return nil
	})
	if err != nil {
		return err
	}
	var kept []IndexInfo
	for _, ix := range t.info.Indexes {
		if ix.ID != id {
			kept = append(kept, ix)
		}
	}
	return t.reloadIndexes(kept)
}

// reloadIndexes sets the table object's FULLTEXT indexes from indexes.
func (t *Table) reloadIndexes(indexes []IndexInfo) error {
	info := *t.info
	info.Indexes = indexes
	t.info = &info
	t.fulltext = nil
	for i := range info.Indexes {
		if info.Indexes[i].Fulltext == nil {
			continue
		}
		ft, err := newFulltextIndex(t, &info.Indexes[i])
		if err != nil {
			return err
		}
		t.fulltext = append(t.fulltext, ft)
	}
	return nil
}

// fulltextEditor is ed, keeping the table's FULLTEXT indexes up to date.
func (t *Table) fulltextEditor(ctx *sql.Context, ed sql.TableEditor) sql.TableEditor {
	fail := func(err error) sql.TableEditor { return &failedEditor{err: err} }
	db := &Database{cat: t.cat, name: t.info.DB}
	get := func(name string) (fulltext.EditableTable, error) {
		tbl, ok, err := db.GetTableInsensitive(ctx, name)
		if err != nil {
			return nil, err
		}
		et, isEditable := tbl.(fulltext.EditableTable)
		if !ok || !isEditable {
			return nil, fmt.Errorf("FULLTEXT table %s of %s is missing", name, t.info.Name)
		}
		return et, nil
	}
	var sets []fulltext.TableSet
	var config fulltext.EditableTable
	for _, ft := range t.fulltext {
		n := ft.info.Fulltext.Tables
		var set fulltext.TableSet
		var err error
		set.Index = ft
		if config == nil {
			if config, err = get(n.Config); err != nil {
				return fail(err)
			}
		}
		if set.Position, err = get(n.Position); err != nil {
			return fail(err)
		}
		if set.DocCount, err = get(n.DocCount); err != nil {
			return fail(err)
		}
		if set.GlobalCount, err = get(n.GlobalCount); err != nil {
			return fail(err)
		}
		if set.RowCount, err = get(n.RowCount); err != nil {
			return fail(err)
		}
		sets = append(sets, set)
	}
	ftEd, err := fulltext.CreateEditor(ctx, t, config, sets...)
	if err != nil {
		return fail(err)
	}
	multi, err := fulltext.CreateMultiTableEditor(ctx, ed, ftEd)
	if err != nil {
		return fail(err)
	}
	return multi
}

// ftParentEditor is a table's editor as go-mysql-server's fulltext editor
// needs it: addressable by index, and setting AUTO_INCREMENT values.
type ftParentEditor struct {
	*fkEditor
	sql.AutoIncrementSetter
}

// failedEditor reports an error that kept the editor from being made.
type failedEditor struct{ err error }

func (e *failedEditor) StatementBegin(*sql.Context)                 {}
func (e *failedEditor) DiscardChanges(*sql.Context, error) error    { return nil }
func (e *failedEditor) StatementComplete(*sql.Context) error        { return e.err }
func (e *failedEditor) Insert(*sql.Context, sql.Row) error          { return e.err }
func (e *failedEditor) Update(*sql.Context, sql.Row, sql.Row) error { return e.err }
func (e *failedEditor) Delete(*sql.Context, sql.Row) error          { return e.err }
func (e *failedEditor) Close(*sql.Context) error                    { return e.err }

// fulltextIndex is a FULLTEXT index as go-mysql-server sees it.
type fulltextIndex struct {
	t    *Table
	info *IndexInfo
	ords []int
}

func newFulltextIndex(t *Table, info *IndexInfo) (*fulltextIndex, error) {
	ft := &fulltextIndex{t: t, info: info}
	for _, c := range info.Columns {
		ord, ok := t.colOrd[c.ColID]
		if !ok {
			return nil, fmt.Errorf("index %s: unknown column id %d", info.Name, c.ColID)
		}
		ft.ords = append(ft.ords, ord)
	}
	return ft, nil
}

func (i *fulltextIndex) ID() string       { return i.info.Name }
func (i *fulltextIndex) Database() string { return i.t.info.DB }
func (i *fulltextIndex) Table() string    { return i.t.info.Name }
func (i *fulltextIndex) Expressions() []string {
	out := make([]string, len(i.ords))
	for n, ord := range i.ords {
		out[n] = strings.ToLower(i.t.info.Name) + "." + strings.ToLower(i.t.schema.Schema[ord].Name)
	}
	return out
}
func (i *fulltextIndex) IsUnique() bool                             { return false }
func (i *fulltextIndex) IsSpatial() bool                            { return false }
func (i *fulltextIndex) IsFullText() bool                           { return true }
func (i *fulltextIndex) IsVector() bool                             { return false }
func (i *fulltextIndex) Comment() string                            { return i.info.Comment }
func (i *fulltextIndex) IndexType() string                          { return "FULLTEXT" }
func (i *fulltextIndex) IsGenerated() bool                          { return false }
func (i *fulltextIndex) CanSupport(*sql.Context, ...sql.Range) bool { return false }
func (i *fulltextIndex) CanSupportOrderBy(sql.Expression) bool      { return false }
func (i *fulltextIndex) PrefixLengths() []uint16                    { return nil }

func (i *fulltextIndex) ColumnExpressionTypes() []sql.ColumnExpressionType {
	exprs := i.Expressions()
	out := make([]sql.ColumnExpressionType, len(exprs))
	for n, ord := range i.ords {
		out[n] = sql.ColumnExpressionType{Expression: exprs[n], Type: i.t.schema.Schema[ord].Type}
	}
	return out
}

func (i *fulltextIndex) FullTextTableNames(*sql.Context) (fulltext.IndexTableNames, error) {
	return i.info.Fulltext.Tables, nil
}

func (i *fulltextIndex) FullTextKeyColumns(*sql.Context) (fulltext.KeyColumns, error) {
	f := i.info.Fulltext
	return fulltext.KeyColumns{Type: f.KeyType, Name: f.KeyName, Positions: f.KeyPositions}, nil
}
