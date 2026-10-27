package plan

import (
	"fmt"
	"io"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// TableCopier is a supporting node that allows for the optimization of copying tables. It should be used in two cases.
// 1) CREATE TABLE SELECT *
// 2) INSERT INTO SELECT * where the inserted table is empty. // TODO: Implement this optimization
type TableCopier struct {
	Source      sql.Node
	Destination sql.Node
	db          sql.Database
	options     CopierProps
}

var _ sql.Databaser = (*TableCopier)(nil)
var _ sql.Node = (*TableCopier)(nil)
var _ sql.CollationCoercible = (*TableCopier)(nil)

type CopierProps struct {
	replace bool
	ignore  bool
}

// NewCopierProps is a copy that replaces or skips rows with a duplicate key
// (s2z patch: CREATE TABLE ... REPLACE | IGNORE SELECT).
func NewCopierProps(replace, ignore bool) CopierProps {
	return CopierProps{replace: replace, ignore: ignore}
}

func NewTableCopier(db sql.Database, createTableNode sql.Node, source sql.Node, prop CopierProps) *TableCopier {
	return &TableCopier{
		Source:      source,
		Destination: createTableNode,
		db:          db,
		options:     prop,
	}
}

func (tc *TableCopier) WithDatabase(db sql.Database) (sql.Node, error) {
	ntc := *tc
	ntc.db = db
	return &ntc, nil
}

func (tc *TableCopier) IsReadOnly() bool {
	return false
}

func (tc *TableCopier) Database() sql.Database {
	return tc.db
}

func (tc *TableCopier) ProcessCreateTable(ctx *sql.Context, b sql.NodeExecBuilder, row sql.Row) (sql.RowIter, error) {
	ct := tc.Destination.(*CreateTable)

	_, err := b.Build(ctx, ct, row)
	if err != nil {
		return sql.RowsToRowIter(), err
	}

	table, tableExists, err := tc.db.GetTableInsensitive(ctx, ct.Name())
	if err != nil {
		return sql.RowsToRowIter(), err
	}

	if !tableExists {
		return sql.RowsToRowIter(), fmt.Errorf("error: Newly created table does not exist")
	}

	// s2z patch: the table's CHECK constraints hold for the SELECT's rows
	// (they were not evaluated: CREATE TABLE t (b int CHECK (b > 0))
	// SELECT 0 AS b made the table); a direct copy skips them.
	checks := ct.Checks()
	if len(checks) == 0 && tc.createTableSelectCanBeCopied(table) {
		return tc.CopyTableOver(ctx, tc.Source.Schema()[0].Source, table.Name())
	}

	// TODO: Improve parsing for CREATE TABLE SELECT to allow for IGNORE/REPLACE and custom specs
	var ii sql.Node = NewInsertInto(tc.db, NewResolvedTable(table, tc.db, nil), tc.Source, tc.options.replace, nil, nil, tc.options.ignore)
	if len(checks) > 0 {
		ii = ii.(*InsertInto).WithChecks(checks)
	}

	it, err := b.Build(ctx, ii, row)
	if err != nil {
		tc.dropCreated(ctx, ct.Name())
		return nil, err
	}
	return &createSelectIter{RowIter: it, drop: func() { tc.dropCreated(ctx, ct.Name()) }}, nil
}

// s2z patch: a CREATE TABLE ... SELECT whose rows fail (a CHECK, a
// duplicate key) leaves no table, as in MySQL.
func (tc *TableCopier) dropCreated(ctx *sql.Context, name string) {
	db := tc.db
	if privDb, ok := db.(mysql_db.PrivilegedDatabase); ok {
		db = privDb.Unwrap()
	}
	if d, ok := db.(sql.TableDropper); ok {
		_ = d.DropTable(ctx, name)
	}
}

type createSelectIter struct {
	sql.RowIter
	drop   func()
	failed bool
}

var _ sql.MutableRowIter = (*createSelectIter)(nil)

// GetChildIter and WithChildIter let the DML accumulator see the insert
// iterator under it.
func (i *createSelectIter) GetChildIter() sql.RowIter { return i.RowIter }

func (i *createSelectIter) WithChildIter(child sql.RowIter) sql.RowIter {
	ni := *i
	ni.RowIter = child
	return &ni
}

func (i *createSelectIter) Next(ctx *sql.Context) (sql.Row, error) {
	r, err := i.RowIter.Next(ctx)
	if err != nil && err != io.EOF && !i.failed {
		i.failed = true
		i.drop()
	}
	return r, err
}

// createTableSelectCanBeCopied determines whether the newly created table's data can just be copied from the Source table
func (tc *TableCopier) createTableSelectCanBeCopied(tableNode sql.Table) bool {
	// The differences in LIMIT between integrators prevent us from using a copy
	if _, ok := tc.Source.(*Limit); ok {
		return false
	}

	// If the DB does not implement the TableCopierDatabase interface we cannot copy over the table.
	if privDb, ok := tc.db.(mysql_db.PrivilegedDatabase); ok {
		if _, ok := privDb.Unwrap().(sql.TableCopierDatabase); !ok {
			return false
		}
	} else if _, ok := tc.db.(sql.TableCopierDatabase); !ok {
		return false
	}

	// If there isn't a match in schema we cannot do a direct copy.
	sourceSchema := tc.Source.Schema()
	tableNodeSchema := tableNode.Schema()

	if len(sourceSchema) != len(tableNodeSchema) {
		return false
	}

	for i, sn := range sourceSchema {
		if sn.Name != tableNodeSchema[i].Name {
			return false
		}
	}

	return true
}

// CopyTableOver is used when we can guarantee the Destination table will have the same data as the source table.
func (tc *TableCopier) CopyTableOver(ctx *sql.Context, sourceTable string, destinationTable string) (sql.RowIter, error) {
	db, ok := tc.db.(sql.TableCopierDatabase)
	if !ok {
		return sql.RowsToRowIter(), sql.ErrTableCopyingNotSupported.New()
	}

	rowsUpdated, err := db.CopyTableData(ctx, sourceTable, destinationTable)
	if err != nil {
		return sql.RowsToRowIter(), err
	}

	return sql.RowsToRowIter([]sql.Row{{types.OkResult{RowsAffected: rowsUpdated, InsertID: 0, Info: nil}}}...), nil
}

func (tc *TableCopier) Schema() sql.Schema {
	return tc.Destination.Schema()
}

func (tc *TableCopier) Children() []sql.Node {
	return nil
}

func (tc *TableCopier) WithChildren(...sql.Node) (sql.Node, error) {
	return tc, nil
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*TableCopier) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 7
}

func (tc *TableCopier) Resolved() bool {
	return tc.Source.Resolved()
}

func (tc *TableCopier) String() string {
	return fmt.Sprintf("TABLE_COPY SRC: %s into DST: %s", tc.Source, tc.Destination)
}
