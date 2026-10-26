package sqlengine

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/vt/sqlparser"
)

// MySQL 8 warns (1681) about deprecated column type attributes in CREATE
// TABLE and ALTER TABLE: integer display widths, ZEROFILL, digits for FLOAT
// and DOUBLE, UNSIGNED for DECIMAL, FLOAT and DOUBLE. go-mysql-server
// accepts them silently, and its schema no longer tells an explicit display
// width from the default, so a pre-analyze rule parses the statement's text
// again and warns as MySQL does, one warning per column and attribute (issue
// #28; the warnings most often missing in MySQL's test suite, see mtr/).

const ddlWarningsID analyzer.RuleId = 10004

const (
	warnDisplayWidth = "Integer display width is deprecated and will be removed in a future release."
	warnZerofill     = "The ZEROFILL attribute is deprecated and will be removed in a future release. Use the LPAD function to zero-pad numbers, or store the formatted numbers in a CHAR column."
	warnFloatDigits  = "Specifying number of digits for floating point data types is deprecated and will be removed in a future release."
	warnUnsigned     = "UNSIGNED for decimal and floating point data types is deprecated and support for it will be removed in a future release."
)

// ddlStatement matches the start of a CREATE TABLE or ALTER TABLE, after
// comments and whitespace; other statements are not parsed again.
var ddlStatement = regexp.MustCompile(`(?is)^(\s+|/\*.*?\*/|(#|-- )[^\n]*\n)*(create\s+(temporary\s+)?table|alter\s+table)\s`)

func ddlWarnings(ctx *sql.Context, a *analyzer.Analyzer, n sql.Node, scope *plan.Scope, sel analyzer.RuleSelector, qf *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	if !scope.IsEmpty() {
		return n, transform.SameTree, nil
	}
	// MySQL warns about column types while parsing, before anything else.
	if ddlStatement.MatchString(ctx.Query()) {
		if st, err := sqlparser.Parse(ctx.Query()); err == nil {
			for _, c := range columnDefinitions(st) {
				if strings.EqualFold(c.Type.Type, "year") && c.Type.Length != nil {
					// YEAR(4) is deprecated; other widths are refused.
					if string(c.Type.Length.Val) != "4" {
						return nil, transform.SameTree, mysql.NewSQLError(1818, "HY000", "Invalid display width. Use YEAR instead.")
					}
					ctx.Warn(1287, "'YEAR(4)' is deprecated and will be removed in a future release. Please use YEAR instead")
				}
				for _, w := range columnTypeWarnings(&c.Type) {
					ctx.Warn(1681, "%s", w)
				}
			}
		}
	}
	lexWarnings(ctx)
	tableNotes(ctx, n)
	return n, transform.SameTree, nil
}

func columnDefinitions(st sqlparser.Statement) []*sqlparser.ColumnDefinition {
	var cols []*sqlparser.ColumnDefinition
	add := func(d *sqlparser.DDL) {
		if d != nil && d.TableSpec != nil {
			cols = append(cols, d.TableSpec.Columns...)
		}
	}
	switch s := st.(type) {
	case *sqlparser.DDL:
		add(s)
	case *sqlparser.AlterTable:
		for _, d := range s.Statements {
			add(d)
		}
	}
	return cols
}

// columnTypeWarnings is what MySQL 8.0 warns about a column type, in its
// order. A signed TINYINT(1) (BOOL) keeps its display width silently.
func columnTypeWarnings(t *sqlparser.ColumnType) []string {
	var ws []string
	switch typ := strings.ToLower(t.Type); typ {
	case "tinyint", "smallint", "mediumint", "int", "integer", "bigint":
		boolean := typ == "tinyint" && !bool(t.Unsigned) && !bool(t.Zerofill) && t.Length != nil && string(t.Length.Val) == "1"
		if (t.Length != nil || bool(t.Zerofill)) && !boolean {
			ws = append(ws, warnDisplayWidth)
		}
	case "float", "double", "real", "double precision":
		if t.Scale != nil {
			ws = append(ws, warnFloatDigits)
		}
		if bool(t.Unsigned) {
			ws = append(ws, warnUnsigned)
		}
	case "decimal", "numeric", "dec", "fixed":
		if bool(t.Unsigned) {
			ws = append(ws, warnUnsigned)
		}
	}
	if bool(t.Zerofill) {
		ws = append(ws, warnZerofill)
	}
	return ws
}

// tableNotes adds MySQL's notes and warnings about the tables and indexes
// a statement creates: 1050 for CREATE TABLE IF NOT EXISTS on an existing
// table, and 1831 for an index that repeats another of the same kind (same
// columns and prefix lengths, both unique or both not; the primary key is
// not compared).
func tableNotes(ctx *sql.Context, n sql.Node) {
	switch n := n.(type) {
	case *plan.CreateTable:
		if db := n.Database(); n.IfNotExists() && db != nil {
			if _, ok, err := db.GetTableInsensitive(ctx, n.Name()); err == nil && ok {
				ctx.Session.Warn(&sql.Warning{Level: "Note", Code: 1050, Message: fmt.Sprintf("Table '%s' already exists", n.Name())})
				return
			}
		}
		var keys indexKeys
		for _, d := range n.Indexes() {
			if d.Constraint == sql.IndexConstraint_Primary || d.Constraint == sql.IndexConstraint_Fulltext || d.Constraint == sql.IndexConstraint_Spatial {
				continue
			}
			keys.add(ctx, dbName(n.Database()), n.Name(), d.Name, d.Constraint == sql.IndexConstraint_Unique, d.Columns)
		}
	default:
		// ALTER TABLE ... ADD INDEX and CREATE INDEX: against the table's
		// indexes and the statement's earlier ones.
		var keys *indexKeys
		transform.Inspect(n, func(n sql.Node) bool {
			ai, ok := n.(*plan.AlterIndex)
			if !ok || ai.Action != plan.IndexAction_Create || ai.Constraint == sql.IndexConstraint_Fulltext || ai.Constraint == sql.IndexConstraint_Spatial {
				return true
			}
			if keys == nil {
				keys = &indexKeys{}
				if t, ok := ai.Table.UnderlyingTable().(sql.IndexAddressableTable); ok {
					idxs, _ := t.GetIndexes(ctx)
					for _, x := range idxs {
						if x.ID() != "PRIMARY" {
							keys.seen = append(keys.seen, indexKey{name: x.ID(), sig: existingSig(x)})
						}
					}
				}
			}
			keys.add(ctx, dbName(ai.Database()), ai.Table.Name(), ai.IndexName, ai.Constraint == sql.IndexConstraint_Unique, ai.Columns)
			return true
		})
	}
}

type indexKey struct{ name, sig string }

type indexKeys struct{ seen []indexKey }

// add records an index and warns if it repeats an earlier one. An unnamed
// index gets MySQL's name: its first column, then _2, _3, ...
func (k *indexKeys) add(ctx *sql.Context, db, table, name string, unique bool, cols []sql.IndexColumn) {
	var sig strings.Builder
	fmt.Fprintf(&sig, "%v", unique)
	for _, c := range cols {
		fmt.Fprintf(&sig, ",%s/%d", strings.ToLower(c.Name), c.Length)
	}
	if name == "" && len(cols) > 0 {
		name = cols[0].Name
		for i := 2; k.has(name); i++ {
			name = fmt.Sprintf("%s_%d", cols[0].Name, i)
		}
	}
	for _, s := range k.seen {
		if s.sig == sig.String() {
			ctx.Warn(1831, "Duplicate index '%s' defined on the table '%s.%s'. This is deprecated and will be disallowed in a future release.", name, db, table)
			break
		}
	}
	k.seen = append(k.seen, indexKey{name: name, sig: sig.String()})
}

func (k *indexKeys) has(name string) bool {
	for _, s := range k.seen {
		if strings.EqualFold(s.name, name) {
			return true
		}
	}
	return false
}

func existingSig(x sql.Index) string {
	var sig strings.Builder
	fmt.Fprintf(&sig, "%v", x.IsUnique())
	pl := x.PrefixLengths()
	for i, e := range x.Expressions() {
		var l uint16
		if i < len(pl) {
			l = pl[i]
		}
		fmt.Fprintf(&sig, ",%s/%d", strings.ToLower(e[strings.LastIndexByte(e, '.')+1:]), l)
	}
	return sig.String()
}

func dbName(db sql.Database) string {
	if db == nil {
		return ""
	}
	return db.Name()
}
