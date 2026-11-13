package sqlengine

import (
	"fmt"
	"math/big"
	"regexp"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/vt/sqlparser"
)

// ddlErrors refuses the CREATE TABLE and ALTER TABLE definitions MySQL
// refuses and go-mysql-server accepts: names that are empty or end in a
// space, display widths out of range, FLOAT(M,D) with M < D, a literal
// default on a BLOB, TEXT, GEOMETRY or JSON column in strict mode, and a
// zero date as a default under NO_ZERO_DATE in strict mode. A table MySQL
// refuses and sqld creates makes the test's next CREATE of it fail.
func ddlErrors(ctx *sql.Context, st sqlparser.Statement) error {
	var ddls []*sqlparser.DDL
	switch s := st.(type) {
	case *sqlparser.DDL:
		ddls = append(ddls, s)
	case *sqlparser.AlterTable:
		ddls = s.Statements
	}
	if err := requirePrimaryKey(ctx, st, ddls); err != nil {
		return err
	}
	mode := sql.LoadSqlMode(ctx)
	strict := mode.ModeEnabled("STRICT_TRANS_TABLES") || mode.ModeEnabled("STRICT_ALL_TABLES") || mode.ModeEnabled("TRADITIONAL")
	noZeroDate := strict && (mode.ModeEnabled("NO_ZERO_DATE") || mode.ModeEnabled("TRADITIONAL"))
	for _, d := range ddls {
		if d == nil {
			continue
		}
		if d.Action == sqlparser.CreateStr {
			if name := d.Table.Name.String(); badName(name) {
				return mysql.NewSQLError(1103, "42000", "Incorrect table name '%s'", name)
			}
		}
		if d.TableSpec == nil {
			continue
		}
		for _, c := range d.TableSpec.Columns {
			if err := columnError(c, strict, noZeroDate); err != nil {
				return err
			}
		}
		if d.Action == sqlparser.CreateStr {
			if err := checkErrors(d); err != nil {
				return err
			}
		}
	}
	return nil
}

// checkErrors refuses a new table's CHECK constraint that is not a
// condition (3812), names a column the table does not have (3820) or uses
// an AUTO_INCREMENT column (3818). Unnamed
// constraints are named as MySQL names them, <table>_chk_<n> in order.
func checkErrors(d *sqlparser.DDL) error {
	auto := map[string]bool{}
	cols := map[string]bool{}
	for _, c := range d.TableSpec.Columns {
		cols[c.Name.Lowered()] = true
		if c.Type.Autoincrement {
			auto[c.Name.Lowered()] = true
		}
	}
	n := 0
	for _, c := range d.TableSpec.Constraints {
		cc, ok := c.Details.(*sqlparser.CheckConstraintDefinition)
		if !ok {
			continue
		}
		name := c.Name
		if name == "" {
			n++
			name = fmt.Sprintf("%s_chk_%d", d.Table.Name.String(), n)
		}
		if !isCondition(cc.Expr) {
			return mysql.NewSQLError(3812, "HY000", "An expression of non-boolean type specified to a check constraint '%s'.", name)
		}
		// a column's own CHECK may name only that column (3813); any CHECK
		// only the table's columns (3820)
		own := ""
		for _, col := range d.TableSpec.Columns {
			if col.Type.Constraint == c {
				own = col.Name.Lowered()
			}
		}
		var other, missing string
		_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
			if col, ok := node.(*sqlparser.ColName); ok {
				n := col.Name.Lowered()
				if own != "" && n != own && other == "" {
					other = n
				}
				if !cols[n] && missing == "" {
					missing = col.Name.String()
				}
			}
			return true, nil
		}, cc.Expr)
		if other != "" {
			return mysql.NewSQLError(3813, "HY000", "Column check constraint '%s' references other column.", name)
		}
		if missing != "" {
			return mysql.NewSQLError(3820, "HY000", "Check constraint '%s' refers to non-existing column '%s'.", name, missing)
		}
		usesAuto := false
		_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
			if col, ok := node.(*sqlparser.ColName); ok && auto[col.Name.Lowered()] {
				usesAuto = true
			}
			return !usesAuto, nil
		}, cc.Expr)
		if usesAuto {
			return mysql.NewSQLError(3818, "HY000", "Check constraint '%s' cannot refer to an auto-increment column.", name)
		}
	}
	return nil
}

// isCondition is whether e is a condition, as MySQL requires of a CHECK
// constraint: a comparison, a logical operator, IS, IN, LIKE, BETWEEN,
// EXISTS, TRUE or FALSE, or a function returning one; not a column, a
// number or an expression that only may be 0 or 1.
func isCondition(e sqlparser.Expr) bool {
	switch e := e.(type) {
	case *sqlparser.ParenExpr:
		return isCondition(e.Expr)
	case *sqlparser.ComparisonExpr, *sqlparser.AndExpr, *sqlparser.OrExpr, *sqlparser.XorExpr, *sqlparser.NotExpr,
		*sqlparser.IsExpr, *sqlparser.RangeCond, *sqlparser.ExistsExpr, sqlparser.BoolVal:
		return true
	case *sqlparser.FuncExpr:
		switch e.Name.Lowered() {
		case "isnull", "regexp_like", "json_contains", "json_contains_path", "json_overlaps",
			"st_contains", "st_within", "st_intersects", "st_equals", "st_disjoint", "st_overlaps", "st_touches", "st_crosses":
			return true
		}
	}
	return false
}

// badName is a table or column name MySQL refuses: empty, or ending in a
// space.
func badName(name string) bool {
	return name == "" || strings.HasSuffix(name, " ")
}

func columnError(c *sqlparser.ColumnDefinition, strict, noZeroDate bool) error {
	name := c.Name.String()
	if badName(name) {
		return mysql.NewSQLError(1166, "42000", "Incorrect column name '%s'", name)
	}
	t := &c.Type
	typ := strings.ToLower(t.Type)
	if t.Length != nil {
		n, ok := new(big.Int).SetString(string(t.Length.Val), 10)
		var max int64
		switch typ {
		case "tinyint", "smallint", "mediumint", "int", "integer", "bigint":
			max = 255
		case "tinyblob", "blob", "mediumblob", "longblob", "tinytext", "text", "mediumtext", "longtext":
			max = 4294967295
		}
		if ok && max > 0 && n.Cmp(big.NewInt(max)) > 0 {
			return mysql.NewSQLError(1439, "42000", "Display width out of range for column '%s' (max = %d)", name, max)
		}
		if t.Scale != nil && (typ == "float" || typ == "double" || typ == "real" || typ == "decimal" || typ == "numeric") {
			s, sok := new(big.Int).SetString(string(t.Scale.Val), 10)
			if ok && sok && n.Cmp(s) < 0 {
				return mysql.NewSQLError(1427, "42000", "For float(M,D), double(M,D) or decimal(M,D), M must be >= D (column '%s').", name)
			}
		}
	}
	if t.Default == nil {
		return nil
	}
	if _, isExpr := t.Default.(*sqlparser.ParenExpr); isExpr {
		return nil
	}
	switch typ {
	case "tinyblob", "blob", "mediumblob", "longblob", "tinytext", "text", "mediumtext", "longtext", "json",
		"geometry", "point", "linestring", "polygon", "multipoint", "multilinestring", "multipolygon", "geometrycollection", "geomcollection":
		if _, isNull := t.Default.(*sqlparser.NullVal); strict && !isNull {
			return mysql.NewSQLError(1101, "42000", "BLOB, TEXT, GEOMETRY or JSON column '%s' can't have a default value", name)
		}
	case "date", "datetime", "timestamp":
		if noZeroDate && zeroDateLiteral(t.Default) {
			return mysql.NewSQLError(1067, "42000", "Invalid default value for '%s'", name)
		}
	}
	return nil
}

// zeroDateLiteral is whether e is 0 or a zero date as a string.
func zeroDateLiteral(e sqlparser.Expr) bool {
	v, ok := e.(*sqlparser.SQLVal)
	if !ok {
		return false
	}
	s := strings.TrimSpace(string(v.Val))
	switch v.Type {
	case sqlparser.IntVal:
		return strings.Trim(s, "0") == ""
	case sqlparser.StrVal:
		return strings.Trim(s, "0-: .") == "" && s != ""
	}
	return false
}

// routineStatement matches the start of a CREATE FUNCTION or TRIGGER.
var routineStatement = regexp.MustCompile(`(?is)^(\s+|/\*.*?\*/|(#|-- )[^\n]*\n)*create\s+(definer\s*=\s*\S+\s+)?(function|trigger)\s`)

// routineBodyErrors refuses a stored function's or trigger's body with a
// statement MySQL does not allow in one: DDL or a transaction statement,
// which commits (1422), FLUSH (1336) and LOCK TABLES (1314). A
// procedure may have them.
func routineBodyErrors(st sqlparser.Statement) error {
	d, ok := st.(*sqlparser.DDL)
	if !ok || d.Action != sqlparser.CreateStr {
		return nil
	}
	var body sqlparser.Statement
	switch {
	case d.TriggerSpec != nil:
		body = d.TriggerSpec.Body
	case d.ProcedureSpec != nil && strings.HasPrefix(d.ProcedureSpec.ProcName.Name.String(), sqlparser.FunctionProcedurePrefix):
		body = d.ProcedureSpec.Body
	default:
		return nil
	}
	return bodyStatementError(body)
}

func bodyStatementError(s sqlparser.Statement) error {
	each := func(ss sqlparser.Statements) error {
		for _, st := range ss {
			if err := bodyStatementError(st); err != nil {
				return err
			}
		}
		return nil
	}
	commit := mysql.NewSQLError(1422, "HY000", "Explicit or implicit commit is not allowed in stored function or trigger.")
	switch n := s.(type) {
	case *sqlparser.DDL:
		if n.Temporary || n.TriggerSpec != nil || n.ProcedureSpec != nil {
			return nil // temporary tables don't commit; nested routines fail on their own
		}
		return commit
	case *sqlparser.AlterTable, *sqlparser.Begin, *sqlparser.Commit, *sqlparser.Rollback:
		return commit
	case *sqlparser.Flush:
		return mysql.NewSQLError(1336, "0A000", "FLUSH is not allowed in stored function or trigger")
	case *sqlparser.LockTables:
		return mysql.NewSQLError(1314, "0A000", "LOCK is not allowed in stored procedures")
	case *sqlparser.BeginEndBlock:
		return each(n.Statements)
	case *sqlparser.IfStatement:
		for _, c := range n.Conditions {
			if err := each(c.Statements); err != nil {
				return err
			}
		}
		return each(n.Else)
	case *sqlparser.CaseStatement:
		for _, c := range n.Cases {
			if err := each(c.Statements); err != nil {
				return err
			}
		}
		return each(n.Else)
	case *sqlparser.Loop:
		return each(n.Statements)
	case *sqlparser.While:
		return each(n.Statements)
	case *sqlparser.Repeat:
		return each(n.Statements)
	case *sqlparser.Declare:
		if n.Handler != nil {
			return bodyStatementError(n.Handler.Statement)
		}
	}
	return nil
}

// errRequirePrimaryKey is MySQL's refusal of a table without a primary key
// under sql_require_primary_key.
var errRequirePrimaryKey = mysql.NewSQLError(3750, "HY000", "Unable to create or change a table without a primary key, when the system variable 'sql_require_primary_key' is set. Add a primary key to the table or unset this variable to avoid this message. Note that tables without a primary key can cause performance problems in row-based replication, so please consult your DBA before changing this setting.")

// requirePrimaryKey refuses, under sql_require_primary_key, a CREATE TABLE
// without a primary key (a UNIQUE key is not one; temporary tables and
// CREATE ... SELECT too) and an ALTER TABLE that drops the primary key
// without adding one.
func requirePrimaryKey(ctx *sql.Context, st sqlparser.Statement, ddls []*sqlparser.DDL) error {
	v, err := ctx.GetSessionVariable(ctx, "sql_require_primary_key")
	if err != nil || !truthy(v) {
		return nil
	}
	switch st.(type) {
	case *sqlparser.DDL:
		d := ddls[0]
		if d.Action != sqlparser.CreateStr || d.OptLike != nil || d.ViewSpec != nil || d.TriggerSpec != nil || d.ProcedureSpec != nil || d.EventSpec != nil {
			return nil
		}
		if d.TableSpec == nil || !specHasPrimaryKey(d.TableSpec) {
			return errRequirePrimaryKey
		}
	case *sqlparser.AlterTable:
		drops, adds := false, false
		for _, d := range ddls {
			if d == nil {
				continue
			}
			if is := d.IndexSpec; is != nil && strings.EqualFold(is.Type, sqlparser.PrimaryStr) {
				switch strings.ToLower(is.Action) {
				case sqlparser.DropStr:
					drops = true
				case sqlparser.CreateStr:
					adds = true
				}
			}
			if d.TableSpec != nil && specHasPrimaryKey(d.TableSpec) {
				adds = true
			}
		}
		if drops && !adds {
			return errRequirePrimaryKey
		}
	}
	return nil
}

func specHasPrimaryKey(ts *sqlparser.TableSpec) bool {
	for _, c := range ts.Columns {
		if c.Type.KeyOpt == sqlparser.ColKeyPrimary {
			return true
		}
	}
	for _, ix := range ts.Indexes {
		if ix.Info != nil && ix.Info.Primary {
			return true
		}
	}
	return false
}

func truthy(v any) bool {
	switch x := v.(type) {
	case bool:
		return x
	case int8:
		return x != 0
	case int64:
		return x != 0
	case string:
		return strings.EqualFold(x, "on") || x == "1"
	}
	return fmt.Sprint(v) == "1"
}
