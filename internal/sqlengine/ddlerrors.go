package sqlengine

import (
	"fmt"
	"math/big"
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
// condition (3812) or uses an AUTO_INCREMENT column (3818). Unnamed
// constraints are named as MySQL names them, <table>_chk_<n> in order.
func checkErrors(d *sqlparser.DDL) error {
	auto := map[string]bool{}
	for _, c := range d.TableSpec.Columns {
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
