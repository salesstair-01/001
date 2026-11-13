// s2z patch: the nullability of CREATE TABLE ... SELECT's columns.

package analyzer

import (
	"context"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/shopspring/decimal"
	"strings"
)

// selectColumnsNullable makes the columns of sch nullable unless the
// SELECT's expression for it is one MySQL knows is never NULL: a NOT NULL
// column, a literal, and arithmetic, CAST, COALESCE and IFNULL of
// those (an expression implementing NullOnlyOfNull is NULL only of a NULL
// argument). go-mysql-server's functions often say they are never NULL and
// return NULL for an argument they can't use (TO_DAYS(20090224) in CONCAT),
// which made the copy fail with 1048.
func selectColumnsNullable(n sql.Node, sch sql.Schema) {
	var proj *plan.Project
	transform.Inspect(n, func(n sql.Node) bool {
		if p, ok := n.(*plan.Project); ok && proj == nil && len(p.Projections) == len(sch) {
			proj = p
		}
		return proj == nil
	})
	if proj == nil {
		return
	}
	for i, e := range proj.Projections {
		if !sch[i].Nullable && !neverNull(e) {
			sch[i].Nullable = true
		}
	}
}

func neverNull(e sql.Expression) bool {
	if nn, ok := e.(interface{ NeverNull() bool }); ok && nn.NeverNull() {
		return true
	}
	switch e := e.(type) {
	case *expression.Alias:
		return neverNull(e.Child)
	case *expression.GetField:
		return !e.IsNullable()
	case *expression.Literal:
		return e.Value() != nil
	case *expression.Convert:
		if types.IsText(e.Type()) {
			return false // MySQL's CAST(... AS CHAR) may be NULL
		}
		return neverNull(e.Child)
	case *expression.Arithmetic:
		if !e.NullOnlyOfNull() {
			return false
		}
		for _, c := range e.Children() {
			if !neverNull(c) {
				return false
			}
		}
		return true
	case interface{ NullOnlyOfNull() bool }:
		if !e.NullOnlyOfNull() {
			return false
		}
		for _, c := range e.(sql.Expression).Children() {
			if !neverNull(c) {
				return false
			}
		}
		return true
	case *expression.UnaryMinus:
		for _, c := range e.Children() {
			if !neverNull(c) {
				return false
			}
		}
		return true
	case *function.Now, *function.UTCTimestamp, function.CurrDate, *function.CurrTime:
		return true // the clock is never NULL, with or without a precision
	case *function.Coalesce, *function.IfNull:
		for _, c := range e.Children() {
			if neverNull(c) {
				return true
			}
		}
		return false
	}
	return !e.IsNullable() && len(e.Children()) == 0
}

// selectColumnDefaults gives CREATE TABLE ... SELECT's columns MySQL's
// defaults: a column of a table keeps its default; an expression that is
// never NULL defaults to its type's zero (DEFAULT '0', ”, '0.0',
// '00:00:00.00'), except a DATE, DATETIME or TIMESTAMP, a TEXT or BLOB,
// and the other types MySQL gives none. (s2z patch)
func selectColumnDefaults(n sql.Node, orig, sch sql.Schema) {
	var proj *plan.Project
	transform.Inspect(n, func(n sql.Node) bool {
		if p, ok := n.(*plan.Project); ok && proj == nil && len(p.Projections) == len(sch) {
			proj = p
		}
		return proj == nil
	})
	if proj == nil || len(orig) != len(sch) {
		return
	}
	for i, e := range proj.Projections {
		if sch[i].Default != nil || sch[i].Generated != nil || sch[i].AutoIncrement {
			continue
		}
		for {
			a, ok := e.(*expression.Alias)
			if !ok {
				break
			}
			e = a.Child
		}
		if _, isCol := e.(*expression.GetField); isCol {
			if d := orig[i].Default; d != nil && orig[i].Generated == nil {
				sch[i].Default = d
			}
			continue
		}
		if sch[i].Nullable || !zeroDefaulted(sch[i].Type) {
			continue
		}
		z, _, err := sch[i].Type.Convert(context.Background(), sch[i].Type.Zero())
		if err != nil {
			continue
		}
		if dt, ok := sch[i].Type.(sql.DecimalType); ok {
			z = decimal.New(0, -int32(dt.Scale())) // '0.0', with the scale's digits
		}
		if types.IsBinaryType(sch[i].Type) {
			z = "" // written '', as MySQL writes it
		}
		if tt, ok := sch[i].Type.(types.TimespanType_); ok {
			// as text, which the catalog stores and reads back: '00:00:00.00'
			z = "00:00:00"
			if p, fixed := tt.Precision(); fixed && p > 0 {
				z = "00:00:00." + strings.Repeat("0", p)
			}
		}
		if d, err := sql.NewColumnDefaultValue(expression.NewLiteral(z, sch[i].Type), sch[i].Type, true, false, false); err == nil {
			sch[i].Default = d
		}
	}
}

func zeroDefaulted(t sql.Type) bool {
	switch {
	case types.IsTextBlob(t), types.IsJSON(t), types.IsGeometry(t), types.IsEnum(t), types.IsSet(t), types.IsYear(t):
		return false
	case types.IsTime(t): // DATE, DATETIME, TIMESTAMP
		return false
	}
	return types.IsNumber(t) || types.IsText(t) || types.IsTimespan(t)
}

// selectLiteralTypes types a string literal's column as MySQL does,
// VARCHAR of its characters (CHAR(0) when empty), and a binary one
// VARBINARY; go-mysql-server made them LONGTEXT. (s2z patch)
func selectLiteralTypes(n sql.Node, sch sql.Schema) {
	var proj *plan.Project
	transform.Inspect(n, func(n sql.Node) bool {
		if p, ok := n.(*plan.Project); ok && proj == nil && len(p.Projections) == len(sch) {
			proj = p
		}
		return proj == nil
	})
	if proj == nil {
		return
	}
	for i, e := range proj.Projections {
		for {
			a, ok := e.(*expression.Alias)
			if !ok {
				break
			}
			e = a.Child
		}
		lit, ok := e.(*expression.Literal)
		if !ok || !types.IsTextBlob(sch[i].Type) {
			continue
		}
		var t sql.Type
		var err error
		switch v := lit.Value().(type) {
		case string:
			coll := sql.Collation_Default
			if st, ok := sch[i].Type.(sql.StringType); ok {
				coll = st.Collation()
			}
			n := int64(len([]rune(v)))
			if n == 0 {
				t, err = types.CreateString(sqltypes.Char, 0, coll)
			} else {
				t, err = types.CreateString(sqltypes.VarChar, n, coll)
			}
		case []byte:
			t, err = types.CreateBinary(sqltypes.VarBinary, int64(len(v)))
		default:
			continue
		}
		if err == nil {
			sch[i].Type = t
		}
	}
}
