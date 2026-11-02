// s2z patch: the nullability of CREATE TABLE ... SELECT's columns.

package analyzer

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"
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
