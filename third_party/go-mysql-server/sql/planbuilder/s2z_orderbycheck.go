// s2z patch: ORDER BY rules MySQL enforces and go-mysql-server did not.

package planbuilder

import (
	"strings"

	"github.com/dolthub/vitess/go/mysql"
	ast "github.com/dolthub/vitess/go/vt/sqlparser"
)

// checkOrderBy refuses, as MySQL does, an ORDER BY of a SELECT DISTINCT
// that uses a column not in the select list (3065) or an aggregate that is
// not a select expression (3066), and an aggregate in the ORDER BY of a
// query that aggregates nothing (3029). A select expression, an alias, a
// position and a window function are always allowed.
func (b *Builder) checkOrderBy(s *ast.Select) {
	if s == nil || len(s.OrderBy) == 0 {
		return
	}
	exprs, names := map[string]bool{}, map[string]bool{}
	aggregated := len(s.GroupBy) > 0 || s.Having != nil
	for _, se := range s.SelectExprs {
		ae, ok := se.(*ast.AliasedExpr)
		if !ok {
			return // SELECT *: every column is in the list
		}
		exprs[strings.ToLower(ast.String(ae.Expr))] = true
		if !ae.As.IsEmpty() {
			names[ae.As.Lowered()] = true
		}
		if c, ok := ae.Expr.(*ast.ColName); ok {
			names[c.Name.Lowered()] = true
		}
		aggregated = aggregated || hasAggregate(ae.Expr)
	}
	for i, o := range s.OrderBy {
		if _, ok := o.Expr.(*ast.SQLVal); ok || exprs[strings.ToLower(ast.String(o.Expr))] || hasWindow(o.Expr) {
			continue
		}
		agg := hasAggregate(o.Expr)
		if agg && !aggregated {
			b.handleErr(mysql.NewSQLError(3029, "HY000", "Expression #%d of ORDER BY contains aggregate function and applies to the result of a non-aggregated query", i+1))
		}
		if !s.QueryOpts.Distinct {
			continue
		}
		if agg {
			b.handleErr(mysql.NewSQLError(3066, "HY000", "Expression #%d of ORDER BY clause is not in SELECT list, contains aggregate function; this is incompatible with DISTINCT", i+1))
		}
		_ = ast.Walk(func(node ast.SQLNode) (bool, error) {
			switch n := node.(type) {
			case *ast.Subquery:
				return false, nil
			case *ast.FuncExpr:
				if isAggregateFunc(n.Name.Lowered()) {
					return false, nil // a column in an aggregate is no reference to a row's
				}
			case *ast.ColName:
				if !names[n.Name.Lowered()] {
					b.handleErr(mysql.NewSQLError(3065, "HY000", "Expression #%d of ORDER BY clause is not in SELECT list, references column '%s' which is not in SELECT list; this is incompatible with DISTINCT", i+1, ast.String(n)))
				}
			}
			return true, nil
		}, o.Expr)
	}
}
