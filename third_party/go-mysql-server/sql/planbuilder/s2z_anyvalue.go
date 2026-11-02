// s2z patch: ANY_VALUE of an aggregate.

package planbuilder

import (
	ast "github.com/dolthub/vitess/go/vt/sqlparser"
)

// anyValueOfAggregates replaces ANY_VALUE(e), where e has an aggregate,
// with e: ANY_VALUE returns its argument (it only exempts it from
// ONLY_FULL_GROUP_BY), and of an aggregate that is the aggregate's value.
// go-mysql-server takes ANY_VALUE for an aggregate itself and could not
// plan one of an aggregate (ORDER BY any_value(count(*) - count(b))).
func anyValueOfAggregates(s *ast.Select) {
	if s == nil {
		return
	}
	rewrite := func(e ast.Expr) ast.Expr {
		if e == nil {
			return nil
		}
		replace := map[ast.Expr]ast.Expr{}
		_ = ast.Walk(func(node ast.SQLNode) (bool, error) {
			switch n := node.(type) {
			case *ast.Subquery:
				return false, nil
			case *ast.FuncExpr:
				if n.Name.Lowered() == "any_value" && n.Over == nil && len(n.Exprs) == 1 {
					if ae, ok := n.Exprs[0].(*ast.AliasedExpr); ok && hasAggregate(ae.Expr) {
						replace[n] = &ast.ParenExpr{Expr: ae.Expr}
						return false, nil
					}
				}
			}
			return true, nil
		}, e)
		for from, to := range replace {
			e = ast.ReplaceExpr(e, from, to)
		}
		return e
	}
	for _, se := range s.SelectExprs {
		if ae, ok := se.(*ast.AliasedExpr); ok {
			ae.Expr = rewrite(ae.Expr)
		}
	}
	for _, o := range s.OrderBy {
		o.Expr = rewrite(o.Expr)
	}
	if s.Having != nil {
		s.Having.Expr = rewrite(s.Having.Expr)
	}
}
