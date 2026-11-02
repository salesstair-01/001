// s2z patch: select aliases in ORDER BY expressions.

package planbuilder

import (
	"strings"

	ast "github.com/dolthub/vitess/go/vt/sqlparser"
)

// orderByAliases replaces the select aliases an ORDER BY expression uses
// (ORDER BY 30 - num, UPPER(str)) with their expressions, as MySQL
// resolves them; go-mysql-server resolved an alias only as the whole ORDER
// BY expression. An alias of a window function or of RAND() is left (its
// value would differ), and so is a bare alias.
func orderByAliases(s *ast.Select) {
	if s == nil || len(s.OrderBy) == 0 {
		return
	}
	aliases := map[string]ast.Expr{}
	for _, se := range s.SelectExprs {
		ae, ok := se.(*ast.AliasedExpr)
		if !ok || ae.As.IsEmpty() {
			continue
		}
		if c, isCol := ae.Expr.(*ast.ColName); isCol && c.Name.Equal(ae.As) {
			continue
		}
		if selectExprVolatile(ae.Expr) {
			continue
		}
		aliases[ae.As.Lowered()] = ae.Expr
	}
	if len(aliases) == 0 {
		return
	}
	for _, o := range s.OrderBy {
		switch o.Expr.(type) {
		case *ast.ColName, *ast.SQLVal:
			continue
		}
		replace := map[ast.Expr]ast.Expr{}
		_ = ast.Walk(func(node ast.SQLNode) (bool, error) {
			switch n := node.(type) {
			case *ast.Subquery:
				return false, nil
			case *ast.ColName:
				if n.Qualifier.IsEmpty() {
					if e, ok := aliases[n.Name.Lowered()]; ok {
						if c, err := cloneExpr(e); err == nil {
							replace[n] = &ast.ParenExpr{Expr: c}
						}
					}
				}
			}
			return true, nil
		}, o.Expr)
		for from, to := range replace {
			o.Expr = ast.ReplaceExpr(o.Expr, from, to)
		}
	}
}

// selectExprVolatile is whether e has a window function, RAND() or a user
// variable assignment: evaluated again, it would not be the selected value.
func selectExprVolatile(e ast.Expr) bool {
	found := false
	_ = ast.Walk(func(node ast.SQLNode) (bool, error) {
		switch n := node.(type) {
		case *ast.FuncExpr:
			name := n.Name.Lowered()
			if n.Over != nil || (isWindowFunc(name) && !isAggregateFunc(name)) || name == "rand" || strings.HasPrefix(name, "s2z_assign") {
				found = true
			}
		case *ast.Subquery:
			found = true
		}
		return !found, nil
	}, e)
	return found
}
