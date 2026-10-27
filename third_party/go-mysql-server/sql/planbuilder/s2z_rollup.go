// s2z patch: GROUP BY ... WITH ROLLUP.

package planbuilder

import (
	"fmt"
	"strings"

	ast "github.com/dolthub/vitess/go/vt/sqlparser"
)

// rollup rewrites SELECT ... GROUP BY g1, ..., gn WITH ROLLUP, which
// go-mysql-server 0.20 lacks, into a UNION ALL of the query grouped by
// g1..gn, g1..gn-1, ..., and not grouped, each with the columns it rolls up
// (outside aggregates) as NULL and GROUPING(g) as 0 or 1, under an outer
// SELECT that orders the rows as MySQL does: by g1, its super-aggregate
// after its groups, and so on (after the query's own ORDER BY). HAVING
// applies in each branch, to its rows; DISTINCT, ORDER BY and LIMIT stay
// outside. A query whose ORDER BY it can't map onto the output columns is
// left as it is (and fails, as before).
func rollup(s *ast.Select) error {
	if s == nil || !s.Rollup {
		return nil
	}
	if selectHasWindow(s) {
		return fmt.Errorf("WITH ROLLUP and window functions together are not supported")
	}
	n := len(s.GroupBy)
	groups := make([]string, n)
	for i, g := range s.GroupBy {
		groups[i] = strings.ToLower(ast.String(g))
	}

	// the output columns' names
	names := make([]string, len(s.SelectExprs))
	exprText := map[string]string{} // a select expression -> its column
	taken := map[string]bool{}
	for i, se := range s.SelectExprs {
		ae, ok := se.(*ast.AliasedExpr)
		if !ok {
			return fmt.Errorf("WITH ROLLUP with SELECT * is not supported")
		}
		name := ae.As.String()
		if name == "" {
			name = ae.InputExpression
		}
		if name == "" {
			name = ast.String(ae.Expr)
		}
		if taken[strings.ToLower(name)] {
			name = fmt.Sprintf("s2z_c%d", i)
		}
		taken[strings.ToLower(name)] = true
		names[i] = name
		exprText[strings.ToLower(ast.String(ae.Expr))] = name
	}

	// the outer ORDER BY, over the output columns
	var order ast.OrderBy
	for _, o := range s.OrderBy {
		no := *o
		switch e := o.Expr.(type) {
		case *ast.SQLVal:
			// a position
		case *ast.ColName:
			if e.Qualifier.IsEmpty() && taken[e.Name.Lowered()] {
				break
			}
			if c, ok := orderColumn(o.Expr, exprText, groups); ok {
				no.Expr = c
			} else {
				return nil
			}
		default:
			c, ok := orderColumn(o.Expr, exprText, groups)
			if !ok {
				return nil
			}
			no.Expr = c
		}
		order = append(order, &no)
	}
	for i := range groups {
		order = append(order,
			&ast.Order{Expr: &ast.ComparisonExpr{Operator: ast.GreaterEqualStr, Left: col("s2z_lvl"), Right: intVal(n - i)}, Direction: ast.AscScr},
			&ast.Order{Expr: col(fmt.Sprintf("s2z_g%d", i)), Direction: ast.AscScr},
		)
	}

	// one branch per level: level L rolls up the last L columns
	tmp := *s
	tmp.OrderBy, tmp.Limit, tmp.Into, tmp.Lock, tmp.With = nil, nil, nil, "", nil
	tmp.QueryOpts = ast.QueryOpts{}
	tmp.Rollup = false
	text := ast.String(&tmp)
	var union ast.SelectStatement
	for level := 0; level <= n; level++ {
		parsed, err := ast.Parse(text)
		if err != nil {
			return err
		}
		b, ok := parsed.(*ast.Select)
		if !ok {
			return fmt.Errorf("WITH ROLLUP: unexpected %T", parsed)
		}
		rolled := map[string]bool{}
		for _, g := range groups[n-level:] {
			rolled[g] = true
		}
		b.GroupBy = b.GroupBy[:n-level]
		for i, se := range b.SelectExprs {
			ae := se.(*ast.AliasedExpr)
			ae.Expr = rollupExpr(ae.Expr, rolled)
			ae.As = ast.NewColIdent(names[i])
		}
		if b.Having != nil {
			b.Having.Expr = rollupExpr(b.Having.Expr, rolled)
		}
		if level == n {
			// no rows, no grand total, as in MySQL
			nonEmpty := &ast.ComparisonExpr{Operator: ast.GreaterThanStr, Left: &ast.FuncExpr{Name: ast.NewColIdent("count"), Exprs: ast.SelectExprs{&ast.StarExpr{}}}, Right: intVal(0)}
			if b.Having == nil {
				b.Having = ast.NewWhere(ast.HavingStr, nonEmpty)
			} else {
				b.Having.Expr = &ast.AndExpr{Left: b.Having.Expr, Right: nonEmpty}
			}
		}
		b.SelectExprs = append(b.SelectExprs, &ast.AliasedExpr{Expr: intVal(level), As: ast.NewColIdent("s2z_lvl")})
		for i, g := range s.GroupBy {
			var e ast.Expr = &ast.NullVal{}
			if !rolled[groups[i]] {
				ge, err := cloneExpr(g)
				if err != nil {
					return err
				}
				e = ge
			}
			b.SelectExprs = append(b.SelectExprs, &ast.AliasedExpr{Expr: e, As: ast.NewColIdent(fmt.Sprintf("s2z_g%d", i))})
		}
		if union == nil {
			union = b
		} else {
			union = &ast.SetOp{Type: ast.UnionAllStr, Left: union, Right: b}
		}
	}

	outer := make(ast.SelectExprs, len(s.SelectExprs))
	for i, se := range s.SelectExprs {
		nae := *se.(*ast.AliasedExpr)
		nae.Expr = col(names[i])
		outer[i] = &nae
	}
	s.SelectExprs = outer
	s.From = ast.TableExprs{&ast.AliasedTableExpr{Expr: &ast.Subquery{Select: union}, As: ast.NewTableIdent("s2z_r")}}
	s.Where, s.GroupBy, s.Having, s.Rollup = nil, nil, nil, false
	s.OrderBy = order
	return nil
}

// orderColumn is the output column an ORDER BY expression is: a select
// expression's, or a GROUP BY expression's hidden one.
func orderColumn(e ast.Expr, exprText map[string]string, groups []string) (ast.Expr, bool) {
	t := strings.ToLower(ast.String(e))
	if name, ok := exprText[t]; ok {
		return col(name), true
	}
	for i, g := range groups {
		if g == t {
			return col(fmt.Sprintf("s2z_g%d", i)), true
		}
	}
	return nil, false
}

// rollupExpr is e with the rolled-up GROUP BY expressions NULL outside
// aggregates, and GROUPING(g) 1 for those and 0 for the others.
func rollupExpr(e ast.Expr, rolled map[string]bool) ast.Expr {
	replace := map[ast.Expr]ast.Expr{}
	_ = ast.Walk(func(node ast.SQLNode) (bool, error) {
		switch n := node.(type) {
		case *ast.Subquery, *ast.GroupConcatExpr:
			return false, nil
		case *ast.FuncExpr:
			name := n.Name.Lowered()
			if name == "grouping" {
				v := 0
				for _, se := range n.Exprs {
					if ae, ok := se.(*ast.AliasedExpr); ok && rolled[strings.ToLower(ast.String(ae.Expr))] {
						v = 1
					}
				}
				replace[n] = intVal(v)
				return false, nil
			}
			if n.Over == nil && isAggregateFunc(name) {
				return false, nil
			}
		}
		if x, ok := node.(ast.Expr); ok && rolled[strings.ToLower(ast.String(x))] {
			replace[x] = &ast.NullVal{}
			return false, nil
		}
		return true, nil
	}, e)
	for from, to := range replace {
		e = ast.ReplaceExpr(e, from, to)
	}
	return e
}

func cloneExpr(e ast.Expr) (ast.Expr, error) {
	parsed, err := ast.Parse("select " + ast.String(e))
	if err != nil {
		return nil, err
	}
	return parsed.(*ast.Select).SelectExprs[0].(*ast.AliasedExpr).Expr, nil
}

func col(name string) *ast.ColName { return &ast.ColName{Name: ast.NewColIdent(name)} }

func intVal(i int) ast.Expr { return ast.NewIntVal([]byte(fmt.Sprint(i))) }
