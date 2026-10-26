// s2z patch: window functions over a grouped query.

package planbuilder

import (
	"fmt"
	"strings"

	ast "github.com/dolthub/vitess/go/vt/sqlparser"
)

// MySQL evaluates window functions after grouping: in
//
//	SELECT a, SUM(b), RANK() OVER (ORDER BY SUM(b)) FROM t GROUP BY a
//
// the window runs over the groups. go-mysql-server 0.20 refuses a window
// function in a grouped query (1140) or loses track of the aggregate's
// column. groupedWindows rewrites such a SELECT into one over a derived
// table that does the grouping:
//
//	SELECT a, `SUM(b)`, RANK() OVER (ORDER BY s2z_w1) FROM
//	  (SELECT a, SUM(b) AS `SUM(b)`, SUM(b) AS s2z_w1 FROM t GROUP BY a) s2z_w
//
// Every largest part of a select expression, ORDER BY item or window
// definition that contains no window function moves into the derived table
// as a column. FROM, WHERE, GROUP BY and HAVING move with it; DISTINCT,
// ORDER BY and LIMIT stay outside. It changes s in place; the result has no
// grouping, so building it again leaves it as it is.
func groupedWindows(s *ast.Select) {
	if s == nil || len(s.From) == 0 || s.Into != nil || !selectHasWindow(s) || !selectIsGrouped(s) {
		return
	}
	for _, se := range s.SelectExprs {
		if _, ok := se.(*ast.AliasedExpr); !ok {
			return // SELECT *, t.*: left to go-mysql-server
		}
	}

	inner := &ast.Select{From: s.From, Where: s.Where, GroupBy: s.GroupBy, Having: s.Having}
	names := map[string]string{} // expression -> its derived column
	taken := map[string]bool{}
	n := 0
	column := func(e ast.Expr, name string) ast.Expr {
		key := ast.String(e)
		if c, ok := names[key]; ok && name == "" {
			return &ast.ColName{Name: ast.NewColIdent(c)}
		}
		if name == "" || taken[strings.ToLower(name)] {
			n++
			name = fmt.Sprintf("s2z_w%d", n)
		}
		taken[strings.ToLower(name)] = true
		names[key] = name
		inner.SelectExprs = append(inner.SelectExprs, &ast.AliasedExpr{Expr: e, As: ast.NewColIdent(name)})
		return &ast.ColName{Name: ast.NewColIdent(name)}
	}
	var lift func(e ast.Expr) ast.Expr
	leaf := func(e ast.Expr) ast.Expr {
		switch e.(type) {
		case *ast.SQLVal, *ast.NullVal, ast.BoolVal:
			return e
		}
		return column(e, "")
	}
	liftWindow := func(f *ast.FuncExpr) {
		for _, se := range f.Exprs {
			if ae, ok := se.(*ast.AliasedExpr); ok {
				ae.Expr = lift(ae.Expr)
			}
		}
		if f.Over != nil {
			for i := range f.Over.PartitionBy {
				f.Over.PartitionBy[i] = lift(f.Over.PartitionBy[i])
			}
			for _, o := range f.Over.OrderBy {
				o.Expr = lift(o.Expr)
			}
		}
	}
	lift = func(e ast.Expr) ast.Expr {
		if !hasWindow(e) {
			return leaf(e)
		}
		if f, ok := e.(*ast.FuncExpr); ok && f.Over != nil {
			liftWindow(f)
			return f
		}
		// an expression around window functions: lift its window-free
		// parts and the windows' own arguments
		var parts []ast.Expr
		var windows []*ast.FuncExpr
		_ = ast.Walk(func(node ast.SQLNode) (bool, error) {
			x, ok := node.(ast.Expr)
			if !ok || x == e {
				return true, nil
			}
			if f, ok := x.(*ast.FuncExpr); ok && f.Over != nil {
				windows = append(windows, f)
				return false, nil
			}
			if !hasWindow(x) {
				parts = append(parts, x)
				return false, nil
			}
			return true, nil
		}, e)
		for _, w := range windows {
			liftWindow(w)
		}
		for _, p := range parts {
			e = ast.ReplaceExpr(e, p, leaf(p))
		}
		return e
	}

	// the select list first, so a window-free expression keeps its name
	outer := make(ast.SelectExprs, len(s.SelectExprs))
	aliases := map[string]bool{}
	for i, se := range s.SelectExprs {
		ae := se.(*ast.AliasedExpr)
		if !ae.As.IsEmpty() {
			aliases[ae.As.Lowered()] = true
		}
		nae := *ae
		if hasWindow(ae.Expr) {
			nae.Expr = lift(ae.Expr)
		} else {
			name := ae.As.String()
			if name == "" {
				name = ae.InputExpression
			}
			if name == "" {
				name = ast.String(ae.Expr)
			}
			nae.Expr = column(ae.Expr, name)
			if c := nae.Expr.(*ast.ColName).Name.String(); c != name && ae.As.IsEmpty() {
				nae.As = ast.NewColIdent(name) // a duplicate name, renamed inside
			}
		}
		outer[i] = &nae
	}
	for _, o := range s.OrderBy {
		if c, ok := o.Expr.(*ast.ColName); ok && c.Qualifier.IsEmpty() && aliases[c.Name.Lowered()] {
			continue
		}
		if _, ok := o.Expr.(*ast.SQLVal); ok {
			continue // a position in the select list
		}
		o.Expr = lift(o.Expr)
	}
	for _, w := range s.Window {
		for i := range w.PartitionBy {
			w.PartitionBy[i] = lift(w.PartitionBy[i])
		}
		for _, o := range w.OrderBy {
			o.Expr = lift(o.Expr)
		}
	}

	if len(inner.SelectExprs) == 0 { // COUNT(*) OVER () ... GROUP BY a: a row per group
		inner.SelectExprs = ast.SelectExprs{&ast.AliasedExpr{Expr: ast.NewIntVal([]byte("1")), As: ast.NewColIdent("s2z_w0")}}
	}
	s.SelectExprs = outer
	s.From = ast.TableExprs{&ast.AliasedTableExpr{
		Expr: &ast.Subquery{Select: inner},
		As:   ast.NewTableIdent("s2z_w"),
	}}
	s.Where, s.GroupBy, s.Having = nil, nil, nil
}

// hasWindow is whether e has a window function outside a subquery.
func hasWindow(e ast.SQLNode) bool {
	found := false
	_ = ast.Walk(func(node ast.SQLNode) (bool, error) {
		switch n := node.(type) {
		case *ast.Subquery:
			return false, nil
		case *ast.FuncExpr:
			if n.Over != nil {
				found = true
				return false, nil
			}
		}
		return !found, nil
	}, e)
	return found
}

func selectHasWindow(s *ast.Select) bool {
	for _, se := range s.SelectExprs {
		if hasWindow(se) {
			return true
		}
	}
	for _, o := range s.OrderBy {
		if hasWindow(o.Expr) {
			return true
		}
	}
	return false
}

// selectIsGrouped is whether s groups its rows: GROUP BY, HAVING or an
// aggregate outside a window function (one inside a window's arguments,
// as in SUM(SUM(b)) OVER (), counts).
func selectIsGrouped(s *ast.Select) bool {
	if len(s.GroupBy) > 0 || s.Having != nil {
		return true
	}
	found := false
	visit := func(node ast.SQLNode) (bool, error) {
		switch n := node.(type) {
		case *ast.Subquery:
			return false, nil
		case *ast.GroupConcatExpr:
			found = true
		case *ast.FuncExpr:
			if n.Over == nil && isAggregateFunc(n.Name.Lowered()) {
				found = true
			}
		}
		return !found, nil
	}
	for _, se := range s.SelectExprs {
		_ = ast.Walk(visit, se)
	}
	for _, o := range s.OrderBy {
		_ = ast.Walk(visit, o.Expr)
	}
	return found
}
