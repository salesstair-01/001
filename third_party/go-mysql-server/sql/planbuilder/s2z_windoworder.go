// s2z patch: the row order of a query with window functions.

package planbuilder

import (
	"strings"

	ast "github.com/dolthub/vitess/go/vt/sqlparser"
)

// windowOutputOrder gives a SELECT with window functions and no ORDER BY
// the order MySQL returns its rows in: MySQL sorts the rows by the window's
// PARTITION BY and ORDER BY to evaluate it, and returns them so; go-mysql-
// server returned them in the table's order. Only when every window
// function of the select list uses the same window (the order of several
// windows depends on MySQL's sort plan), and not with DISTINCT.
func windowOutputOrder(s *ast.Select) {
	if s == nil || len(s.OrderBy) > 0 || s.QueryOpts.Distinct {
		return
	}
	named := map[string]*ast.WindowDef{}
	for _, w := range s.Window {
		named[w.Name.Lowered()] = w
	}
	// resolve is a window's PARTITION BY and ORDER BY, through references
	var resolve func(w *ast.WindowDef, depth int) (ast.Exprs, ast.OrderBy, bool)
	resolve = func(w *ast.WindowDef, depth int) (ast.Exprs, ast.OrderBy, bool) {
		parts, order := w.PartitionBy, w.OrderBy
		if !w.NameRef.IsEmpty() {
			ref, ok := named[w.NameRef.Lowered()]
			if !ok || depth > 10 {
				return nil, nil, false
			}
			rp, ro, ok := resolve(ref, depth+1)
			if !ok {
				return nil, nil, false
			}
			if len(parts) == 0 {
				parts = rp
			}
			if len(order) == 0 {
				order = ro
			}
		}
		return parts, order, true
	}
	var key string
	var parts ast.Exprs
	var order ast.OrderBy
	count := 0
	ok := true
	for _, se := range s.SelectExprs {
		ae, isAliased := se.(*ast.AliasedExpr)
		if !isAliased {
			continue
		}
		_ = ast.Walk(func(node ast.SQLNode) (bool, error) {
			switch n := node.(type) {
			case *ast.Subquery:
				return false, nil
			case *ast.FuncExpr:
				if n.Over == nil {
					return true, nil
				}
				p, o, resolved := resolve((*ast.WindowDef)(n.Over), 0)
				if !resolved {
					ok = false
					return false, nil
				}
				k := strings.ToLower(ast.String(p) + "|" + ast.String(o))
				if count > 0 && k != key {
					ok = false
				}
				key, parts, order = k, p, o
				count++
			}
			return true, nil
		}, ae.Expr)
	}
	if !ok || count == 0 || (len(parts) == 0 && len(order) == 0) {
		return
	}
	var out ast.OrderBy
	for _, p := range parts {
		c, err := cloneExpr(p)
		if err != nil {
			return
		}
		out = append(out, &ast.Order{Expr: c, Direction: ast.AscScr})
	}
	for _, o := range order {
		c, err := cloneExpr(o.Expr)
		if err != nil {
			return
		}
		dir := o.Direction
		if dir == "" {
			dir = ast.AscScr
		}
		out = append(out, &ast.Order{Expr: c, Direction: dir})
	}
	s.OrderBy = out
}
