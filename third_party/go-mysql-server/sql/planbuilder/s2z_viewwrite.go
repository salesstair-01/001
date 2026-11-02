// s2z patch: INSERT, UPDATE and DELETE through a view.

package planbuilder

import (
	"strings"

	"github.com/dolthub/vitess/go/mysql"
	ast "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql"
)

// viewTarget is an updatable view as its base table: the view's columns
// as expressions of the table's, and the rows it shows.
type viewTarget struct {
	base    ast.TableName
	cols    map[string]ast.Expr // a view column (lower case) -> the table's column; nil when not a column
	order   []string            // the view's columns, in order; nil for SELECT *
	where   ast.Expr
	checked bool // WITH CHECK OPTION
}

// viewDefinition is tn's definition when tn names a view, and its database.
func (b *Builder) viewDefinition(tn ast.TableName) (sql.ViewDefinition, string, bool) {
	dbName := tn.DbQualifier.String()
	if dbName == "" {
		dbName = b.ctx.GetCurrentDatabase()
	}
	if dbName == "" {
		return sql.ViewDefinition{}, "", false
	}
	db, err := b.cat.Database(b.ctx, dbName)
	if err != nil {
		return sql.ViewDefinition{}, "", false
	}
	if _, isTable, err := db.GetTableInsensitive(b.ctx, tn.Name.String()); err != nil || isTable {
		return sql.ViewDefinition{}, "", false
	}
	vdb, ok := db.(sql.ViewDatabase)
	if !ok {
		return sql.ViewDefinition{}, "", false
	}
	def, ok, err := vdb.GetViewDefinition(b.ctx, tn.Name.String())
	if err != nil || !ok {
		return sql.ViewDefinition{}, "", false
	}
	return def, dbName, true
}

// updatableView is the view tn as a target for INSERT, UPDATE or DELETE:
// one whose definition selects from one table (or such a view), without
// DISTINCT, GROUP BY, HAVING, aggregates, LIMIT, UNION or a WITH. go-mysql-
// server writes to no view; MySQL writes such a view's table.
func (b *Builder) updatableView(tn ast.TableName) (*viewTarget, bool) {
	def, dbName, ok := b.viewDefinition(tn)
	if !ok {
		return nil, false
	}
	stmt, err := ast.Parse(def.CreateViewStatement)
	if err != nil {
		return nil, false
	}
	ddl, ok := stmt.(*ast.DDL)
	if !ok || ddl.ViewSpec == nil {
		return nil, false
	}
	sel, ok := ddl.ViewSpec.ViewExpr.(*ast.Select)
	if !ok || sel.QueryOpts.Distinct || len(sel.GroupBy) > 0 || sel.Having != nil || sel.Limit != nil || sel.With != nil || len(sel.From) != 1 {
		return nil, false
	}
	ate, ok := sel.From[0].(*ast.AliasedTableExpr)
	if !ok {
		return nil, false
	}
	base, ok := ate.Expr.(ast.TableName)
	if !ok {
		return nil, false
	}
	if base.DbQualifier.IsEmpty() {
		base.DbQualifier = ast.NewTableIdent(dbName)
	}
	alias := ate.As.String()
	if alias == "" {
		alias = base.Name.String()
	}
	vt := &viewTarget{base: base, cols: map[string]ast.Expr{}, checked: ddl.ViewSpec.CheckOption != ast.ViewCheckOptionUnspecified}
	// the table's column for an expression of the view's FROM
	unqualify := func(e ast.Expr) ast.Expr {
		_ = ast.Walk(func(node ast.SQLNode) (bool, error) {
			if c, ok := node.(*ast.ColName); ok && strings.EqualFold(c.Qualifier.Name.String(), alias) {
				c.Qualifier = ast.TableName{}
			}
			return true, nil
		}, e)
		return e
	}
	star := false
	for i, se := range sel.SelectExprs {
		switch se := se.(type) {
		case *ast.StarExpr:
			star = true
		case *ast.AliasedExpr:
			if hasAggregate(se.Expr) {
				return nil, false
			}
			name := se.As.String()
			if name == "" {
				if c, ok := se.Expr.(*ast.ColName); ok {
					name = c.Name.String()
				} else {
					name = ast.String(se.Expr)
				}
			}
			if i < len(ddl.ViewSpec.Columns) {
				name = ddl.ViewSpec.Columns[i].String()
			}
			var target ast.Expr
			if c, ok := se.Expr.(*ast.ColName); ok {
				target = unqualify(&ast.ColName{Name: c.Name, Qualifier: c.Qualifier})
			}
			vt.cols[strings.ToLower(name)] = target
			vt.order = append(vt.order, name)
		default:
			return nil, false
		}
	}
	if star {
		if len(sel.SelectExprs) > 1 || len(ddl.ViewSpec.Columns) > 0 {
			return nil, false
		}
		vt.cols, vt.order = nil, nil // every column is the table's own
	}
	if sel.Where != nil {
		vt.where = unqualify(sel.Where.Expr)
	}
	// a view of an updatable view writes that view's table
	if inner, ok := b.updatableView(base); ok {
		if inner.cols != nil {
			if vt.cols == nil {
				return nil, false
			}
			for k, e := range vt.cols {
				if e != nil {
					vt.cols[k] = inner.mapExpr(e, "")
				}
			}
		}
		if vt.where != nil {
			vt.where = inner.mapExpr(vt.where, "")
		}
		vt.where = andExpr(vt.where, inner.where)
		vt.base, vt.checked = inner.base, vt.checked || inner.checked
	}
	return vt, true
}

func hasAggregate(e ast.Expr) bool {
	found := false
	_ = ast.Walk(func(node ast.SQLNode) (bool, error) {
		switch n := node.(type) {
		case *ast.Subquery:
			return false, nil
		case *ast.FuncExpr:
			// ANY_VALUE is no aggregate in MySQL: it only marks a value
			// (whatever it has)
			if n.Name.Lowered() == "any_value" {
				return false, nil
			}
			if n.Over == nil && isAggregateFunc(n.Name.Lowered()) {
				found = true
			}
		case *ast.GroupConcatExpr:
			found = true
		}
		return !found, nil
	}, e)
	return found
}

// checkNestedAggregate refuses an aggregate of an aggregate, as MySQL
// (1111); go-mysql-server failed to plan it.
func checkNestedAggregate(b *Builder, args ast.SelectExprs) {
	for _, a := range args {
		if ae, ok := a.(*ast.AliasedExpr); ok && hasAggregate(ae.Expr) {
			b.handleErr(mysql.NewSQLError(1111, "HY000", "Invalid use of group function"))
		}
	}
}

func andExpr(a, b ast.Expr) ast.Expr {
	switch {
	case a == nil:
		return b
	case b == nil:
		return a
	}
	return &ast.AndExpr{Left: &ast.ParenExpr{Expr: a}, Right: &ast.ParenExpr{Expr: b}}
}

// mapExpr is e with the view's columns as the table's, qualified with
// qualifier when it is not empty; a reference qualified with another name
// than the view's (or the alias it has) stays.
func (vt *viewTarget) mapExpr(e ast.Expr, viewName string) ast.Expr {
	if e == nil || vt.cols == nil {
		return e
	}
	e, _ = cloneASTExpr(e)
	replace := map[ast.Expr]ast.Expr{}
	_ = ast.Walk(func(node ast.SQLNode) (bool, error) {
		switch n := node.(type) {
		case *ast.Subquery:
			return false, nil
		case *ast.ColName:
			q := n.Qualifier.Name.String()
			if q != "" && !strings.EqualFold(q, viewName) {
				return false, nil
			}
			if to, ok := vt.cols[n.Name.Lowered()]; ok && to != nil {
				c, _ := cloneASTExpr(to)
				replace[n] = c
			}
		}
		return true, nil
	}, e)
	for from, to := range replace {
		e = ast.ReplaceExpr(e, from, to)
	}
	return e
}

// column is the table's column a view column names, "" when it names none.
func (vt *viewTarget) column(name string, viewName string) string {
	if vt.cols == nil {
		return name
	}
	to, ok := vt.cols[strings.ToLower(name)]
	if !ok {
		return ""
	}
	c, ok := to.(*ast.ColName)
	if !ok {
		return ""
	}
	return c.Name.String()
}

func cloneASTExpr(e ast.Expr) (ast.Expr, error) {
	parsed, err := ast.Parse("select " + ast.String(e))
	if err != nil {
		return e, err
	}
	return parsed.(*ast.Select).SelectExprs[0].(*ast.AliasedExpr).Expr, nil
}

// rewriteInsertIntoView makes an INSERT into an updatable view one into its
// table.
func (b *Builder) rewriteInsertIntoView(i *ast.Insert) {
	vt, ok := b.updatableView(i.Table)
	viewName := i.Table.Name.String()
	if !ok {
		if _, _, isView := b.viewDefinition(i.Table); isView {
			b.handleErr(mysql.NewSQLError(1471, "HY000", "The target table %s of the INSERT is not insertable-into", viewName))
		}
		return
	}

	cols := i.Columns
	if len(cols) == 0 && vt.order != nil {
		for _, c := range vt.order {
			cols = append(cols, ast.NewColIdent(c))
		}
	}
	if vt.cols != nil {
		mapped := make(ast.Columns, len(cols))
		for k, c := range cols {
			name := vt.column(c.String(), viewName)
			if name == "" {
				if _, known := vt.cols[c.Lowered()]; known {
					b.handleErr(mysql.NewSQLError(1348, "HY000", "Column '%s' is not updatable", c.String()))
				}
				b.handleErr(sql.ErrUnknownColumn.New(c.String(), viewName))
			}
			mapped[k] = ast.NewColIdent(name)
		}
		cols = mapped
		for _, e := range vt.cols {
			if e == nil { // a column that is no column of the table
				b.handleErr(mysql.NewSQLError(1471, "HY000", "The target table %s of the INSERT is not insertable-into", viewName))
			}
		}
	}
	for k, a := range i.OnDup {
		if name := vt.column(a.Name.Name.String(), viewName); name != "" {
			a.Name = &ast.ColName{Name: ast.NewColIdent(name)}
		}
		a.Expr = vt.mapExpr(a.Expr, viewName)
		i.OnDup[k] = a
	}
	i.Table, i.Columns = vt.base, cols
}

// viewInTableExprs is the single table expression of an UPDATE or DELETE
// when it names an updatable view: its alias, and the view.
func (b *Builder) viewInTableExprs(te ast.TableExprs, stmt string) (*ast.AliasedTableExpr, *viewTarget, bool) {
	if len(te) != 1 {
		return nil, nil, false
	}
	ate, ok := te[0].(*ast.AliasedTableExpr)
	if !ok {
		return nil, nil, false
	}
	tn, ok := ate.Expr.(ast.TableName)
	if !ok {
		return nil, nil, false
	}
	vt, ok := b.updatableView(tn)
	if !ok {
		if _, _, isView := b.viewDefinition(tn); isView {
			b.handleErr(mysql.NewSQLError(1288, "HY000", "The target table %s of the %s is not updatable", tn.Name.String(), stmt))
		}
	}
	return ate, vt, ok
}

// rewriteUpdateOfView makes an UPDATE of an updatable view one of its
// table, of the rows the view shows.
func (b *Builder) rewriteUpdateOfView(u *ast.Update) {
	ate, vt, ok := b.viewInTableExprs(u.TableExprs, "UPDATE")
	if !ok {
		return
	}
	viewName := ate.As.String()
	if viewName == "" {
		viewName = ate.Expr.(ast.TableName).Name.String()
	}
	for k, a := range u.Exprs {
		name := vt.column(a.Name.Name.String(), viewName)
		if name == "" {
			b.handleErr(sql.ErrUnknownColumn.New(a.Name.Name.String(), viewName))
		}
		u.Exprs[k] = &ast.AssignmentExpr{Name: &ast.ColName{Name: ast.NewColIdent(name)}, Expr: vt.mapExpr(a.Expr, viewName)}
	}
	u.Where = viewWhere(u.Where, vt, viewName)
	for _, o := range u.OrderBy {
		o.Expr = vt.mapExpr(o.Expr, viewName)
	}
	u.TableExprs = ast.TableExprs{&ast.AliasedTableExpr{Expr: vt.base, As: ast.NewTableIdent(viewName)}}
}

// rewriteDeleteFromView makes a DELETE from an updatable view one from its
// table, of the rows the view shows.
func (b *Builder) rewriteDeleteFromView(d *ast.Delete) {
	if len(d.Targets) > 0 {
		return
	}
	ate, vt, ok := b.viewInTableExprs(d.TableExprs, "DELETE")
	if !ok {
		return
	}
	viewName := ate.As.String()
	if viewName == "" {
		viewName = ate.Expr.(ast.TableName).Name.String()
	}
	d.Where = viewWhere(d.Where, vt, viewName)
	for _, o := range d.OrderBy {
		o.Expr = vt.mapExpr(o.Expr, viewName)
	}
	d.TableExprs = ast.TableExprs{&ast.AliasedTableExpr{Expr: vt.base, As: ast.NewTableIdent(viewName)}}
}

func viewWhere(w *ast.Where, vt *viewTarget, viewName string) *ast.Where {
	var e ast.Expr
	if w != nil {
		e = vt.mapExpr(w.Expr, viewName)
	}
	if vt.where != nil {
		c, _ := cloneASTExpr(vt.where)
		e = andExpr(e, c)
	}
	if e == nil {
		return nil
	}
	return ast.NewWhere(ast.WhereStr, e)
}
