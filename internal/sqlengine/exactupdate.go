package sqlengine

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/rowexec"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// go-mysql-server skips an UPDATE of a row whose new values equal the old
// ones under the column types' Compare, which for strings is the column's
// collation: UPDATE t SET v = 'B' on a row holding 'b' (case-insensitive
// collation) changed nothing and reported 0 rows affected. MySQL compares
// the stored bytes: it writes 'B' and reports 1.
//
// The update iterator and its affected-rows count take the schema they
// compare with from the table being updated (plan.GetUpdatable). So the
// execution of an UPDATE swaps in a copy of the table whose Schema() gives
// every text column the binary collation; reads, WHERE and the indexes
// still use the column's own collation, from the table's internal schema.
// A multi-table UPDATE (plan.UpdateJoin) compares old and new join rows
// with the schema of the join under its UpdateSource; there the join is
// wrapped in exactJoinRows, which passes its rows through with that schema
// byte-exact.

// exactUpdates is the execution-builder override that does the swap.
type exactUpdates struct {
	base sql.NodeExecBuilder
}

func newExecBuilder() sql.NodeExecBuilder {
	o := &exactUpdates{}
	o.base = rowexec.NewOverrideBuilder(o)
	return o.base
}

// Build rewrites an UPDATE of a KV table and builds it with the default
// builder, and lists SHOW WARNINGS in MySQL's order; nil for every other
// node, which the default builder then builds.
func (o *exactUpdates) Build(ctx *sql.Context, n sql.Node, row sql.Row) (sql.RowIter, error) {
	if w, ok := n.(plan.ShowWarnings); ok {
		return showWarnings(w), nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err // past max_execution_time, or killed
	}
	if it, ok, err := o.buildSticky(ctx, n, row); ok || err != nil {
		return it, err
	}
	switch n := n.(type) {
	case *plan.IfElseBlock:
		return o.buildTriggerIf(ctx, n, row)
	case *plan.CreateTrigger:
		return nil, checkCreateTrigger(ctx, n)
	case *plan.RecursiveCte:
		nn, err := guardRecursiveCte(ctx, n)
		if err != nil || nn == nil {
			return nil, err
		}
		return o.base.Build(ctx, nn, row)
	case *cteDepthGuard:
		if err := n.next(); err != nil {
			return nil, err
		}
		it, err := o.base.Build(ctx, n.child, row)
		if err != nil {
			return nil, err
		}
		return &cteRowsIter{RowIter: it, g: n}, nil
	}
	if nn, err := simpleDML(ctx, n); err != nil || nn != nil {
		if err != nil {
			return nil, err
		}
		return o.base.Build(ctx, nn, row)
	}
	if ct, ok := n.(*plan.CreateTable); ok {
		return nil, checkCreateChecks(ctx, ct)
	}
	if e, ok := n.(*exactJoinRows); ok {
		return o.base.Build(ctx, e.Child, row)
	}
	u, ok := n.(*plan.Update)
	if !ok {
		return nil, nil
	}
	if hasUpdateJoin(u.Child) {
		child, same, err := transform.Node(u.Child, exactJoinSource)
		if err != nil || same {
			return nil, err
		}
		nu, err := u.WithChildren(child)
		if err != nil {
			return nil, err
		}
		return o.base.Build(ctx, nu, row)
	}
	if t, err := plan.GetUpdatable(u.Child); err != nil || !needsExact(t) {
		return nil, nil // not ours, or already rewritten
	}
	child, same, err := transform.Node(u.Child, exactTables)
	if err != nil || same {
		return nil, err
	}
	nu, err := u.WithChildren(child)
	if err != nil {
		return nil, err
	}
	return o.base.Build(ctx, nu, row)
}

func needsExact(t sql.Table) bool {
	if fk, ok := t.(*plan.ForeignKeyHandler); ok {
		_, ok := kvTable(fk.Table)
		return ok && !isExact(fk.Sch)
	}
	kt, ok := kvTable(t)
	return ok && kt.cmpSchema == nil && !isExact(kt.schema.Schema)
}

// kvTable returns the KV table t is, plain or as an index lookup's table.
func kvTable(t sql.Table) (*Table, bool) {
	switch t := t.(type) {
	case *Table:
		return t, true
	case *indexedTable:
		return t.Table, true
	}
	return nil, false
}

// exactTables replaces each KV table under an UPDATE with its byte-exact copy.
func exactTables(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
	switch n := n.(type) {
	case *plan.ResolvedTable:
		t, ok := kvTable(n.Table)
		if !ok || t.cmpSchema != nil || isExact(t.schema.Schema) {
			return n, transform.SameTree, nil
		}
		var exact sql.Table = t.exact()
		if _, indexed := n.Table.(*indexedTable); indexed {
			exact = &indexedTable{Table: t.exact()}
		}
		nt, err := n.WithTable(exact)
		return nt, transform.NewTree, err
	case *plan.IndexedTableAccess:
		rt, ok := n.TableNode.(*plan.ResolvedTable)
		if !ok {
			return n, transform.SameTree, nil
		}
		nrt, same, err := exactTables(rt)
		if err != nil || same {
			return n, transform.SameTree, err
		}
		ni := *n
		ni.TableNode = nrt.(*plan.ResolvedTable)
		return &ni, transform.NewTree, nil
	case *plan.ForeignKeyHandler:
		// Its Sch is the schema the affected-rows count compares with.
		if isExact(n.Sch) {
			return n, transform.SameTree, nil
		}
		nh := *n
		nh.Sch = exactSchema(n.Sch)
		return &nh, transform.NewTree, nil
	}
	return n, transform.SameTree, nil
}

// exact returns a copy of t whose Schema() compares text byte by byte.
func (t *Table) exact() *Table {
	nt := *t
	nt.cmpSchema = exactSchema(t.schema.Schema)
	return &nt
}

// exactSchema copies sch with every text column in the binary collation.
func exactSchema(sch sql.Schema) sql.Schema {
	out := make(sql.Schema, len(sch))
	for i, c := range sch {
		nc := *c
		if st, ok := c.Type.(sql.StringType); ok && types.IsTextOnly(c.Type) {
			if bt, err := types.CreateString(st.Type(), st.MaxByteLength(), sql.Collation_binary); err == nil {
				nc.Type = bt
			}
		}
		out[i] = &nc
	}
	return out
}

func isExact(sch sql.Schema) bool {
	for _, c := range sch {
		if types.IsTextOnly(c.Type) {
			return false
		}
	}
	return true
}

// showWarnings lists warnings in the order they were raised, as MySQL does.
// go-mysql-server's session returns them newest first.
func showWarnings(w plan.ShowWarnings) sql.RowIter {
	rows := make([]sql.Row, len(w))
	for i, x := range w {
		rows[len(w)-1-i] = sql.NewRow(x.Level, x.Code, x.Message)
	}
	return sql.RowsToRowIter(rows...)
}

func hasUpdateJoin(n sql.Node) bool {
	found := false
	transform.Inspect(n, func(n sql.Node) bool {
		if _, ok := n.(*plan.UpdateJoin); ok {
			found = true
		}
		return !found
	})
	return found
}

// exactJoinSource wraps the join under an UPDATE JOIN's UpdateSource.
func exactJoinSource(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
	us, ok := n.(*plan.UpdateSource)
	if !ok {
		return n, transform.SameTree, nil
	}
	if _, done := us.Child.(*exactJoinRows); done {
		return n, transform.SameTree, nil
	}
	nn, err := us.WithChildren(&exactJoinRows{plan.UnaryNode{Child: us.Child}})
	return nn, transform.NewTree, err
}

// exactJoinRows is its child, with text columns compared byte by byte.
type exactJoinRows struct{ plan.UnaryNode }

func (e *exactJoinRows) Schema() sql.Schema { return exactSchema(e.Child.Schema()) }
func (e *exactJoinRows) String() string     { return e.Child.String() }
func (e *exactJoinRows) IsReadOnly() bool   { return true }
func (e *exactJoinRows) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(e, len(children), 1)
	}
	return &exactJoinRows{plan.UnaryNode{Child: children[0]}}, nil
}
