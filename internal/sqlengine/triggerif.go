package sqlengine

import (
	"io"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// A trigger body's statements that set NEW columns return the row they
// made, as old+new halves, and go-mysql-server's trigger block takes the
// second half as the new row; it treats an IF containing such a statement
// the same way, but an IF returns its branch's last statement's row. So in
// IF ... THEN SET NEW.a = 1; SET @x = 2; END IF the new row became @x's
// SET's empty row, and the UPDATE or INSERT failed with a panic that cost
// the connection (MySQL's trigger.test, issue #20). The builder runs such
// an IF itself, as the trigger block runs its statements: each statement
// sees the row so far, and only the NEW-setting ones change it.

// setsNew reports whether n sets a column (a NEW column, in a trigger).
func setsNew(n sql.Node) bool {
	found := false
	transform.Inspect(n, func(n sql.Node) bool {
		if s, ok := n.(*plan.Set); ok {
			for _, e := range s.Exprs {
				if sf, ok := e.(*expression.SetField); ok {
					if _, ok := sf.LeftChild.(*expression.GetField); ok {
						found = true
					}
				}
			}
		}
		return !found
	})
	return found
}

// buildTriggerIf runs a trigger's IF that sets NEW columns; nil when n is
// not one (no row, or no NEW assignment), for the default builder.
func (o *exactUpdates) buildTriggerIf(ctx *sql.Context, n *plan.IfElseBlock, row sql.Row) (sql.RowIter, error) {
	if len(row) == 0 || !setsNew(n) {
		return nil, nil
	}
	var body sql.Node = n.Else
	for _, c := range n.IfConditionals {
		v, err := c.Condition.Eval(ctx, row)
		if err != nil {
			return nil, err
		}
		if v == nil {
			continue
		}
		if ok, err := sql.ConvertToBool(ctx, v); err != nil {
			return nil, err
		} else if ok {
			body = c.Body
			break
		}
	}
	cur := row
	if body != nil {
		stmts := []sql.Node{body}
		if b, ok := body.(*plan.Block); ok {
			stmts = b.Children()
		}
		for _, s := range stmts {
			it, err := o.base.Build(ctx, s, cur)
			if err != nil {
				return nil, err
			}
			for {
				r, err := it.Next(ctx)
				if err == io.EOF {
					break
				}
				if err != nil {
					_ = it.Close(ctx)
					return nil, err
				}
				if setsNew(s) && len(r) == 2*len(cur) {
					cur = r[len(r)/2:]
				}
			}
			if err := it.Close(ctx); err != nil {
				return nil, err
			}
		}
	}
	// the trigger block takes the second half
	return sql.RowsToRowIter(append(append(sql.Row{}, cur...), cur...)), nil
}
