package sqlengine

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/vt/sqlparser"
)

// @v := expr in an expression (SELECT @n := @n + 1, WHERE (@a := a) > 1):
// the parser makes it s2z_assign_user_var('v', expr) (third_party/vitess),
// which sets @v to the value and is the value, as in MySQL.

func init() {
	overrides[sqlparser.UserVarAssignFunction] = sql.Function2{Name: sqlparser.UserVarAssignFunction, Fn: func(name, e sql.Expression) sql.Expression {
		return &assignUserVar{name: name, expr: e}
	}}
}

type assignUserVar struct {
	name, expr sql.Expression // name: a string literal
}

var (
	_ sql.FunctionExpression         = (*assignUserVar)(nil)
	_ sql.NonDeterministicExpression = (*assignUserVar)(nil)
)

func (a *assignUserVar) FunctionName() string       { return sqlparser.UserVarAssignFunction }
func (a *assignUserVar) Description() string        { return "assigns a user variable" }
func (a *assignUserVar) Resolved() bool             { return a.name.Resolved() && a.expr.Resolved() }
func (a *assignUserVar) IsNullable() bool           { return true }
func (a *assignUserVar) Type() sql.Type             { return a.expr.Type() }
func (a *assignUserVar) Children() []sql.Expression { return []sql.Expression{a.name, a.expr} }
func (a *assignUserVar) IsNonDeterministic() bool   { return true }

func (a *assignUserVar) String() string {
	return fmt.Sprintf("@%s := %s", a.name, a.expr)
}

func (a *assignUserVar) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(a, len(children), 2)
	}
	return &assignUserVar{name: children[0], expr: children[1]}, nil
}

func (a *assignUserVar) Eval(ctx *sql.Context, row sql.Row) (any, error) {
	n, err := a.name.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	v, err := a.expr.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if err := ctx.SetUserVariable(ctx, fmt.Sprint(n), v, a.expr.Type()); err != nil {
		return nil, err
	}
	return v, nil
}
