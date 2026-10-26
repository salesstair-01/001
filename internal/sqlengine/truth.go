package sqlengine

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression/function"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// IF(cond, a, b) takes a string condition as a number, as MySQL does: by its
// numeric prefix, silently ('2:3' is 2, true). go-mysql-server converted
// the whole string, so '2:3' was 0 and warned; a trigger aggregating
// IF(@a, CONCAT(@a, ':', NEW.i), NEW.i) lost every other row (MySQL's
// trigger.test).

func init() {
	overrides["if"] = sql.Function3{Name: "if", Fn: func(c, a, b sql.Expression) sql.Expression {
		return function.NewIf(&stringTruth{c}, a, b)
	}}
}

// stringTruth is its child, a string as 1 or 0 by its numeric prefix.
type stringTruth struct{ sql.Expression }

func (t *stringTruth) Eval(ctx *sql.Context, row sql.Row) (any, error) {
	v, err := t.Expression.Eval(ctx, row)
	if err != nil || v == nil {
		return v, err
	}
	var s string
	switch x := v.(type) {
	case string:
		s = x
	case []byte:
		s = string(x)
	default:
		return v, nil
	}
	if stringPrefixFloat(s) != 0 {
		return int8(1), nil
	}
	return int8(0), nil
}

func (t *stringTruth) Type() sql.Type {
	if types.IsText(t.Expression.Type()) {
		return types.Int8
	}
	return t.Expression.Type()
}

func (t *stringTruth) WithChildren(c ...sql.Expression) (sql.Expression, error) {
	if len(c) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(t, len(c), 1)
	}
	return &stringTruth{c[0]}, nil
}

func (t *stringTruth) Children() []sql.Expression { return []sql.Expression{t.Expression} }
