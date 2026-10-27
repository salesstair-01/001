// s2z patch: quantified comparisons, x op ANY / SOME / ALL (subquery).

package plan

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// QuantifiedSubquery is left op ANY (subquery) or left op ALL (subquery),
// as MySQL evaluates them: ANY is true when the comparison is true for a
// row, false for no rows, else NULL when one comparison was NULL; ALL is
// false when it is false for a row, true for no rows, else NULL when one
// was NULL. (= ANY and <> ALL are built as IN and NOT IN.)
type QuantifiedSubquery struct {
	expression.BinaryExpressionStub
	Op  string // "=", "<>", "<", "<=", ">", ">=", "<=>"
	All bool
}

var _ sql.Expression = (*QuantifiedSubquery)(nil)

func NewQuantifiedSubquery(left, right sql.Expression, op string, all bool) *QuantifiedSubquery {
	return &QuantifiedSubquery{BinaryExpressionStub: expression.BinaryExpressionStub{LeftChild: left, RightChild: right}, Op: op, All: all}
}

func (q *QuantifiedSubquery) Type() sql.Type { return types.Boolean }

func (q *QuantifiedSubquery) IsNullable() bool { return true }

func (q *QuantifiedSubquery) quantifier() string {
	if q.All {
		return "ALL"
	}
	return "ANY"
}

func (q *QuantifiedSubquery) String() string {
	return fmt.Sprintf("(%s %s %s %s)", q.LeftChild, q.Op, q.quantifier(), q.RightChild)
}

func (q *QuantifiedSubquery) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(q, len(children), 2)
	}
	return NewQuantifiedSubquery(children[0], children[1], q.Op, q.All), nil
}

// compare is left op right, with the comparison's own type rules.
func (q *QuantifiedSubquery) compare(ctx *sql.Context, left, right sql.Expression) (interface{}, error) {
	var c sql.Expression
	switch q.Op {
	case "=":
		c = expression.NewEquals(left, right)
	case "<>", "!=":
		c = expression.NewNot(expression.NewEquals(left, right))
	case "<":
		c = expression.NewLessThan(left, right)
	case "<=":
		c = expression.NewLessThanOrEqual(left, right)
	case ">":
		c = expression.NewGreaterThan(left, right)
	case ">=":
		c = expression.NewGreaterThanOrEqual(left, right)
	case "<=>":
		c = expression.NewNullSafeEquals(left, right)
	default:
		return nil, sql.ErrUnsupportedFeature.New(q.Op + " " + q.quantifier())
	}
	return c.Eval(ctx, nil)
}

func (q *QuantifiedSubquery) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	sq, ok := q.RightChild.(*Subquery)
	if !ok {
		return nil, sql.ErrUnsupportedFeature.New(fmt.Sprintf("%s %T", q.quantifier(), q.RightChild))
	}
	if n, m := types.NumColumns(q.LeftChild.Type()), types.NumColumns(sq.Type()); n != m {
		return nil, sql.ErrInvalidOperandColumns.New(n, m)
	}
	lv, err := q.LeftChild.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	values, err := sq.EvalMultiple(ctx, row)
	if err != nil {
		return nil, err
	}
	left := expression.NewLiteral(lv, q.LeftChild.Type())
	sawNull := false
	for _, v := range values {
		res, err := q.compare(ctx, left, expression.NewLiteral(v, sq.Type()))
		if err != nil {
			return nil, err
		}
		if res == nil {
			sawNull = true
			continue
		}
		t, err := sql.ConvertToBool(ctx, res)
		if err != nil {
			return nil, err
		}
		if t != q.All { // a true for ANY, a false for ALL decides
			return t, nil
		}
	}
	if sawNull {
		return nil, nil
	}
	return q.All, nil
}
