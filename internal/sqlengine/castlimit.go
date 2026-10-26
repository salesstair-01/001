package sqlengine

import (
	"reflect"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// go-mysql-server pads CAST(x AS BINARY(n)) to n bytes by allocating them:
// CAST('a' AS BINARY(4294967294)) asks for 4 GiB and can take the query
// engine down (found by MySQL's cast.test, see mtr/). MySQL returns NULL
// with warning 1301 when n is over max_allowed_packet. A pre-analyze rule
// wraps such casts so that they do the same.

const castLimitID analyzer.RuleId = 10002

// Casts shorter than this are left alone: max_allowed_packet is at least 1 KiB
// and casts of a few MiB are cheap.
const castLimitMin = 1 << 20

func castLimit(ctx *sql.Context, a *analyzer.Analyzer, n sql.Node, scope *plan.Scope, sel analyzer.RuleSelector, qf *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	return transform.NodeExprsWithOpaque(n, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
		switch e := e.(type) {
		case *expression.Convert:
			if to, n := convertTarget(e); to == expression.ConvertToBinary && n > castLimitMin {
				return &limitedConvert{Convert: e, length: n}, transform.NewTree, nil
			}
		case *plan.Subquery:
			q, same, err := castLimit(ctx, a, e.Query, scope, sel, qf)
			if err != nil || same {
				return e, transform.SameTree, err
			}
			return e.WithQuery(q), transform.NewTree, nil
		}
		return e, transform.SameTree, nil
	})
}

// convertTarget is a Convert's target type and length (unexported fields).
func convertTarget(c *expression.Convert) (string, int64) {
	v := reflect.ValueOf(c).Elem()
	to, n := v.FieldByName("castToType"), v.FieldByName("typeLength")
	if !to.IsValid() || !n.IsValid() {
		return "", 0
	}
	return to.String(), n.Int()
}

// limitedConvert is a binary cast that returns NULL, with MySQL's warning,
// when its length is over max_allowed_packet.
type limitedConvert struct {
	*expression.Convert
	length int64
}

func (c *limitedConvert) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	limit := int64(64 << 20)
	if v, err := ctx.GetSessionVariable(ctx, "max_allowed_packet"); err == nil {
		switch v := v.(type) {
		case int64:
			limit = v
		case uint64:
			limit = int64(v)
		case int:
			limit = int64(v)
		case int32:
			limit = int64(v)
		}
	}
	if c.length > limit {
		ctx.Warn(1301, "Result of cast_as_binary() was larger than max_allowed_packet (%d) - truncated", limit)
		return nil, nil
	}
	return c.Convert.Eval(ctx, row)
}

func (c *limitedConvert) IsNullable() bool { return true }

func (c *limitedConvert) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	e, err := c.Convert.WithChildren(children...)
	if err != nil {
		return nil, err
	}
	conv, ok := e.(*expression.Convert)
	if !ok {
		return e, nil
	}
	return &limitedConvert{Convert: conv, length: c.length}, nil
}
