package sqlengine

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// go-mysql-server 0.20 rewrites `s LIKE 'p%'` to `s >= 'p' AND s <= 'p\xff'`,
// which drops every string that continues with a character sorting after the
// byte 0xFF (emoji, CJK, and others, depending on the collation); the MySQL
// differential tests found it. The rewrite skips a LIKE with an ESCAPE
// clause, so this rule gives each LIKE the default escape explicitly, which
// changes nothing else. The price is that a prefix LIKE no longer becomes an
// index range.
const explicitLikeEscapeID analyzer.RuleId = 10001

func explicitLikeEscape(ctx *sql.Context, a *analyzer.Analyzer, n sql.Node, scope *plan.Scope, sel analyzer.RuleSelector, qf *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	return transform.NodeExprsWithOpaque(n, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
		switch e := e.(type) {
		case *expression.Like:
			if e.Escape != nil {
				return e, transform.SameTree, nil
			}
			return expression.NewLike(e.LeftChild, e.RightChild, expression.NewLiteral(`\`, types.LongText)), transform.NewTree, nil
		case *plan.Subquery:
			q, same, err := explicitLikeEscape(ctx, a, e.Query, scope, sel, qf)
			if err != nil || same {
				return e, transform.SameTree, err
			}
			return e.WithQuery(q), transform.NewTree, nil
		}
		return e, transform.SameTree, nil
	})
}
