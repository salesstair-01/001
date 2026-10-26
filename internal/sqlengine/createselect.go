package sqlengine

import (
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// CREATE TABLE t (c int DEFAULT 9, a bigint) SELECT a, b FROM s makes
// columns c, a, b, as in MySQL (go-mysql-server merges the schemas so), and
// fills each from the SELECT column of the same name, or its default; but
// go-mysql-server inserted the SELECT's rows by position (1366: 'x' is not
// a valid value for bigint). A post-analyze rule puts a projection in table
// order on the SELECT.

const createSelectID analyzer.RuleId = 10009

func createSelect(ctx *sql.Context, a *analyzer.Analyzer, n sql.Node, scope *plan.Scope, sel analyzer.RuleSelector, qf *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	tc, ok := n.(*plan.TableCopier)
	if !ok {
		return n, transform.SameTree, nil
	}
	ct, ok := tc.Destination.(*plan.CreateTable)
	if !ok {
		return n, transform.SameTree, nil
	}
	src, dst := tc.Source.Schema(), ct.PkSchema().Schema
	positional := len(src) == len(dst)
	for i := 0; positional && i < len(src); i++ {
		positional = strings.EqualFold(src[i].Name, dst[i].Name)
	}
	if positional {
		return n, transform.SameTree, nil
	}
	exprs := make([]sql.Expression, len(dst))
	for i, col := range dst {
		j := src.IndexOfColName(col.Name)
		switch {
		case j >= 0:
			exprs[i] = expression.NewGetField(j, src[j].Type, src[j].Name, src[j].Nullable)
		case col.Default != nil:
			exprs[i] = col.Default
		default:
			exprs[i] = expression.NewLiteral(nil, types.Null)
		}
	}
	ntc := *tc
	ntc.Source = plan.NewProject(exprs, tc.Source)
	return &ntc, transform.NewTree, nil
}
