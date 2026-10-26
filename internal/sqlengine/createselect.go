package sqlengine

import (
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/sqltypes"
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
	userVarColumns(ctx, tc.Source, dst)
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

// userVarColumns gives a column of NULLs, and one of a user variable, the
// type MySQL gives it. go-mysql-server gave NULLs the NULL type, which it
// stored and then could not read back (the table was unusable); MySQL's is
// BINARY(0), or for a user variable LONGBLOB when it is set (to NULL) and
// LONGTEXT when it is not. A user variable's column is BIGINT, DOUBLE,
// DECIMAL(65,30) or LONGTEXT, where go-mysql-server took its value's
// literal type (TINYINT for 1).
func userVarColumns(ctx *sql.Context, src sql.Node, dst sql.Schema) {
	var exprs []sql.Expression
	if p, ok := src.(*plan.Project); ok {
		exprs = p.Projections
	}
	for _, col := range dst {
		var uv *expression.UserVar
		for _, e := range exprs {
			if a, ok := e.(*expression.Alias); ok && strings.EqualFold(a.Name(), col.Name) {
				e = a.Child
			} else if !strings.EqualFold(e.String(), col.Name) {
				continue
			}
			uv, _ = e.(*expression.UserVar)
			break
		}
		switch {
		case col.Type == types.Null && uv == nil:
			col.Type = types.MustCreateBinary(sqltypes.Binary, 0)
		case uv == nil:
		case col.Type == types.Null:
			col.Type = types.LongText
			if typ, _, err := ctx.GetUserVariable(ctx, uv.Name); err == nil && typ != nil {
				col.Type = types.LongBlob
			}
		case types.IsUnsigned(col.Type):
			col.Type = types.Uint64
		case types.IsInteger(col.Type):
			col.Type = types.Int64
		case types.IsFloat(col.Type):
			col.Type = types.Float64
		case types.IsDecimal(col.Type):
			col.Type = types.MustCreateDecimalType(65, 30)
		case types.IsText(col.Type):
			col.Type = types.LongText
		}
	}
}
