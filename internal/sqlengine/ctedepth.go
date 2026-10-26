package sqlengine

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/vitess/go/mysql"
)

// go-mysql-server stops a recursive CTE only after a fixed 10,001
// iterations, ignoring @@cte_max_recursion_depth (default 1000), so a CTE
// whose rows double each iteration grew until the process ran out of
// memory (issue #18). Each iteration builds the recursive part again; the
// builder puts a cteDepthGuard over it, which counts the builds and fails
// as MySQL does (3636) once they exceed the limit. MySQL counts the last,
// empty iteration too, and so do the builds.

// The guard also counts the rows the recursive part produces: a CTE whose
// rows double each iteration exhausts memory long before 1000 iterations.
// Past maxCTERows it fails as MySQL does when the temporary table holding
// them is full (1114).
type cteDepthGuard struct {
	child sql.Node
	name  string // the CTE's
	limit int64
	n     int64
	rows  int64
}

// maxCTERows bounds the rows one recursive CTE buffers.
var maxCTERows int64 = 4 << 20

type cteRowsIter struct {
	sql.RowIter
	g      *cteDepthGuard
	polled uint32
}

func (it *cteRowsIter) Next(ctx *sql.Context) (sql.Row, error) {
	if err := pollCancel(ctx, &it.polled); err != nil {
		return nil, err
	}
	r, err := it.RowIter.Next(ctx)
	if err == nil {
		if it.g.rows++; it.g.rows > maxCTERows {
			return nil, mysql.NewSQLError(1114, "HY000", "The table '%s' is full", it.g.name)
		}
	}
	return r, err
}

var _ sql.Node = (*cteDepthGuard)(nil)

func (g *cteDepthGuard) Resolved() bool       { return g.child.Resolved() }
func (g *cteDepthGuard) IsReadOnly() bool     { return g.child.IsReadOnly() }
func (g *cteDepthGuard) String() string       { return g.child.String() }
func (g *cteDepthGuard) Schema() sql.Schema   { return g.child.Schema() }
func (g *cteDepthGuard) Children() []sql.Node { return []sql.Node{g.child} }
func (g *cteDepthGuard) WithChildren(c ...sql.Node) (sql.Node, error) {
	if len(c) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(g, len(c), 1)
	}
	n := *g
	n.child = c[0]
	return &n, nil
}

// guardRecursiveCte is n with its recursive part guarded; nil if it is
// already.
func guardRecursiveCte(ctx *sql.Context, n *plan.RecursiveCte) (sql.Node, error) {
	if _, ok := n.Right().(*cteDepthGuard); ok {
		return nil, nil
	}
	limit := int64(1000)
	if v, err := ctx.GetSessionVariable(ctx, "cte_max_recursion_depth"); err == nil {
		if i, err := fnInt64(ctx, v); err == nil {
			limit = i
		}
	}
	return n.WithChildren(n.Left(), &cteDepthGuard{child: n.Right(), name: n.Name(), limit: limit})
}

// next counts a build of the recursive part.
func (g *cteDepthGuard) next() error {
	g.n++
	if g.n > g.limit {
		return mysql.NewSQLError(3636, "HY000", "%s", fmt.Sprintf("Recursive query aborted after %d iterations. Try increasing @@cte_max_recursion_depth to a larger value.", g.n))
	}
	return nil
}
