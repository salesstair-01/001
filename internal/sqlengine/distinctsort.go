package sqlengine

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// SELECT DISTINCT expr AS c ... ORDER BY 1 (an ordinal, or a select
// expression repeated) failed with "unable to find field with index N":
// go-mysql-server sorts above the DISTINCT by the aliased expression, which
// reads columns the distinct rows no longer have (found by MySQL's test
// suite, issue #17), or did not sort when another sort field was a plain
// column. A pre-analyze rule sorts below the projection instead, by the
// expression under the alias, as go-mysql-server does for ORDER BY c;
// its DISTINCT keeps the first row of each value in input order, so the
// result is the same.

const distinctSortID analyzer.RuleId = 10006

func distinctSort(ctx *sql.Context, a *analyzer.Analyzer, n sql.Node, scope *plan.Scope, sel analyzer.RuleSelector, qf *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	return transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		s, ok := n.(*plan.Sort)
		if !ok {
			return n, transform.SameTree, nil
		}
		d, ok := s.Child.(*plan.Distinct)
		if !ok {
			return n, transform.SameTree, nil
		}
		p, ok := d.Child.(*plan.Project)
		if !ok {
			return n, transform.SameTree, nil
		}
		below := p.Child.Schema()
		fields := make(sql.SortFields, len(s.SortFields))
		for i, f := range s.SortFields {
			switch c := f.Column.(type) {
			case *expression.Alias:
				f.Column = c.Child
				f.Column2, _ = c.Child.(sql.Expression2)
			case *expression.GetField:
				if below.IndexOfColName(c.Name()) < 0 {
					return n, transform.SameTree, nil
				}
			default:
				return n, transform.SameTree, nil
			}
			fields[i] = f
		}
		sorted := plan.NewSort(fields, p.Child)
		proj, err := p.WithChildren(sorted)
		if err != nil {
			return nil, transform.SameTree, err
		}
		dist, err := d.WithChildren(proj)
		if err != nil {
			return nil, transform.SameTree, err
		}
		return dist, transform.NewTree, nil
	})
}
