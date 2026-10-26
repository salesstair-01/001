package sqlengine

import (
	"sync"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

// go-mysql-server analyzes an INSERT of literal values, a single-table
// UPDATE and a single-table DELETE with a short list of its own rules
// (analyzer/node_batches.go), skipping the rules this package adds. The
// execution builder applies the expression rules among them to those
// statements instead, before building them.

// simpleDMLRules are the added rules that rewrite expressions, in the
// analyzer's order.
var simpleDMLRules = []analyzer.RuleFunc{explicitLikeEscape, castLimit, constFold, numDatetime, intArith}

func isSimpleDML(n sql.Node) bool {
	switch n := n.(type) {
	case *plan.InsertInto:
		return n.LiteralValueSource
	case *plan.Update:
		return n.HasSingleRel && !n.IsJoin
	case *plan.DeleteFrom:
		return !n.HasExplicitTargets() && n.RefsSingleRel
	}
	return false
}

// rewritten holds the statements simpleDML returned, which the builder then
// sees again and builds as they are.
var rewritten sync.Map

// simpleDML applies simpleDMLRules to a simple statement; nil when none
// changes it.
func simpleDML(ctx *sql.Context, n sql.Node) (sql.Node, error) {
	if _, ok := rewritten.LoadAndDelete(n); ok || !isSimpleDML(n) {
		return nil, nil
	}
	changed := false
	for _, rule := range simpleDMLRules {
		nn, same, err := rule(ctx, nil, n, nil, analyzer.DefaultRuleSelector, nil)
		if err != nil {
			return nil, err
		}
		if !same {
			n, changed = nn, true
		}
	}
	if !changed {
		return nil, nil
	}
	rewritten.Store(n, struct{}{})
	return n, nil
}
