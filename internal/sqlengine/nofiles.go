package sqlengine

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/expression/function"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/vitess/go/mysql"
)

// A query engine serves many tenants, so a statement must not touch its
// file system. go-mysql-server reads and writes files on the query engine's
// host for SELECT ... INTO OUTFILE / DUMPFILE, LOAD DATA INFILE (not LOCAL)
// and LOAD_FILE(), anywhere when secure_file_priv is empty (its default;
// found by MySQL's test suite, see mtr/). They are refused as MySQL refuses
// them with secure_file_priv = NULL. LOAD DATA LOCAL reads the client's file
// and stays allowed.

const noFilesID analyzer.RuleId = 10003

var errNoFiles = mysql.NewSQLError(1290, "HY000",
	"The MySQL server is running with the --secure-file-priv option so it cannot execute this statement")

func noFiles(ctx *sql.Context, a *analyzer.Analyzer, n sql.Node, scope *plan.Scope, sel analyzer.RuleSelector, qf *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	if touchesFiles(n) {
		return nil, transform.SameTree, errNoFiles
	}
	return n, transform.SameTree, nil
}

func touchesFiles(n sql.Node) bool {
	found := false
	transform.Inspect(n, func(n sql.Node) bool {
		if n == nil || found {
			return !found
		}
		switch n := n.(type) {
		case *plan.Into:
			if n.Outfile != "" || n.Dumpfile != "" {
				found = true
			}
		case *plan.LoadData:
			if !n.Local {
				found = true
			}
		}
		if e, ok := n.(sql.Expressioner); ok {
			for _, x := range e.Expressions() {
				transform.InspectExpr(x, func(x sql.Expression) bool {
					switch x := x.(type) {
					case *function.LoadFile:
						found = true
					case *plan.Subquery:
						found = found || touchesFiles(x.Query)
					}
					return found
				})
			}
		}
		return !found
	})
	return found
}
