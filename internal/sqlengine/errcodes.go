package sqlengine

import (
	"context"
	"errors"
	"regexp"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/sqltypes"
	querypb "github.com/dolthub/vitess/go/vt/proto/query"
)

// go-mysql-server reports many errors as 1105 (ER_UNKNOWN_ERROR) where
// MySQL has a code of its own; clients and tests that branch on the code
// (a parse error is 1064, a duplicate table 1050) can't tell them apart.
// The handler gives such errors MySQL's code and SQLSTATE, by their message
// (found by MySQL's test suite, see mtr/).
var errCodes = []struct {
	re    *regexp.Regexp
	code  int
	state string
}{
	{regexp.MustCompile(`^syntax error at position`), 1064, "42000"},
	{regexp.MustCompile(`could not be found in any table in scope`), 1054, "42S22"},
	{regexp.MustCompile(`^table with name \S+ already exists`), 1050, "42S01"},
	{regexp.MustCompile(`^no database selected`), 1046, "3D000"},
	{regexp.MustCompile(`^ambiguous column name`), 1052, "23000"},
	{regexp.MustCompile(`^number of values does not match number of columns provided`), 1136, "21S01"},
	{regexp.MustCompile(`^Invalid default value for`), 1067, "42000"},
	{regexp.MustCompile(`^function: '[^']*' not found`), 1305, "42000"},
	{regexp.MustCompile(`^Incorrect datetime value`), 1292, "22007"},
	{regexp.MustCompile(`^Check constraint ".*" violated`), 3819, "HY000"},
	{regexp.MustCompile(`^Constraint ".*" does not exist`), 3821, "HY000"},
	{regexp.MustCompile(`^trigger ".*" does not exist`), 1360, "HY000"},
	{regexp.MustCompile(`^Updating of (old|new) row is not allowed`), 1362, "HY000"},
	{regexp.MustCompile(`^stored procedure ".*" does not exist`), 1305, "42000"},
	{regexp.MustCompile(`^Result consisted of more than one row`), 1172, "42000"},
}

var (
	errExecutionTime = mysql.NewSQLError(3024, "HY000", "Query execution was interrupted, maximum statement execution time exceeded")
	errInterrupted   = mysql.NewSQLError(1317, "70100", "Query execution was interrupted")
)

func withMySQLCode(err error) error {
	var se *mysql.SQLError
	if err == nil || (errors.As(err, &se) && se.Num != mysql.ERUnknownError) {
		return err
	}
	msg := err.Error()
	if se != nil {
		msg = se.Message
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded) || msg == context.DeadlineExceeded.Error():
		return errExecutionTime
	case errors.Is(err, context.Canceled) || msg == context.Canceled.Error():
		return errInterrupted
	case sql.ErrNoMemoryAvailable.Is(err) || msg == "no memory available":
		memRejected.Inc()
		return errOutOfMemory
	}
	for _, c := range errCodes {
		if c.re.MatchString(msg) {
			return mysql.NewSQLError(c.code, c.state, "%s", msg)
		}
	}
	return err
}

func (h autocommitHandler) ComQuery(ctx context.Context, c *mysql.Conn, query string, callback mysql.ResultSpoolFn) error {
	ctx, cancel := context.WithCancel(ctx) // ends a statement's max_execution_time timer (exectime.go)
	defer cancel()
	return withQueryCode(h.Handler.ComQuery(ctx, c, query, padFractions(callback)), query)
}

func (h autocommitHandler) ComMultiQuery(ctx context.Context, c *mysql.Conn, query string, callback mysql.ResultSpoolFn) (string, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	rest, err := h.Handler.ComMultiQuery(ctx, c, query, padFractions(callback))
	return rest, withQueryCode(err, query)
}

var (
	reTriggerRowErr = regexp.MustCompile(`^table not found: (new|old)$`)
	reTriggerEvent  = regexp.MustCompile(`(?is)^\s*create\s.*?\btrigger\s+\S+\s+(?:before|after)\s+(insert|update|delete)\b`)
)

// withQueryCode is withMySQLCode, knowing the statement: in CREATE TRIGGER,
// go-mysql-server's "table not found: new" is MySQL's 1363 for the row the
// event lacks (OLD in an INSERT trigger, NEW in a DELETE one) and 1054
// otherwise (a column the table lacks).
func withQueryCode(err error, query string) error {
	if err == nil {
		return nil
	}
	if m := reTriggerRowErr.FindStringSubmatch(sqlErrorMessage(err)); m != nil {
		if e := reTriggerEvent.FindStringSubmatch(query); e != nil {
			row, event := strings.ToUpper(m[1]), strings.ToUpper(e[1])
			if (row == "OLD" && event == "INSERT") || (row == "NEW" && event == "DELETE") {
				return mysql.NewSQLError(1363, "HY000", "There is no %s row in on %s trigger", row, event)
			}
			return mysql.NewSQLError(1054, "42S22", "Unknown column in '%s'", row)
		}
	}
	return withMySQLCode(err)
}

func sqlErrorMessage(err error) string {
	var se *mysql.SQLError
	if errors.As(err, &se) {
		return se.Message
	}
	return err.Error()
}

func (h autocommitHandler) ComPrepare(ctx context.Context, c *mysql.Conn, query string, prepare *mysql.PrepareData) ([]*querypb.Field, error) {
	fields, err := h.Handler.ComPrepare(ctx, c, query, prepare)
	return fields, withMySQLCode(err)
}

func (h autocommitHandler) ComStmtExecute(ctx context.Context, c *mysql.Conn, prepare *mysql.PrepareData, callback func(*sqltypes.Result) error) error {
	spool := padFractions(func(r *sqltypes.Result, _ bool) error { return callback(r) })
	return withMySQLCode(h.Handler.ComStmtExecute(ctx, c, prepare, func(r *sqltypes.Result) error { return spool(r, false) }))
}
