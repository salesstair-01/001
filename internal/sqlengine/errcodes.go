package sqlengine

import (
	"context"
	"errors"
	"regexp"

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
}

func withMySQLCode(err error) error {
	var se *mysql.SQLError
	if err == nil || (errors.As(err, &se) && se.Num != mysql.ERUnknownError) {
		return err
	}
	msg := err.Error()
	if se != nil {
		msg = se.Message
	}
	for _, c := range errCodes {
		if c.re.MatchString(msg) {
			return mysql.NewSQLError(c.code, c.state, "%s", msg)
		}
	}
	return err
}

func (h autocommitHandler) ComQuery(ctx context.Context, c *mysql.Conn, query string, callback mysql.ResultSpoolFn) error {
	return withMySQLCode(h.Handler.ComQuery(ctx, c, query, callback))
}

func (h autocommitHandler) ComMultiQuery(ctx context.Context, c *mysql.Conn, query string, callback mysql.ResultSpoolFn) (string, error) {
	rest, err := h.Handler.ComMultiQuery(ctx, c, query, callback)
	return rest, withMySQLCode(err)
}

func (h autocommitHandler) ComPrepare(ctx context.Context, c *mysql.Conn, query string, prepare *mysql.PrepareData) ([]*querypb.Field, error) {
	fields, err := h.Handler.ComPrepare(ctx, c, query, prepare)
	return fields, withMySQLCode(err)
}

func (h autocommitHandler) ComStmtExecute(ctx context.Context, c *mysql.Conn, prepare *mysql.PrepareData, callback func(*sqltypes.Result) error) error {
	return withMySQLCode(h.Handler.ComStmtExecute(ctx, c, prepare, callback))
}
