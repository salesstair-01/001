package sqlengine

import (
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/vt/sqlparser"
)

// Deprecation warnings MySQL 8 gives while parsing a statement, which
// go-mysql-server doesn't (issue #28; among the warnings most often missing
// in MySQL's test suite): 3719 for each 'utf8' as a character set name,
// 3778 for a utf8mb3 collation, 1287 for 'utf8mb3' and for each BINARY
// operator (BINARY 'a' = 'A'; not CAST(x AS BINARY), nor the column type),
// for @v := e in an expression, SQL_CALC_FOUND_ROWS and FOUND_ROWS().
// They come from the statement's tokens, once per statement.

const (
	warnUTF8Alias  = "'utf8' is currently an alias for the character set UTF8MB3, but will be an alias for UTF8MB4 in a future release. Please consider using UTF8MB4 in order to be unambiguous."
	warnUTF8MB3    = "'utf8mb3' is deprecated and will be removed in a future release. Please use utf8mb4 instead"
	warnBinaryExpr = "'BINARY expr' is deprecated and will be removed in a future release. Please use CAST instead"
	warnAssignExpr = "Setting user variables within expressions is deprecated and will be removed in a future release. Consider alternatives: 'SET variable=expression, ...', or 'SELECT expression(s) INTO variables(s)'."
	warnCalcFound  = "SQL_CALC_FOUND_ROWS is deprecated and will be removed in a future release. Consider using two separate queries instead."
	warnFoundRows  = "FOUND_ROWS() is deprecated and will be removed in a future release. Consider using COUNT(*) instead."
)

type lexToken struct {
	typ int
	val string // lower-case
}

func lexWarnings(ctx *sql.Context) {
	if s, ok := ctx.Session.(*Session); ok {
		if s.lexWarned == ctx.Pid() {
			return
		}
		s.lexWarned = ctx.Pid()
	}
	q := ctx.Query()
	low := strings.ToLower(q)
	if !strings.Contains(low, "utf8") && !strings.Contains(low, "binary") && !strings.Contains(low, ":=") &&
		!strings.Contains(low, "found_rows") {
		return
	}
	var toks []lexToken
	tz := sqlparser.NewStringTokenizer(q)
	for {
		typ, val := tz.Scan()
		if typ == 0 || typ == sqlparser.LEX_ERROR {
			break
		}
		toks = append(toks, lexToken{typ, strings.ToLower(string(val))})
	}
	ddl := len(toks) > 0 && (toks[0].typ == sqlparser.CREATE || toks[0].typ == sqlparser.ALTER)
	set := len(toks) > 0 && toks[0].typ == sqlparser.SET
	depth := 0
	for i, t := range toks {
		switch t.typ {
		case '(':
			depth++
		case ')':
			depth--
		}
		prev := func(k int) lexToken {
			if i-k >= 0 {
				return toks[i-k]
			}
			return lexToken{}
		}
		next := lexToken{}
		if i+1 < len(toks) {
			next = toks[i+1]
		}
		switch {
		case t.typ == sqlparser.UNDERSCORE_UTF8:
			ctx.Warn(3719, "%s", warnUTF8Alias)
		case t.typ == sqlparser.ID && t.val == "utf8":
			ctx.Warn(3719, "%s", warnUTF8Alias)
		case t.typ == sqlparser.ID && t.val == "utf8mb3":
			ctx.Warn(1287, "%s", warnUTF8MB3)
		case t.typ == sqlparser.ID && (strings.HasPrefix(t.val, "utf8_") || strings.HasPrefix(t.val, "utf8mb3_")) &&
			(prev(1).typ == sqlparser.COLLATE || (prev(1).typ == '=' && prev(2).typ == sqlparser.COLLATE)):
			name := "utf8mb3_" + t.val[strings.IndexByte(t.val, '_')+1:]
			ctx.Warn(3778, "'%s' is a collation of the deprecated character set UTF8MB3. Please consider using UTF8MB4 with an appropriate collation instead.", name)
		case t.typ == sqlparser.ASSIGNMENT_OP:
			// SET @v := e assigns; @v := e anywhere else is an expression
			if !(set && depth == 0 && (prev(2).typ == sqlparser.SET || prev(2).typ == ',')) {
				ctx.Warn(1287, "%s", warnAssignExpr)
			}
		case t.typ == sqlparser.SQL_CALC_FOUND_ROWS:
			ctx.Warn(1287, "%s", warnCalcFound)
		case t.val == "found_rows" && next.typ == '(':
			ctx.Warn(1287, "%s", warnFoundRows)
		case t.typ == sqlparser.BINARY && !ddl && prev(1).typ != sqlparser.AS &&
			next.typ != '(' && next.typ != ')' && next.typ != ',':
			ctx.Warn(1287, "%s", warnBinaryExpr)
		}
	}
}
