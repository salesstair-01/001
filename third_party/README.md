# Vendored Go modules

## vitess

`github.com/dolthub/vitess` at `v0.0.0-20250512224608-8fb9c6ea092c`
(Apache-2.0; the version go-mysql-server 0.20.0 uses), used through a
`replace` in `../go.mod`. It is go-mysql-server's MySQL protocol and parser,
and the proxy's client and server. Changes, each marked `s2z patch` in the
code:

| change | file |
|---|---|
| An OK packet's info string is written as a `string<lenenc>`, as MySQL's `net_send_ok` writes it. Upstream wrote a `#` byte and a `string<EOF>`, which clients that parse the info read as a 35-byte length: `PREPARE` dropped the connection of the Rust `mysql` crate, and longer infos were truncated (issue #15). | `go/mysql/conn.go` (`writeOKPacketWithInfo`) |
| The client keeps the info of the OK packet it reads (`Result.Info`), so the proxy can pass it on. | `go/mysql/conn.go` (`parseOKPacketInfo`), `go/mysql/query.go` |
| In a `/*! ... */` comment, a version is 5 digits, as in MySQL; fewer digits are SQL. Upstream sliced `sql[0:-1]` and panicked on `/*!2*/` (issue #17). | `go/vt/sqlparser/comments.go` (`ExtractMysqlComment`) |
| Grammar (issue #27), each as MySQL accepts it: `CREATE TABLE t (columns) [AS] SELECT ...`; `DROP TABLES`; `LOCK TABLE`; `IS [NOT] UNKNOWN`; `YEAR(n)`; `TEXT(n)`/`BLOB(n)` as the smallest type that holds n; `STD`, `VARIANCE` and the other statistics aggregates as column aliases; index hints with `KEY`, `FOR JOIN / ORDER BY / GROUP BY`, `PRIMARY` and several hints; `ALTER TABLE ... ADD CONSTRAINT PRIMARY KEY`; `KEY USING BTREE (c)`; a parenthesized query as a statement, with `ORDER BY` / `LIMIT` after it; `~x` as `x ^ 0xFFFFFFFFFFFFFFFF` (go-mysql-server has no unary `~`). | `go/vt/sqlparser/sql.y` (regenerated `sql.go`), `s2z_patches.go` |
| Grammar: `x op ANY / SOME / ALL (subquery)` (`ComparisonExpr.Quantifier`; `any` and `some` stay usable as names, a `(` after them in a comparison starting the subquery); `ROW(a, b)` row constructors; `@v := expr` in an expression, a call of `s2z_assign_user_var`, which sqld defines (`internal/sqlengine/uservar.go`); `DO expr, ...` as `SET @s2z_do = expr, ...`; `DELETE [LOW_PRIORITY] [IGNORE]`, the options without effect. | `go/vt/sqlparser/sql.y` (regenerated `sql.go`), `ast.go`, `keywords.go`, `s2z_patches.go` |
| `ORDER BY std` (and the other statistics aggregates' names) orders by a column alias of that name. | `go/vt/sqlparser/sql.y` (regenerated `sql.go`) |
| Stored functions (issue #20): `CREATE FUNCTION f(params) RETURNS type [CHARSET c] [COLLATE c] characteristics body` is `CREATE PROCEDURE s2z_fn_f(params, OUT s2z_ret type) s2z_fn: BEGIN body END`, each `RETURN e` `BEGIN SET s2z_ret = e; LEAVE s2z_fn; END`; `DROP FUNCTION [IF EXISTS] f` drops that procedure. sqld stores it as a function and calls it (`internal/sqlengine/storedfunc.go`). `RETURNS` is a non-reserved keyword; a `(` right after a column type is its length (precedence `LOWER_THAN_PAREN`), which keeps the grammar free of conflicts. | `go/vt/sqlparser/sql.y` (regenerated `sql.go`), `keywords.go`, `s2z_patches.go` (`functionAsProcedure`, `rewriteReturns`) |
| Typed literals (issue #25): `DATE '...'`, `TIME '...'` and `TIMESTAMP '...'` in an expression become calls of `s2z_date_literal` / `s2z_time_literal` / `s2z_timestamp_literal`, which sqld defines (`internal/sqlengine/typedliteral.go`); upstream drops them to plain strings. As column defaults they stay strings. Compound `INTERVAL` units (`YEAR_MONTH`, `DAY_HOUR`, ...), which upstream refuses after `INTERVAL expr`. A `TIME(n)` column type is TIME (go-mysql-server refuses n below 6). | `go/vt/sqlparser/sql.y` (regenerated `sql.go`), `ast.go` (`SQLVal.Typed`), `s2z_patches.go` |

Tests: `go/mysql/s2z_okinfo_test.go`, `go/vt/sqlparser/s2z_comments_test.go`,
`go/vt/sqlparser/s2z_syntax_test.go`
(`cd third_party/vitess/go && go test ./mysql ./vt/sqlparser`, in CI's test
job; vitess's own tests in both packages pass too). `internal/sqlengine` and
`internal/proxy` test the info end to end (`TestOKPacketInfo`,
`TestInfoAndWarningsPassThrough`).

One upstream test expected `(select ... from t)` to be a syntax error; it is
removed (`parse_test.go`).

`sql.go` is generated from `sql.y` by goyacc (`golang.org/x/tools/cmd/goyacc`;
v0.38.0 was used, with no grammar conflicts):

    cd third_party/vitess/go/vt/sqlparser && goyacc -o sql.go sql.y && rm y.output && gofmt -w sql.go

To upgrade: copy the new version over `vitess/` and re-apply the changes
above.

## go-mysql-server

`github.com/dolthub/go-mysql-server` at `v0.20.0` (Apache-2.0), used through
a `replace` in `../go.mod`: sqld's query engine. Left out of the copy: tests
and test data, `enginetest`, `_example`, `_integration` and the collation
table generator (`sql/encodings/generate`, a `package main` nothing
imports). Changes, each marked `s2z patch` in the code:

| change | file |
|---|---|
| A multi-table UPDATE updates tables without a primary key (issue #29): a keyless table implementing `sql.KeylessJoinUpdatable` gets an updater for distinct join rows (the join dedupes a table's rows by content, so identical rows are updated together), and `sql.KeylessJoinRowCounter` makes the matched and changed counts count them all. Upstream refused keyless tables. | `sql/tables.go`, `sql/planbuilder/dml.go`, `sql/analyzer/assign_update_join.go` (`rowUpdatersByTable`), `sql/rowexec/update.go`, `sql/rowexec/dml_iters.go` |
| Stored procedure handlers (issue #19): the handler's whole statement runs (a BEGIN ... END body was skipped after its first operation), its rows to their end (their `io.EOF` was taken for a jump to -1: the procedure started over, forever); an EXIT handler resumes at its block's end so the block's handlers are popped (they stayed in force, and a later error looped). | `sql/procedures/interpreter_logic.go` (`handleError`) |
| `SELECT ... INTO @var` in a procedure sets the user variable (it failed on the name); no row warns 1329 and leaves the variables (it returned `io.EOF`, a jump); two rows fail with "more than one row". | `sql/procedures/interpreter_logic.go` (`execOp`) |
| A procedure's local variable takes its declared type when set (`SELECT SUM(x) INTO v` kept the float in an INT `v`, then wrote it back into a statement as an invalid integer). | `sql/procedures/interpreter_stack.go` (`SetVariable`) |
| Window functions over a grouped query (`RANK() OVER (ORDER BY SUM(b)) ... GROUP BY a`, `SUM(SUM(b)) OVER ()`), which upstream refused (1140) or mis-resolved: the SELECT is rewritten over a derived table that does the grouping, each largest window-free part of the select list, ORDER BY and window definitions a column of it (select-list columns keep their names). | `sql/planbuilder/s2z_groupwindow.go`, `sql/planbuilder/select.go` (`buildSelect`) |
| An index lookup on an integer column with a constant outside its range (`some_id IN ('-1', '0')` on SMALLINT UNSIGNED, `< '99999'`) compares as MySQL does: no row for `=`/`IN`, every non-NULL row for `<>`, the whole or no range for `<`/`>`; a string is read as its numeric prefix (0 for none). Upstream failed the query (1366) converting the key. | `sql/index_builder.go` (`rangeKey`) |
| Quantified comparisons (`plan.QuantifiedSubquery`): `x op ANY (subquery)` is true when the comparison is true for a row, false for none, else NULL if one was NULL; `ALL` false when it is false for a row, true for none, else NULL if one was NULL; `= ANY` is IN and `<> ALL` NOT IN. | `sql/plan/s2z_quantified.go`, `sql/planbuilder/scalar.go` (`buildComparison`) |
| `CUME_DIST()` and `NTH_VALUE(expr, n)` window functions (issue #26); NTH_VALUE's default frame is MySQL's (the partition up to the current row's peers). | `sql/expression/function/aggregation/s2z_window_functions.go`, `.../aggregation/window/s2z_cume_nth.go`, `sql/expression/function/registry.go`, `sql/planbuilder/aggregates.go` (`isWindowFunc`) |
| `STD`, `STDDEV_SAMP`, `VAR_POP`, `VAR_SAMP` (and their aliases) are DOUBLE, as in MySQL; they had their argument's type, so over an integer column a window's 2.25 came out as 2. | `sql/expression/function/aggregation/unary_aggs.og.go` |
