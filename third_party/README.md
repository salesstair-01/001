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
