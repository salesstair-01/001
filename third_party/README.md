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

Tests: `go/mysql/s2z_okinfo_test.go`, `go/vt/sqlparser/s2z_comments_test.go`
(`cd third_party/vitess/go && go test ./mysql ./vt/sqlparser`, in CI's test
job; vitess's own tests in both packages pass too). `internal/sqlengine` and
`internal/proxy` test the info end to end (`TestOKPacketInfo`,
`TestInfoAndWarningsPassThrough`).

To upgrade: copy the new version over `vitess/` and re-apply the changes
above.
