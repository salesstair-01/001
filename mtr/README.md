# mtr: MySQL's own test suite against sqld

MySQL ships its regression tests as `mysql-test/t/*.test` scripts in the
mysqltest language, with the expected output in `mysql-test/r/*.result`.
`mtr` is a Rust implementation of the part of mysqltest those tests use:
it runs a test file against `sqld` (or any MySQL-protocol server), writes
the output mysqltest would write, and compares it with MySQL's recorded
result statement by statement.

```sh
make mtr                       # fetch MySQL 8.0.46's tests, run scripts/mtr/files.txt against
                               # a fresh kvd + sqld, write scripts/mtr/results.md
mtr/target/release/mtr -bin bin trigger insert      # some files, summary on stdout
mtr/target/release/mtr -dsn 127.0.0.1:13306 -label mysql insert   # against a scratch MySQL
```

The test files are GPL-2.0, so they are not in this repository:
`scripts/mtr/fetch.sh` fetches them (a sparse checkout of the
`mysql-8.0.46` tag) into `scripts/mtr/mysql-server`, as
`scripts/slt/fetch.sh` does for sqllogictest.

## What is compared

Each statement's output (result rows, `ERROR <sqlstate>: <message>`,
`Warnings:`, `affected rows:` with `--enable_info`) is found in the
`.result` file by its echoed text and compared exactly. A statement that
the test expects to fail (`--error`) passes when it fails with one of the
expected codes; the message is not compared. Statements are aligned by
the longest in-order match of their echoes, so one missing or extra
statement does not shift the rest; `--echo` lines are matched between the
statements around them.

`scripts/mtr/baseline.txt` lists the statements that do not reproduce
their recorded result on MySQL 8.0.46 itself under this harness (server
settings, timing, a mysqltest feature the harness lacks); they are left out
of sqld's score. Of the statements in `files.txt`, about 98% reproduce on
MySQL. Regenerate it after changing the list or the harness:

```sh
mysqld --no-defaults --initialize-insecure --user=root --datadir=/tmp/my/data
mysqld --no-defaults --user=root --datadir=/tmp/my/data --port=13306 \
  --max-allowed-packet=64M --log-bin-trust-function-creators=1 &   # as MTR starts it
make mtr-baseline MTR_MYSQL=127.0.0.1:13306
```

Every database but the system ones is dropped before each file, so point
`-dsn` only at a scratch server.

## The mysqltest subset

Statements and delimiters (`delimiter`), result format 1 (indentation of
continuation lines dropped, as `convert_to_format_v1` does), `--echo`,
`--error` (names, codes, `S<sqlstate>`), `let` (values, `` `query` ``,
`query_get_value`), `eval`, `inc`/`dec`, `if`/`while`/`else` with
mysqltest's conditions, `source`, connections (`connect`, `connection`,
`disconnect`, `send`/`reap` on a thread), `--sorted_result`,
`--replace_column`, `--replace_result`, `--replace_regex`,
`--lowercase_result`, `--query_vertical`, warnings (`SHOW WARNINGS` when the
server reports any, as mysqltest does), `--enable_info`,
`--disable_query_log`/`result_log`/`warnings`, `--disable_testcase`,
`--disable_parsing`, `$mysql_errno`, and MTR's `mtr.add_suppression`.
Variable names are case-insensitive, as in mysqltest.

Not implemented (reported per file as skipped commands): file and process
commands (`exec`, `write_file`, `perl`, ...), server restarts (the include
is a no-op), replication commands.

## Files

| file | what |
|---|---|
| `src/script.rs` | the mysqltest language parser |
| `src/run.rs` | the interpreter: variables, connections, output in .result format |
| `src/compare.rs` | aligning the output with the .result, scoring statements |
| `src/errors.rs` | MySQL error names, from `share/messages_to_clients.txt` |
| `src/main.rs` | the command line, kvd + sqld start-up, summaries |
