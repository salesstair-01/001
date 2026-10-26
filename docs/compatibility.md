# MySQL compatibility (v1)

What an application sees when it talks to this system instead of MySQL 8.0.
The SQL layer is [go-mysql-server](https://github.com/dolthub/go-mysql-server)
0.20 over the KV store described in the design doc; this page lists where
the combination differs from MySQL and how each item was checked.

How it is checked:

- **Driver matrix** (`scripts/drivers`, CI job `minio`): go-sql-driver/mysql,
  PyMySQL, mysql2 and Connector/J run the same scenario against `sqld` and
  through `proxyd`. All four pass.
- **sqllogictest** (`make slt`, CI job `sqllogictest`): a subset of the
  SQLite-derived suite; results per file in
  [`scripts/slt/results.md`](../scripts/slt/results.md).
- **MySQL's own test suite** (`make mtr`, CI job `mysql-test-suite`):
  133 files of MySQL 8.0.46's `mysql-test` run by a Rust mysqltest
  implementation (`mtr/`), each statement's output compared with MySQL's
  recorded result; results per file in
  [`scripts/mtr/results.md`](../scripts/mtr/results.md).
- **MySQL 8 differential tests** (`internal/sqlengine/mysqldiff_test.go`):
  seeded random tables and queries run on a real MySQL 8.0 and on this
  engine, results compared: collation order, equality, index ranges, LIKE and
  unique conflicts for each supported collation; round trips, order and
  ranges for each scalar type, in secondary indexes and primary keys. Run
  with `S2Z_MYSQL_DSN='root@tcp(127.0.0.1:3306)/' go test ./internal/sqlengine
  -run MySQLDiff -diff-rounds 20`; `-diff-known-gaps` also generates the
  inputs of the gaps listed under *Types and collations*.
- **ORM suite** (`scripts/orm`, CI job `minio`): a SQLAlchemy 2.0 +
  Alembic app (migrations up/down/up, autogenerate finding no changes,
  reflection, ORM CRUD, relationships, upserts, transactions, row locks)
  passes 16/16 against `sqld`, through `proxyd`, and on MySQL 8.0.
- **Go tests** in `internal/sqlengine` and `internal/proxy` for each item
  below that says "tested".

## Connecting

| | |
|---|---|
| Wire protocol | MySQL client/server protocol; server version `8.0.33-s2z` at the proxy. |
| Authentication | At `proxyd` only: `caching_sha2_password` (fast path and full) and `mysql_native_password`, against the tenants file. `sqld` and `kvd` trust their network. |
| Tenant selection | `user@tenant`, or a bare user with TLS SNI `<tenant>.<domain>`. |
| TLS | At `proxyd`; `-require-tls` refuses plaintext. |
| Prepared statements | Binary protocol supported (tested with Go, mysql2 `execute()`, Connector/J `useServerPrepStmts=true`). The proxy keeps statements and runs them as text on the query engine, so a backend restart needs no re-prepare. |
| Pings, `SELECT 1` | Answered by the proxy; they never wake compute. |
| Results | Buffered whole at the proxy before sending (no row streaming); very large results cost proxy memory. |
| OK packets | Info strings (`Rows matched: 2  Changed: 2  Warnings: 0`, `Statement prepared`) as MySQL writes them, and warning counts, through the proxy too. go-mysql-server writes no info for `INSERT`. |
| Multi-statements | Supported (`CLIENT_MULTI_STATEMENTS`). |
| Autocommit | The handshake announces autocommit on, as MySQL's does, so drivers that switch it off (PyMySQL, SQLAlchemy) send `SET autocommit = 0` and get implicit transactions. |

## SQL surface

Supported and tested: `CREATE/DROP DATABASE`, `CREATE/DROP/RENAME TABLE`,
`CREATE TABLE ... LIKE`, `CREATE TABLE ... AS SELECT`, `TRUNCATE`,
`CREATE/DROP INDEX` (online, unique and non-unique, prefix indexes),
`CREATE/DROP VIEW` (stored in the catalog, visible to every session and query
engine), `INSERT ... ON DUPLICATE KEY UPDATE`, `REPLACE`, multi-row
`INSERT`, `UPDATE`/`DELETE` with `LIMIT`, joins, subqueries,
`UNION`/`EXCEPT`, CTEs including `WITH RECURSIVE`, window
functions, JSON columns and functions, generated columns, `ENUM`/`SET`,
`SHOW CREATE TABLE`, `SHOW [FULL] TABLES`, `information_schema` tables and
columns, `ANALYZE TABLE`, `GET_LOCK`/`RELEASE_LOCK`, `AUTO_INCREMENT`,
tables without a primary key (hidden rowid), column defaults that are
expressions (`DEFAULT CURRENT_TIMESTAMP`, `now()`, `(expr)`), `ON UPDATE
CURRENT_TIMESTAMP`, stored and virtual generated columns, and foreign keys:
declared in `CREATE TABLE` or `ALTER TABLE ... ADD/DROP FOREIGN KEY`,
enforced (1452 on an orphan child, 1451 on a referenced parent), with
`ON DELETE`/`ON UPDATE` `CASCADE`, `SET NULL` and `RESTRICT`; a reference
check takes a shared lock on the parent row, as InnoDB does, so a concurrent
parent delete waits for the child's transaction.

Not supported in v1 (statement fails with an error):

| feature | error |
|---|---|
| Triggers, stored procedures, events | `database "app" doesn't support triggers` (and similar) |
| CHECK constraints | `the table does not support check constraint operations` |
| Savepoints | 1235 `savepoints are not supported in v1` |
| FULLTEXT indexes | refused by go-mysql-server (its error text is garbled: `tables cannot be created on database %!s(MISSING)`) |
| Spatial types | `spatial types are not supported in v1` |
| `CREATE USER`, `GRANT` | `database not found: mysql`; accounts live in the proxy's tenants file |
| `SELECT ... FOR SHARE` | syntax error; use `LOCK IN SHARE MODE` |
| `LOAD DATA` | not supported |
| Files on the server: `SELECT ... INTO OUTFILE` / `DUMPFILE`, `LOAD DATA INFILE`, `LOAD_FILE()` | 1290, as MySQL with `secure_file_priv = NULL`: a query engine serves many tenants |
| `CAST(x AS BINARY(n))` with `n` over `max_allowed_packet` | NULL with warning 1301, as in MySQL (go-mysql-server allocated `n` bytes) |
| SERIALIZABLE | `SET ... ISOLATION LEVEL SERIALIZABLE` is accepted; a statement that reads or writes data at that level fails, while `COMMIT`, `ROLLBACK` and `SET` (to change the level back) work |

Accepted but different:

- Temporary tables live in the query engine's memory, in the session that
  created them, and are never written to storage. They shadow a permanent
  table of the same name; a failed statement and `ROLLBACK` undo their rows,
  as with InnoDB temporary tables; `CREATE`, `DROP` and `ALTER` are not
  transactional. The proxy keeps a session that created one on its query
  engine; a reconnect loses them, as in MySQL. Reads scan the rows (their
  indexes enforce uniqueness but don't speed up lookups), and their size is
  bounded only by the query engine's memory.

- `PARTITION BY` clauses are parsed and ignored (a tenant is one KV
  partition already).
- `information_schema.views` is empty (go-mysql-server filters it by
  privileges, which are not used); `SHOW FULL TABLES` lists views.
- Plain `EXPLAIN` returns go-mysql-server's MySQL-shaped rows, which some
  drivers fail to decode (PyMySQL reads `NULL` text in an integer column);
  `EXPLAIN FORMAT=TREE` returns the plan as text.
- `DESC` in an index definition is accepted; entries are stored ascending.
  Results are correct, but a descending scan of such an index is not cheaper.
- A prefix `LIKE 'abc%'` is evaluated as a filter, not an index range
  (go-mysql-server's range rewrite drops strings that continue past U+00FF,
  so it is disabled).
- Performance shape: every table and index read is a KV call (two for a
  secondary index: entries, then rows). Repeated non-locking reads of the
  same key ranges within one statement, as in the inner side of a nested
  join, are served from a per-statement read cache. Joins are planned by
  go-mysql-server; a many-table join without useful statistics can be planned
  as a large cross product (see `scripts/slt/README.md`).
- Warnings: `SHOW WARNINGS` lists them in the order raised, as MySQL does.
  `CREATE`/`ALTER TABLE` give MySQL's deprecation warnings (1681: integer
  display widths, `ZEROFILL`, `FLOAT(M,D)`, `UNSIGNED` decimals and floats),
  1831 for a duplicate index and note 1050 for `CREATE TABLE IF NOT EXISTS`
  on an existing table. Other warnings MySQL gives are often missing
  (issue #28).
- Error codes: go-mysql-server's generic 1105 errors get MySQL's code where
  the message identifies it (syntax error 1064, unknown column 1054, table
  exists 1050, no database 1046, ambiguous column 1052, value count 1136,
  invalid default 1067, unknown function 1305). Duplicate key 1062, lock wait
  timeout 1205, deadlock and schema-changed 1213, unknown table 1146, unknown
  database 1049; most other errors are 1105 with go-mysql-server's message.

## Transactions and locking

- REPEATABLE READ (default) and READ COMMITTED, as snapshot isolation;
  READ UNCOMMITTED runs as READ COMMITTED.
- Row locks: `SELECT ... FOR UPDATE`, `LOCK IN SHARE MODE`, and the rows an
  `UPDATE`/`DELETE` reads. Waits are FIFO, `innodb_lock_wait_timeout`
  (session, default 50 s) applies, deadlocks are detected and the youngest
  transaction is the victim (1213).
- No gap or next-key locks: a range `FOR UPDATE` doesn't block inserts into
  the range. Use a unique key for check-then-insert.
- Locking reads lock every row the key range returns, before filters on
  non-key columns (as InnoDB does when no index matches).
- After a 1213 the transaction is rolled back; further statements return
  1213 until the client sends `COMMIT` or `ROLLBACK`.
- Limits: rows up to 1 MiB, up to 64 MiB of writes per transaction.

## Schema changes

- DDL commits on its own, as in MySQL.
- `ADD INDEX` is online: the index is written by every writer while it is
  backfilled, then becomes visible. `DROP INDEX` takes two catalog leases
  (about 4 s).
- `ALTER TABLE` covers changes that need no table rewrite: ADD/DROP COLUMN
  (not primary-key or indexed columns), RENAME/MOVE COLUMN, defaults,
  dropping NOT NULL, widening integers and string lengths. Changing the
  primary key or another column type is refused.
- A write that raced a schema change made on another query engine fails with
  1213 and succeeds when retried; reads see a schema at most one lease (2 s)
  old.
- `DROP TABLE`, `DROP DATABASE` and `TRUNCATE` drop catalog rows (TRUNCATE
  gives the table a new id); the old rows stay in storage until a cleanup job
  exists. `TRUNCATE` restarts `AUTO_INCREMENT`.

## Types and collations

- All MySQL scalar types except spatial. DECIMAL keys are exact
  (memcomparable encoding); DATETIME/TIMESTAMP to microseconds.
- Collations: `utf8mb4_0900_ai_ci` (the default, as in MySQL 8),
  `utf8mb4_general_ci`, `utf8mb4_0900_bin`, `utf8mb4_bin`, `binary`. Others are
  refused.
- `CHAR(n)` drops trailing spaces on store, as MySQL does.
- Known gaps, found by the differential tests and inherited from
  go-mysql-server's comparisons (keys follow the same rules, so results are
  consistent, but they differ from MySQL):
  - Collation expansions are not applied: under `utf8mb4_0900_ai_ci`, `ß` and
    `ẞ` do not sort and compare as `ss`, `æ` as `ae`, or ligatures such as `ﬁ`
    as their letters; each is one character with its own weight.
  - PAD SPACE: `utf8mb4_bin` and `utf8mb4_general_ci` should ignore trailing
    spaces (`'a ' = 'a'`). Unique and primary keys do (a duplicate-key error,
    as in MySQL), but comparisons, `GROUP BY` and `DISTINCT` treat the strings
    as different.
  - An `ENUM` compared with a string compares by member index
    (`e < 'b'` means "declared before b"); MySQL compares the strings. `ORDER
    BY` on an ENUM is by index in both.
  - A `BIGINT UNSIGNED` compared with a string literal goes through float64,
    so values within about 2^11 of 2^64 compare equal; integer literals
    compare exactly. Integer `+`, `-` and `*` follow MySQL: unsigned if
    either operand is, 1690 when out of range; `CAST(... AS SIGNED /
    UNSIGNED)` of a string converts as MySQL does, with its warnings.
  - `ENUM ... LIKE` is always false.
  - A multi-table `UPDATE` (`UPDATE a JOIN b ... SET a.v = 'B'`) still skips
    a row whose new value equals the old one under the collation (`b` →
    `B`); a single-table `UPDATE` stores the new bytes and counts the row,
    as MySQL does.
  - Output formatting: `DATETIME(6)` prints without trailing
    zeros in the fraction (`.12`, MySQL `.120000`); `FLOAT` prints every
    digit of the float32 (`-565.3059`, MySQL `-565.306`).
- `AUTO_INCREMENT` within a multi-row insert comes from doubling
  reservations, so bulk inserts can leave gaps (like
  `innodb_autoinc_lock_mode=2`).

## Operations

- Scale to zero: a tenant's KV owner suspends after `-idle-suspend` and the
  next query wakes it; a query that arrives during a wake waits for it.
  Query engines behind the proxy scale with sessions (`-warm`, `-engine-idle`).
- Point-in-time restore: `kvd restore -src N -dst M -at TIME` writes a new
  partition as the old one was at TIME, within the restore window (1 h by
  default); point the tenant at the new partition.
- Metrics: `-metrics-listen` on every binary serves Prometheus text format
  (wakes by reason, cold-start latency, commit and takeover latency).
