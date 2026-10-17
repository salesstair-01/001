# Scale-to-zero MySQL — v1

A MySQL-compatible database where each tenant is one partition, owned by
exactly one KV process at a time, and a commit is acknowledged only once it is
durable in object storage. v1 targets **MinIO only** as the object store.

The design lives in the doc *Scale-to-Zero MySQL DB — v1 Foundation Decisions*
(decisions D1–D7) and its *Build slices* tab. This repo builds the slices in order.

## Status

| # | Slice | State |
|---|-------|-------|
| 0 | Walking skeleton | done — `mysql` client → `sqld` → gRPC → `kvd` |
| 1 | Durable autocommit on object storage | done — Pebble with its WAL off, object-store WAL, group commit; `scripts/crash-test.sh` passes |
| 2 | Fencing, takeover and checkpoints | done — numbered manifests and WAL segments with put-if-absent, compare-on-412, checkpoints + GC, txn ids; fault tests pass 1,000 seeded runs |
| 3 | Transactions and MVCC | done — snapshot reads, statement layers, read-your-writes, RR/RC, SERIALIZABLE refused, MVCC GC, 64 MiB cap |
| 4 | Locks | done — X/S row locks in owner memory, locking reads of the latest version for UPDATE/DELETE/`FOR UPDATE`/`LOCK IN SHARE MODE`, FIFO wait queue with `innodb_lock_wait_timeout` (1205), wait-for-graph deadlock detection with the youngest victim (1213), 2 s heartbeat / 10 s lease; the slice-3 stopgap is gone |
| 5 | Indexes, types, collations | mostly — secondary indexes (non-unique, unique, the NULL rule, unique-key locks, prefix `col(N)`, CREATE/DROP/RENAME INDEX with backfill), AUTO_INCREMENT counters in system table 3, every type round-trips, memcomparable keys incl. DECIMAL and collation weights. MySQL 8 differential property tests (`internal/sqlengine/mysqldiff_test.go`; they found and fixed four bugs, the rest are listed as known gaps in docs/compatibility.md). ORM suite: SQLAlchemy 2.0 + Alembic, 16/16 on `sqld`, through `proxyd` and on MySQL 8 (`scripts/orm`; it found and fixed expression defaults lost on reload, foreign keys — now stored and enforced — and the handshake's missing autocommit flag). Not yet: DESC index columns (accepted, stored ascending) |
| 6 | KV scale-to-zero | done — directory service (`dird`, SQLite partition table, node heartbeats), idle suspend (checkpoint → release → exit), wake on demand by spawning a per-tenant `kvd` or placing on a pooled node, query engines route through the directory and re-resolve on EpochChanged, local SST cache across restarts; wake time measured below |
| 7 | Proxy and query-engine scale-to-zero | done — `proxyd`: TLS, `caching_sha2_password` (and native) auth, tenant from `user@tenant` or the TLS hostname `<tenant>.<domain>`; holds client sockets while compute starts; pings and `SELECT 1` answered locally; `USE`/`SET` tracked and replayed onto a new query engine; prepared statements kept at the proxy, so nothing needs re-preparing; transactions, temp tables, `GET_LOCK`, `LOCK TABLES` pin the session; idle backends released; generic `sqld -generic` engines started on demand with a pre-warmed pool; cold starts measured below |
| 8 | Schema changes across query engines | done — per-table schema versions guard every writing commit, so no write commits with a stale schema; catalog caches refresh under a 2 s lease; index builds go write-only → backfill (locking batches, progress in a catalog job row, resumed by any engine after a KV or engine restart) → public; index drops go write-only and wait out the lease; instant ADD/DROP COLUMN with original defaults; compatible MODIFY/RENAME COLUMN; row counts in system table 2 from ANALYZE and a background auto-analyze that never keeps a tenant awake |
| 9 | Multi-tenant and compatibility hardening | done — tables without a primary key (hidden rowid); per-partition Pebble limits (shared block cache, 4 MiB memtables, one compaction) and fair PUTs (node-wide PUT slots, per-tenant token bucket); 100 tenants on one KV node and one query engine through the proxy with isolation checks; Prometheus metrics on every binary; point-in-time restore into a new partition (`s2zctl`); persistent views and `TRUNCATE`; sqllogictest subset: 139,122 of 140,709 records pass, none wrong (`scripts/slt/results.md`); driver matrix (Go, PyMySQL, mysql2, Connector/J) passing direct and through the proxy; GitHub Actions CI; [docs/compatibility.md](docs/compatibility.md) |

## Layout

| Path | What |
|------|------|
| `internal/encoding` | the shared key encoding (D2) and row format v2 (D3) |
| `internal/objstore` | object-store client: MinIO, in-memory, seeded fault injection |
| `internal/wal` | WAL segment format with CRC-32C |
| `internal/kv` | the KV owner: takeover, group commit, fencing, checkpoints, transactions, MVCC GC |
| `internal/kvrpc` | gRPC API (gob codec, no codegen) and error mapping |
| `internal/sqlengine` | go-mysql-server provider, catalog in system table 1, tables, PK index, `TransactionSession` |
| `internal/directory` | the directory service, its client, the engine's `Router`, and the node `Agent` |
| `cmd/kvd`, `cmd/sqld`, `cmd/dird` | the KV owner, query engine and directory binaries |
| `cmd/crashtest` | the slice-1 crash script driver |
| `internal/proxy` | the MySQL proxy: auth, session tracking and replay, the query-engine pool |
| `cmd/proxyd` | the proxy binary |
| `cmd/wakedemo`, `cmd/waketime` | the slice-6 sleep/wake demo and wake-time benchmark |
| `cmd/coldstart` | the slice-7 cold-start benchmark (every layer at zero, pooled app) |
| `internal/metrics` | dependency-free Prometheus text registry (`-metrics-listen` on every binary) |
| `cmd/s2zctl` | operator tool: restore points and point-in-time restore |
| `cmd/slt`, `scripts/slt` | sqllogictest runner, the pinned file list and recorded results |
| `scripts/drivers` | the driver matrix (Go, Python, Node, Java) |
| `docs/compatibility.md` | what differs from MySQL 8.0, and how it is checked |
| `.github/workflows/ci.yml` | CI: race tests, 1,000 fault seeds, MinIO + crash test + drivers, sqllogictest |

Object layout per partition: `p/<id>/manifest/<N>`, `p/<id>/wal/<seq>`,
`p/<id>/sst/<uuid>.sst`, `p/<id>/meta/<checkpoint>/<pebble file>`.

## Run it

```sh
docker compose up -d minio            # or any MinIO on 127.0.0.1:9000 (minioadmin/minioadmin)
make build
./bin/kvd  -listen 127.0.0.1:7070 -data ./data/kv
./bin/sqld -listen 127.0.0.1:3306 -kv 127.0.0.1:7070 -partition 1
mysql -h 127.0.0.1 -P 3306 -u root app
```

Takeover by hand (slice 2): start a second `kvd` on another port with
`-partition 1`; the first is fenced on its next WAL write and stops serving.

### Scale to zero (slice 6)

```sh
./bin/dird -listen 127.0.0.1:7000 -db ./data/directory.db \
  -spawn-kvd ./bin/kvd -spawn-data ./data/kv -spawn-args "-idle-suspend 5m"
./bin/sqld -listen 127.0.0.1:3306 -directory 127.0.0.1:7000 -partition 1
```

`sqld` asks the directory for the tenant's owner. If none is running, `dird`
starts a dedicated `kvd`, which takes the partition over and registers.
After 5 idle minutes (no open transactions, no requests) the `kvd` writes a
checkpoint, releases the partition and exits. The next query wakes a new
one. Without `-spawn-kvd`, `dird` places partitions on `kvd -directory ...`
nodes that registered themselves (pool mode); a node that stops answering
is replaced on the next query.

`./bin/wakedemo -idle 3s` runs that loop with real processes.

### Proxy and query-engine scale-to-zero (slice 7)

```sh
echo '[{"name":"acme","partition":42,"users":{"app":"s3cret"}}]' > tenants.json
./bin/dird   -listen 127.0.0.1:7000 -spawn-kvd ./bin/kvd -spawn-args "-idle-suspend 5m"
./bin/proxyd -listen 127.0.0.1:3306 -tenants tenants.json -domain db.example.com \
  -spawn-sqld ./bin/sqld -directory 127.0.0.1:7000 -warm 1
mysql -h 127.0.0.1 -P 3306 -u app@acme -ps3cret
```

Apps connect only to `proxyd`. A session gets a connection to a generic
query engine (`sqld -generic`, any tenant, picked by the backend user
`s2z_p<partition>`) when it first runs a statement that needs one. The
connection is released after `-backend-idle` (30 s) unless the session is in
a transaction or pinned. Query engines with no sessions stop after
`-engine-idle` (5 min), beyond the `-warm` ones kept ready.

Cold start of the first query from an app's open connection pool, with every
KV owner (and with `-warm 0`, every query engine) at zero
(`./bin/coldstart -rounds 20 -warm N`; real processes, local MinIO, small tenant):

| query engines kept warm | p50 | p99 |
|---|---|---|
| 0 (every compute layer at zero) | 416 ms | 455 ms |
| 1 | 44 ms | 55 ms |

With no warm engine, most of the time is starting a `sqld` process; the KV
wake is the rest (see the wake-time table above for larger tenants).

Wake time (`./bin/waketime -sizes 100,1024,5120`; local MinIO, rows of 1 KiB
about 2:1 compressible). Cold means every SST is downloaded; warm means the
host's SST cache still has them, as when a tenant wakes on the host it last ran on:

| tenant size | files | cold wake | warm wake (local cache) | first read after wake |
|---|---|---|---|---|
| 100 MB | 44 | 273 ms | 20 ms | 0.1 ms |
| 1 GB | 390 | 2.4 s | 36 ms | 11.6 ms |
| 5 GB | 1,927 | 7.3 s | 85 ms | 0.2 ms |

Wake is the takeover alone (manifest, downloads, Pebble open, WAL replay of
the fence). Through the whole stack, `wakedemo` sees about 60–100 ms from
query to rows for a small tenant, including starting the `kvd` process.

Cold wake grows linearly with tenant size because takeover downloads every
SST before serving (the known v1 limit in D4); reading SSTs on demand from
object storage is the later fix.

### Many tenants per node, metrics and restore (slice 9)

Pool mode (`dird` without `-spawn-kvd`, or `-dedicated=false`) puts many
tenants on one `kvd`. Each partition gets a 4 MiB memtable and one compaction
at a time, all share one block cache (`-block-cache-mb`, 256), and object-store
PUTs take a node-wide slot (`-put-concurrency`, 32) and a token from the
tenant's bucket (`-put-rate`/`-put-burst`, 100/s and 200). A tenant over its
rate waits; its commits meanwhile batch into larger group commits, so it pays
in latency and the others keep their PUTs. Behind the proxy, one query engine
takes up to `-engine-sessions` (256) backend connections before another starts.

Every binary takes `-metrics-listen 127.0.0.1:9100` and serves `/metrics`:
`s2z_kv_commit_seconds`, `s2z_kv_puts_total{partition}`,
`s2z_kv_takeover_seconds{cache}`, `s2z_dir_wakes_total{reason}` (first_use,
suspended, node_lost), `s2z_dir_wake_seconds`, `s2z_proxy_cold_start_seconds{engine}`,
`s2z_proxy_queries_total{tenant}` and session/engine gauges.

Point-in-time restore writes a new partition as an old one was at a time
within the GC grace (1 h):

```sh
./bin/s2zctl restore-points -partition 42
./bin/s2zctl restore -src 42 -dst 1042 -at 2026-10-16T09:30:00Z
# check it, then point the tenant at partition 1042 in the tenants file
```

## Tests

```sh
make test                 # unit + in-process SQL tests (MinIO tests skip)
make test-minio           # also runs the MinIO tests (MINIO_ENDPOINT, default 127.0.0.1:9000)
make fault-1000           # slice 2 bar: 1,000 seeded fault-injection runs
make crash-test           # slice 1: write loop, kill -9 kvd 5 times, verify every acknowledged row
make wake-demo            # slice 6: tenant kvd exits when idle, next query wakes it
make cold-start           # slice 7: pooled app through proxyd, p50/p99 from all-zero
make slt                  # sqllogictest subset, writes scripts/slt/results.md
make drivers              # driver matrix against DRV_HOST:DRV_PORT (scripts/drivers/README.md)
make orm                  # SQLAlchemy + Alembic suite against ORM_HOST:ORM_PORT (scripts/orm/README.md)
S2Z_MYSQL_DSN=... go test ./internal/sqlengine -run MySQLDiff   # differential tests against a MySQL 8
```

CI (`.github/workflows/ci.yml`) runs the race tests, 1,000 fault seeds, the
MinIO tests, the crash test, the driver matrix against `sqld` and `proxyd`,
and the sqllogictest subset, which fails below `scripts/slt/min-pass-rate`.

Replay a failing fault seed with `go test ./internal/kv -run TestFaultInjection -fault-seed=N`.
Fault decisions derive from the seed, operation, key and attempt, so they
replay even when goroutines interleave differently.

## Differences from MySQL in this version

The full list, with how each item is checked, is in
[docs/compatibility.md](docs/compatibility.md). The ones most likely to matter:

- No triggers, stored procedures, CHECK constraints, temporary
  tables, savepoints, FULLTEXT or spatial types.
- No gap or next-key locks: a range `FOR UPDATE` doesn't block inserts into
  the range; use a unique key for check-then-insert.
- `ALTER TABLE` only for changes that need no rewrite; changing the primary
  key or a column's type otherwise is refused.
- A write racing a schema change on another query engine fails with 1213 and
  succeeds when retried.
- SERIALIZABLE is refused; `SELECT ... FOR SHARE` doesn't parse (use
  `LOCK IN SHARE MODE`).
- Rows up to 1 MiB, transactions up to 64 MiB of writes.
- `DROP TABLE`/`DROP DATABASE`/`TRUNCATE` leave the old rows in storage until a
  cleanup job exists.
- Authentication is at `proxyd`; `sqld` and `kvd` must not be exposed.
