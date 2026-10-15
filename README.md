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
| 5 | Indexes, types, collations | mostly — secondary indexes (non-unique, unique, the NULL rule, unique-key locks, prefix `col(N)`, CREATE/DROP/RENAME INDEX with backfill), AUTO_INCREMENT counters in system table 3, every type round-trips, memcomparable keys incl. DECIMAL and collation weights. Not yet: hidden `_rowid`, DESC index columns, the MySQL 8 property-test harness, the ORM suite |
| 6 | KV scale-to-zero | done — directory service (`dird`, SQLite partition table, node heartbeats), idle suspend (checkpoint → release → exit), wake on demand by spawning a per-tenant `kvd` or placing on a pooled node, query engines route through the directory and re-resolve on EpochChanged, local SST cache across restarts; wake time measured below |
| 7–9 | Proxy, schema changes, multi-tenant | not started |

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
| `cmd/wakedemo`, `cmd/waketime` | the slice-6 sleep/wake demo and wake-time benchmark |

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

## Tests

```sh
make test                 # unit + in-process SQL tests (MinIO tests skip)
make test-minio           # also runs the MinIO tests (MINIO_ENDPOINT, default 127.0.0.1:9000)
make fault-1000           # slice 2 bar: 1,000 seeded fault-injection runs
make crash-test           # slice 1: write loop, kill -9 kvd 5 times, verify every acknowledged row
make wake-demo            # slice 6: tenant kvd exits when idle, next query wakes it
```

Replay a failing fault seed with `go test ./internal/kv -run TestFaultInjection -fault-seed=N`.
Fault decisions derive from the seed, operation, key and attempt, so they
replay even when goroutines interleave differently.

## Differences from MySQL in this version

- Every table needs a PRIMARY KEY (the hidden `_rowid` is still to come).
- CREATE INDEX backfills inside one transaction (so tables up to the 64 MiB cap), and is not yet
  safe against concurrent writes from other sessions (online schema change is slice 8).
- DROP INDEX leaves the old entries behind (index ids are never reused).
- AUTO_INCREMENT values within a multi-row INSERT come from doubling reservations, so a bulk
  insert can leave gaps (as with `innodb_autoinc_lock_mode=2`).
- No gap or next-key locks: a range `FOR UPDATE` doesn't block inserts into the range, so
  check-then-insert code must rely on a unique key. Locking reads lock every row the key range
  returns, before WHERE filters on non-key columns (as InnoDB does without an index).
- `SELECT ... FOR SHARE` doesn't parse yet; use `LOCK IN SHARE MODE`.
- After a deadlock (1213) the transaction is rolled back on the owner; further statements in it
  return 1213 until the client sends COMMIT or ROLLBACK.
- SERIALIZABLE is refused; READ UNCOMMITTED runs as READ COMMITTED.
- Savepoints are not supported.
- Collations: only `utf8mb4_0900_ai_ci`, `utf8mb4_general_ci`, `utf8mb4_0900_bin`, `utf8mb4_bin`
  and `binary`. Key weights for `_ci` collations come from go-mysql-server's collation sorter;
  checking them against MySQL's `WEIGHT_STRING()` is still on the verify-before-coding list.
- Rows up to 1 MiB, transactions up to 64 MiB of intents.
- DROP TABLE / DROP DATABASE remove catalog rows only; the data is left behind
  (ids are never reused) until a background cleanup exists.
- One query engine per tenant: the catalog cache is not shared across engines until slice 8.
- No authentication beyond go-mysql-server's defaults until the proxy (slice 7).
- A query arriving while its tenant is waking waits for the wake. A transaction whose owner goes
  away after it has done something (owner crash) is aborted with 1213; idle suspend never cuts
  one off, because a partition with an open transaction is not suspended.
