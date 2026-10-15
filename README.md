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
| 5 | Indexes, types, collations | partly — every type round-trips, memcomparable keys incl. DECIMAL and collation weights; no secondary indexes, AUTO_INCREMENT or `_rowid` yet |
| 6–9 | Scale-to-zero, proxy, schema changes, multi-tenant | not started (suspend/wake primitives exist in `kv`) |

## Layout

| Path | What |
|------|------|
| `internal/encoding` | the shared key encoding (D2) and row format v2 (D3) |
| `internal/objstore` | object-store client: MinIO, in-memory, seeded fault injection |
| `internal/wal` | WAL segment format with CRC-32C |
| `internal/kv` | the KV owner: takeover, group commit, fencing, checkpoints, transactions, MVCC GC |
| `internal/kvrpc` | gRPC API (gob codec, no codegen) and error mapping |
| `internal/sqlengine` | go-mysql-server provider, catalog in system table 1, tables, PK index, `TransactionSession` |
| `cmd/kvd`, `cmd/sqld` | the KV owner and query engine binaries |
| `cmd/crashtest` | the slice-1 crash script driver |

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

## Tests

```sh
make test                 # unit + in-process SQL tests (MinIO tests skip)
make test-minio           # also runs the MinIO tests (MINIO_ENDPOINT, default 127.0.0.1:9000)
make fault-1000           # slice 2 bar: 1,000 seeded fault-injection runs
make crash-test           # slice 1: write loop, kill -9 kvd 5 times, verify every acknowledged row
```

Replay a failing fault seed with `go test ./internal/kv -run TestFaultInjection -fault-seed=N`.
Fault decisions derive from the seed, operation, key and attempt, so they
replay even when goroutines interleave differently.

## Differences from MySQL in this version

- Every table needs a PRIMARY KEY; AUTO_INCREMENT and secondary indexes are not supported yet (slice 5).
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
