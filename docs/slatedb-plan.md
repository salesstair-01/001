# Plan: the KV owner on SlateDB, in Rust, on Ceph RGW

Decided after the SlateDB spike (`spikes/slatedb`) and the Ceph tests
(`docs/object-stores.md`):

- **The KV owner (`kvd`) is rewritten in Rust on SlateDB.** SlateDB takes
  over what the Go owner built itself: the object-store WAL, group commit,
  manifests and fencing, checkpoints, SST upload and download, the file
  cache, the remote-SST adapter, object GC, and Pebble.
- **The SQL engine (`sqld`, go-mysql-server), the proxy and the directory
  stay in Go.** `sqld` already talks to `kvd` over gRPC, which is the seam.
- **Ceph RGW is the object store:** MicroCeph for development and CI, a
  full cluster for release testing.

The Rust owner reached parity and replaced the Go owner (phase 6).

## Design decisions

1. **Protobuf replaces gob** on the KV and directory RPCs, so Go and Rust
   can talk. The message shapes stay as they are; errors keep their
   current encoding (`codes.Aborted` with `kind|hexkey|msg`), so
   `kvrpc.FromStatus` works for both owners. There is no `protoc`
   dependency: `buf` generates the Go code, and `protox` with `tonic`
   builds the Rust side.
2. **One SlateDB database per partition**, at `p/<id>/` in the bucket.
3. **Takeover and fencing.**
   - Opening a partition's database fences the previous writer: it is
     SlateDB's writer epoch, checked when the manifest is written.
   - The owner then reads its own `epoch` key and durably writes
     `epoch + 1`, which is the epoch the query engines see.
   - A fenced owner's next durable write fails ("detected newer DB
     client"). It then stops serving, and its clients get `EpochChanged`,
     exactly as today.
4. **MVCC is ported unchanged.** It is the same as `internal/kv`, just
   written in Rust:
   - keys are the user key plus an inverted `commit_ts`;
   - row locks and intents live in memory, with statement rollback, RR and
     RC snapshots, and transaction leases;
   - committed transaction IDs are stored, so a commit whose response was
     lost is recognised;
   - MVCC GC is done by deleting old versions;
   - AUTO_INCREMENT works as today.

   SlateDB is used as an ordered key-value store. A group commit is one
   `WriteBatch` followed by `AwaitDurable`; versions become visible
   (`visibleTS`) only once durable. This keeps the semantics the test
   suites already pin down. SlateDB's own snapshots and transactions are an
   option for later; there is an open question about how its snapshots
   interact with durability filtering.
5. **Scale to zero.** Suspend is SlateDB's shutdown with a memtable flush;
   wake is opening the database, about 90 ms in the spike. Lazy reads
   through SlateDB's cache replace downloading SSTs.
6. **Configuration.** S3 endpoint and credentials come from the same
   `MINIO_*` variables and `-minio-*` flags (to be renamed `s3` later). The
   flush interval is 10 ms, matching the Go owner's group-commit window.

## Phases

| # | Phase | Done when |
|---|---|---|
| 1 | **Protobuf RPC.** `proto/kv.proto` and `proto/directory.proto`; the Go `kvrpc` and directory moved from gob to the generated code | all existing suites green, behaviour unchanged |
| 2 | **Rust `kvd`, single partition.** `kvd-rs/`: tonic server, SlateDB on S3/RGW, takeover and fencing, Begin/Get/Scan/Mutate/RollbackStatement/Commit/Rollback/TxnStatus/AutoIncrement/Heartbeat/Ping/Open, locks and leases, MVCC GC | `sqld` runs against it; smoke tests pass |
| 3 | **Suites against both owners.** The KV test suites (metamorphic, Pebble compatibility, lock and transaction tests) run over RPC against any `kvd` binary; `crash-test.sh`, the drivers, ORM, sqllogictest and the MySQL diff run with the Rust `kvd` in CI on MicroCeph | the same results as the Go owner |
| 4 | **Node features.** Directory registration and heartbeats, `-auto-open`, `-exit-when-idle`, being spawned by `dird`, idle suspend, Prometheus metrics, per-tenant memory limits (memtable and unflushed bytes) | the scale-to-zero demo and the proxy suites pass |
| 5 | **Restore and operations.** Point-in-time restore on SlateDB checkpoints and clones, `s2zctl` | the restore tests pass |
| 6 | **Switch.** The Rust `kvd` becomes the default; the Go owner's storage layer is removed; docs updated | CI on the Rust owner only |

## Status

- **Phase 1** is done: the Go code speaks protobuf (`proto/s2z`).
- **Phase 2** is done for a single partition: `kvd-rs/` (see its README).
  On Ceph RGW, `sqld` runs against it and passes the crash test, the
  driver matrix, the ORM suite and a fencing check; CI runs these in the
  `kvd-rs` job on MicroCeph.
- **Phase 3** is under way. The transaction, lock and metamorphic suites
  moved to `internal/kvsuite` and run against any owner: the Go owner in
  process (`go test ./internal/kv`), or a `kvd` binary of either
  implementation over gRPC (`make kvsuite`). Both binaries pass 40
  metamorphic seeds of 500 operations each on Ceph RGW. Running the Go
  owner over gRPC found a bug that predates the Rust work: an empty value
  (a secondary-index entry) written in a transaction read back as absent
  until commit, because the wire turns it into nil; fixed in `kvrpc`.
  The MySQL differential tests run against an external `kvd` too
  (`S2Z_KVD_ADDR`), and pass on the Rust owner; CI runs them there, with
  the sqllogictest files that pass in full on the Go owner (`select1`–`3`,
  `evidence/*`), which must pass in full on the Rust owner too. The other
  sqllogictest files gave the same counts by hand. Not ported: the Pebble
  compatibility test, which checks the Go owner's Pebble format.
- **Phase 4** is done except for per-partition PUT accounting: directory
  registration and heartbeats, idle suspend with release, exit-when-idle
  (so `dird -spawn-kvd` works with the Rust binary), a shared block cache
  and per-partition memory limits, and Prometheus metrics. The
  scale-to-zero demo and the driver and ORM suites through `proxyd` pass
  on the Rust owner. A wake takes about 170 ms against 80–95 ms for the Go
  owner: SlateDB's open is 70–85 ms of it.
- **Phase 5**: point-in-time restore is done (`kvd restore`,
  `kvd restore-points`), on the MVCC history kept for a restore window
  rather than on SlateDB checkpoints and WAL replay: it reads the source
  through a checkpointed SlateDB reader and needs no replay. Not done: a
  restore to a time before the first commit still in the window fails
  rather than restoring an empty partition, and `s2zctl` does not drive
  it yet.

- **Phase 6** is done: `kvd` is the Rust binary (`make build` puts it in
  `bin/`). The Go owner (`internal/kv`'s Pebble storage, WAL, checkpoints,
  manifests; `internal/wal`; Go `cmd/kvd`; `cmd/remotebench`, `cmd/s2zctl`,
  `cmd/waketime`) is removed; `internal/kv` keeps only the API's types. Its
  tests were ported to Rust (`kvd-rs/tests`: transactions and locks,
  durability and faults, the metamorphic test, node and restore). The Go
  tests that ran an in-process Go owner (`sqlengine`, `proxy`, `directory`,
  `cmd/slt`) start the Rust `kvd` on a local store directory
  (`internal/kvtest`, `kvd -store-dir`), so `go test ./...` needs no object
  store. Not ported: the Pebble compatibility test (it tested Pebble's
  format), the Go owner's remote-SST and checkpoint-cache tests.
- **Wake latency**, about 170 ms through the stack at the switch, is now
  90–100 ms (the Go owner: 80–95 ms): SlateDB's compactor and garbage
  collector start after the partition serves instead of inside the open
  (takeover 100 ms → 56 ms), and the partition's epoch is SlateDB's writer
  epoch, which the open already raises durably, instead of a durable write
  of its own (→ about 45–50 ms).
- **Store outages**: a failing object-store operation is retried
  `-store-retries` times (60, about a minute) before SlateDB gives up; the
  commit then fails and the partition stops serving, as the Go owner did
  after its 60 s commit timeout. Before, commits waited forever.
- **Open:** per-partition PUT accounting (SlateDB makes its own PUTs).

Found along the way, in the SQL layer and so for both owners:
go-mysql-server v0.20.0 skips an UPDATE whose new row equals the old one
under the column's collation, so `UPDATE t SET v='B'` on a row holding `b`
(case-insensitive collation) changes nothing, where MySQL stores `B`.

## Open questions carried from the spike

- Repeated reads took 150–350 µs, against about 10 µs for the Go owner.
  Profile in phase 2.
- 2.9 GB uploaded per 1 GB stored: tune compaction.
- About 1.3 GB of writer memory after loading 1 GB: phase 4 caps it per
  tenant.
- RGW reclaims the space of deleted objects only later; test clusters need
  room for it.
