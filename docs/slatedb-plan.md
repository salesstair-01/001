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

Until the Rust owner reaches parity, the Go owner stays the default and
both are tested against the same suites.

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

## Open questions carried from the spike

- Repeated reads took 150–350 µs, against about 10 µs for the Go owner.
  Profile in phase 2.
- 2.9 GB uploaded per 1 GB stored: tune compaction.
- About 1.3 GB of writer memory after loading 1 GB: phase 4 caps it per
  tenant.
- RGW reclaims the space of deleted objects only later; test clusters need
  room for it.
