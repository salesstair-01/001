# kvd-rs: the KV owner on SlateDB

The KV owner, `kvd`: it serves the gRPC API of `proto/s2z/kv/kv.proto` to
the query engines (`sqld`). It replaced the Go owner, which it was ported
from (`docs/slatedb-plan.md`); it takes the Go owner's flags.

```sh
make kvd                                                  # cargo build --release; copies it to bin/kvd
bin/kvd -listen 127.0.0.1:7070 -store-dir ./data/store &  # a local store, for development
make ceph-up && eval "$(scripts/ceph/microceph.sh env)"   # or an S3 store (Ceph RGW)
bin/kvd -listen 127.0.0.1:7070 &
bin/sqld -kv 127.0.0.1:7070 -partition 1 &
cd kvd-rs && cargo test --release                         # the owner's tests (tests/), in memory
```

## Tests

`tests/` are the Go owner's tests, ported: `transactions.rs` (snapshots,
rollback, scans, guards, AUTO_INCREMENT, MVCC GC, locks, deadlocks, leases),
`durability.rs` (fencing, takeover during commits, seeded object-store
faults, lost acknowledgements), `metamorphic.rs` (a model of the transaction
semantics checked under restarts and GC) and `node_restore.rs`. They run on
an in-memory store. Longer runs: `KVD_FAULT_SEEDS=200` for `durability`,
`KVD_META_SEEDS=100 KVD_META_OPS=500` for `metamorphic`. The Go side runs
`internal/kvsuite` against the binary over gRPC.

## Layout

| file | what |
|---|---|
| `src/partition.rs` | one partition: a SlateDB database at `p/<id>` in the bucket; takeover, MVCC reads, intents, commit, AUTO_INCREMENT, lease expiry, MVCC GC |
| `src/locks.rs` | row locks and deadlock detection, a port of `internal/kv/locks.go` |
| `src/keys.rs` | stored key layout (escaped user key + inverted commit_ts) |
| `src/node.rs` | the partitions this process serves; concurrent opens share one takeover |
| `src/service.rs` | the `s2z.kv.KV` gRPC service |
| `src/error.rs` | KV errors, encoded as `internal/kvrpc.FromStatus` decodes them |
| `src/agent.rs` | the directory agent: register, heartbeat, release, exit when idle |
| `src/restore.rs` | point-in-time restore |
| `src/localstore.rs` | the local-directory store (`-store-dir`) |
| `src/metrics.rs` | Prometheus metrics, and the store wrapper that counts PUTs by partition |
| `vendor/` | SlateDB 0.17.0, patched to open in fewer object-store round trips (`vendor/README.md`) |

## How it maps onto SlateDB

- **Takeover and fencing.** Opening the database fences the previous
  writer (SlateDB's writer epoch). The owner then durably raises its own
  `epoch` record, which query engines see. A fenced owner's next write
  fails; it stops serving and its clients get `EpochChanged`.
- **Commit.** Under one ordering lock: check guards and insert-only keys,
  assign `commit_ts = max + 1`, and write one `WriteBatch` holding the
  versions, the committed transaction id, the AUTO_INCREMENT counters used
  and the new `max_commit_ts`. Outside the lock: wait for durability, then
  raise the visible timestamp, release the locks and acknowledge. Writes
  from many transactions share SlateDB's WAL flush (every 10 ms), which is
  the group commit.
- **Reads.** Snapshot reads see versions up to the snapshot, never past the
  visible timestamp, so nothing that isn't durable is read. Locking reads
  take the lock first, then read the newest version.
- **Recovery.** SlateDB replays its WAL on open, so after a crash there is
  nothing to do besides reloading the committed ids of the last 10 minutes.

## Node features (phase 4)

- **Directory.** With `-directory`, the node registers (as `-node-id`, at
  `-advertise` or the address it bound, so `dird` can start it on port 0),
  heartbeats its partitions every 2 s and releases each partition it
  suspends (`src/agent.rs`).
- **Idle suspend.** With `-idle-suspend`, a partition with no open
  transactions and no requests for that long is suspended: new requests
  get `EpochChanged` (the query engine re-resolves and wakes it), and the
  database is closed with a memtable flush.
- **Exit when idle.** With `-exit-when-idle`, a dedicated per-tenant
  process exits once it serves nothing, or when it was never given a
  partition within the idle time plus 30 s. `dird -spawn-kvd` starts the
  Rust binary as it starts the Go one.
- **Memory per tenant.** `-block-cache-mb` is one block cache shared by
  every partition of the node (SlateDB would give each database its own);
  `-memtable-mb` is the memtable size before it is flushed to an SST, and
  `-max-unflushed-mb` caps the data a partition holds in memory before
  writes wait.
- **Metrics.** `-metrics-listen` serves `s2z_kv_commit_seconds`,
  `s2z_kv_takeover_seconds{cache="none"}`, `s2z_kv_partitions` and
  `s2z_kv_puts_total{partition}`: SlateDB makes its own PUTs, so they are
  counted at the object store, by the partition's prefix (`p/<id>/`). An
  idle partition makes none.

`-cache-dir`, `-put-concurrency`, `-put-rate` and `-put-burst` are accepted
and have no effect.

## Point-in-time restore (phase 5)

```sh
kvd-rs/target/release/kvd restore-points -partition 42
kvd-rs/target/release/kvd restore -src 42 -dst 1042 -at 2026-10-16T09:30:00Z
```

A partition keeps every MVCC version for `-restore-window` (1 h by default):
GC's safe point never passes the last commit older than the window. Each
commit also writes a record of its wall time and commit_ts. A restore finds
the last commit at or before the time, reads the source through a SlateDB
reader (it holds a checkpoint, so nothing it reads is collected, and it does
not fence the live owner), and writes each key's newest version at or below
that commit into a new partition, with the partition id in the keys
rewritten. Point the tenant at the new partition.
`scripts/kvd-rs/restore-test.sh` checks it end to end through `sqld`.

## Compared with the Go owner it replaced (Ceph RGW, loopback)

| | Go owner | Rust owner |
|---|---|---|
| commit through `sqld` (single client, autocommit INSERT) | 20.7 ms | 11.7 ms |
| point SELECT through `sqld` | 3.4 ms | 3.3 ms |
| crash test, 5 × kill -9 | 609 acknowledged, 0 lost | 397 acknowledged, 0 lost |
| takeover after kill -9 | | 80–210 ms |
| driver matrix (Go, Python, Node, Java) | 4/4 | 4/4 |
| ORM suite (SQLAlchemy + Alembic) | 16/16 | 16/16 |
| fencing: a second owner takes over | old owner's writes refused | old owner's writes refused |
| `internal/kvsuite` over gRPC: semantics, 40 metamorphic seeds × 500 ops | pass | pass |
| sqllogictest subset | 139,122 passed, 0 wrong | the same, run in CI |
| scale-to-zero demo: suspend and exit after 3 s idle, wake on the next query | wake 80–95 ms | wake 56–59 ms in CI (90–100 ms on a local MicroCeph; takeover ~50 ms) |
| drivers and ORM through `proxyd` + `dird` spawning `kvd` | 4/4, 16/16 | 4/4, 16/16 |
| point-in-time restore (`scripts/kvd-rs/restore-test.sh`) | (`s2zctl`) | pass; ~300 ms for a small partition |

The crash-test counts depend on the random kill timing; both pass.
