# kvd-rs: the KV owner on SlateDB

The Rust rewrite of `cmd/kvd`, phase 2 of `docs/slatedb-plan.md`. It
serves the same gRPC API (`proto/s2z/kv/kv.proto`) and takes the same flags,
so `sqld` runs against it unchanged.

```sh
make ceph-up && eval "$(scripts/ceph/microceph.sh env)"   # or scripts/ceph/mini.sh start
make kvd-rs                                               # cargo build --release
kvd-rs/target/release/kvd -listen 127.0.0.1:7070 &
bin/sqld -kv 127.0.0.1:7070 -partition 1 &
make crash-test-rs                                        # kill -9 loop, as for the Go owner
make kvsuite                                              # KV suites over gRPC against both owners
make wake-demo-rs                                         # dird spawns the Rust kvd; it suspends, exits, wakes
```

## Layout

| file | what |
|---|---|
| `src/partition.rs` | one partition: a SlateDB database at `p/<id>` in the bucket; takeover, MVCC reads, intents, commit, AUTO_INCREMENT, lease expiry, MVCC GC |
| `src/locks.rs` | row locks and deadlock detection, a port of `internal/kv/locks.go` |
| `src/keys.rs` | stored key layout (escaped user key + inverted commit_ts) |
| `src/node.rs` | the partitions this process serves; concurrent opens share one takeover |
| `src/service.rs` | the `s2z.kv.KV` gRPC service |
| `src/error.rs` | KV errors, encoded as the Go owner encodes them |

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
  `s2z_kv_takeover_seconds{cache="none"}` and `s2z_kv_partitions`.
  `s2z_kv_puts_total` is not there: SlateDB makes its own PUTs.

`-cache-dir`, `-put-concurrency`, `-put-rate` and `-put-burst` are accepted
and have no effect.

## Results so far (Ceph RGW, loopback)

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
| sqllogictest, the four `index/*/10` files (40,173 records) | 100% | 100%, the same counts (other files: run pending) |
| scale-to-zero demo: suspend and exit after 3 s idle, wake on the next query | wake 80–95 ms | wake ~170 ms (SlateDB open 70–85 ms + durable epoch write) |
| drivers and ORM through `proxyd` + `dird` spawning `kvd` | 4/4, 16/16 | 4/4, 16/16 |

The crash-test counts depend on the random kill timing; both pass.
