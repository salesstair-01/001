# SlateDB spike (against Ceph RGW)

Could [SlateDB](https://slatedb.io) replace the owner's storage layer?
That layer is the object-store WAL, group commit, manifests and fencing,
checkpoints, SST upload and download, and Pebble. The owner's MVCC and
locking layer (`internal/kv/txn.go`) would stay on top. This spike runs
SlateDB v0.17.0 through its official Go binding (`slatedb.io/slatedb-go`,
cgo over a Rust library) against Ceph RGW, the store the owner now targets
(see `docs/object-stores.md`).

The spike is a separate Go module, so the main build and CI need neither
Rust nor cgo.

## Running it

```sh
make ceph-up && eval "$(scripts/ceph/microceph.sh env)"   # or scripts/ceph/mini.sh start
L=$(spikes/slatedb/build-lib.sh)                          # builds libslatedb_uniffi (Rust; ~10 min the first time)
CGO_LDFLAGS="-L$L" go build -C spikes/slatedb -o slatespike .
cd spikes/slatedb && export LD_LIBRARY_PATH=$L
./slatespike smoke                     # write, read, scan, reopen
./slatespike fence                     # a second writer fences the first
./slatespike crash -kills 5 [-fail 0.05 -drop 0.05]   # kill -9 the writer; faults injected by a proxy
./slatespike ffi                       # cost of each call from Go into Rust
./slatespike density -n 100 [-shared-cache]
./slatespike bench -sizes 100,500,1000 [-latency 20ms]
```

SlateDB talks S3 from Rust, so the owner's Go fault wrapper cannot reach
it. Instead, `proxy.go` sits between SlateDB and RGW at the HTTP level. It
adds latency, answers 503 at random, lets a request land and then cuts the
connection, and counts requests and bytes.

The CI job `slatedb-spike`, on this branch only, runs all of the above
against MicroCeph.

## Results (Ceph 19.2 RGW, loopback)

### Correctness

| Check | Result |
|---|---|
| Works on Ceph RGW, including its manifest updates (conditional writes) | yes |
| Fencing: a second writer opens the database | the first writer's next write fails ("detected newer DB client") and never becomes visible |
| 5 × `kill -9` of the writer (8 concurrent committers, each waiting for durability) | 2,728 acknowledged writes, 0 lost |
| The same with 5% injected 503s and 5% dropped responses | 922 acknowledged, 0 lost, 117 faults injected |

### Calls from Go into Rust (in-memory store, so no I/O)

| Operation | Cost |
|---|---|
| Cached point read (`Get`) | 8.5 µs per call |
| Scan, row by row (`Next`) | 3.9 µs per row |
| Scan in batches (`NextBatch(1000)`) | 1.7 µs per row |

The owner's scans call seek and next many times per query, so it would
need batched iteration.

### Density (100 databases in one process, each with one write)

- About 0.2 MB of memory each while idle, with or without a shared cache.
- Threads went from 6 to 12 in total: all databases share one runtime.
- Opening them one after another took about 165 ms each, including one
  durable write.

### Load, wake and reads

Same load as `cmd/remotebench`: 8 MiB transactions of 1 KiB rows. The
database is shut down with a memtable flush (the equivalent of the owner's
checkpoint on suspend), then reopened with nothing cached. `flush_interval`
is set to 10 ms, the same as the owner's group-commit window.

MinIO-speed loopback (no added latency):

| size | load | MB uploaded | cold open | first read | point p50/p99 cold | repeat p50 | scan 1,000 rows | commit p50 | process memory after |
|---|---|---|---|---|---|---|---|---|---|
| 100 MB | 5.7 s | 207 | 85 ms | 23 ms | 4.4 / 10 ms | 166 µs | 209 ms | 11 ms | 321 MB |
| 500 MB | 25.9 s | 1,367 | 97 ms | 22 ms | 5.2 / 22 ms | 169 µs | 172 ms | 12 ms | 981 MB |
| 1,000 MB | 47.7 s | 2,935 | 96 ms | 27 ms | 4.8 / 22 ms | 160 µs | 90 ms | 11 ms | 1,300 MB |

With 20 ms added to every request (S3-like):

| size | load | MB uploaded | cold open | first read | point p50/p99 cold | repeat p50 | scan 1,000 rows | commit p50 | process memory after |
|---|---|---|---|---|---|---|---|---|---|
| 100 MB | 4.2 s | 207 | 529 ms | 79 ms | 26 / 56 ms | 152 µs | 83 ms | 28 ms | 339 MB |
| 500 MB | 21.6 s | 1,300 | 528 ms | 85 ms | 26 / 197 ms | 352 µs | 64 ms | 29 ms | 893 MB |
| 1,000 MB | (run in progress) | | | | | | | | | |

### Compared with the owner today

From `docs/remote-ssts.md` and `docs/object-stores.md`:

| | owner, download on wake | owner, remote SSTs + warming | SlateDB |
|---|---|---|---|
| Cold wake, 1 GB, loopback | 2.0 s | 42 ms | 96 ms |
| Cold wake, 1 GB, 20 ms per request | 1.9 s | 264 ms | about 530 ms at 100–500 MB (1 GB pending) |
| Cold point read p50, 20 ms per request | µs (all local) | 23–24 ms (one GET) | 26 ms |
| Repeat read | 6–10 µs | 9–14 µs | 150–350 µs |
| Uploaded to store 1 GB | 1.7–1.8 GB | 1.7–1.8 GB | 2.9 GB |
| Local disk | whole database | 64 MB | none (memory cache only by default) |

## Open questions

- **Repeated reads take 150–350 µs**, against 8.5 µs for a cached read in
  the in-memory call-cost test. It isn't yet known whether they reach the
  store again (the cache configuration) or what else costs the time.
- **Memory under load:** the writer held about 1.3 GB after loading 1 GB
  (64 MB L0 SSTs, up to 1 GB of unflushed data allowed by default). These
  limits would need tuning per tenant.
- **Uploads:** 2.9 GB were uploaded to store 1 GB, against 1.7–1.8 GB for
  the owner today. Compaction settings may lower that.
- **Writer language:** a Rust `kvd` using SlateDB directly would avoid the
  cost of each call from Go, behind the existing kvd RPC boundary, with
  protobuf in place of gob.
