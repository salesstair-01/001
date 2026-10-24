# Remote SSTs (spike)

`kv.Config.RemoteSSTs` has Pebble v2 write the SSTs of flushes and compactions
straight to the object store and read them back lazily, through a local block
cache. A wake then downloads only Pebble's metadata files instead of the whole
database. It is off by default. This page records how it works, what it costs,
and what we found.

## How it fits the durability rules (D4)

Pebble's remote storage (`Options.Experimental.RemoteStorage`,
`CreateOnShared`) assumes one Pebble instance per store, Pebble-managed
deletion, and overwrite on create. We have owners that come and go, fencing,
and manifests that lag behind Pebble. The adapter
(`internal/kv/engine_remote.go`) handles the difference as follows.

- **Fencing.** Each owner creates objects under its own epoch,
  `p/<id>/rsst/e<epoch>/<pebble name>`. The epoch is passed as the Pebble
  *locator*, which Pebble stores with every object, so later owners still
  find older objects. Epochs are unique because manifests are put-if-absent,
  and every upload is put-if-absent as well.
  - A per-epoch Pebble *creator ID* would not work. Pebble persists the
    creator ID, and file numbers carry on from the checkpoint, so a fenced
    owner and its successor would generate the same object names.
- **Deletion.** The adapter never deletes. When Pebble drops an object it is
  recorded as obsolete, and the owner's manifest-driven GC removes it after
  the grace period, exactly like an uploaded SST. Pebble's reference-marker
  objects are never written (we report them as present), which saves a PUT
  per SST.
- **Manifests** list the live remote SSTs (`"remote"`), with sizes, so a
  wake needs no HEAD requests. The list comes from the checkpoint's copy of
  Pebble's remote-object catalog, intersected with the uploads we have seen
  finish (see *Pebble findings*).
- **Sweep.** Objects of earlier epochs that no stored manifest lists are
  scheduled for deletion after takeover. These come from owners that
  crashed or were fenced before a checkpoint, and from uploads that Pebble
  aborted.
- **Restore** reads the source's remote SSTs in place and writes the
  restored partition locally.

`objstore.Store` gains `GetRange` and `Size` for this.

## Results

`cmd/remotebench` loads a tenant through the KV API (8 MiB transactions of
1 KiB rows, about 2:1 compressible), suspends it, and wakes it on an empty
host with no local file cache. It then makes 200 random point reads twice
(cold, then warm) and scans 1,000 rows. MinIO runs on loopback;
`-latency 20ms` adds 20 ms to every request to approximate S3.

### MinIO on loopback (well under 1 ms per request)

| size | mode | cold wake | data downloaded at wake | first read | point p50 / p99, cold | point p50 / p99, warm | scan 1,000 rows | local disk |
|---|---|---|---|---|---|---|---|---|
| 100 MB | upload at checkpoint | 222 ms | 58 MB | 0.19 ms | 32 / 99 µs | 7 / 18 µs | 12 ms | 58 MB |
| 100 MB | remote SSTs | 26 ms | 0.2 MB | 6.5 ms | 1.9 / 24 ms | 7 / 47 µs | 17 ms | 64 MB |
| 500 MB | upload at checkpoint | 1,035 ms | 289 MB | 0.10 ms | 45 / 372 µs | 10 / 73 µs | 7 ms | 289 MB |
| 500 MB | remote SSTs | 44 ms | 0.8 MB | 9.0 ms | 3.3 / 15 ms | 8 / 45 µs | 41 ms | 64 MB |
| 1,000 MB | upload at checkpoint | 2,025 ms | 578 MB | 0.10 ms | 29 / 209 µs | 9 / 23 µs | 4.5 ms | 578 MB |
| 1,000 MB | remote SSTs | 42 ms | 0.8 MB | 12 ms | 6.5 / 14 ms | 9 / 35 µs | 16 ms | 64 MB |

### 20 ms added per request (S3-like)

| size | mode | cold wake | first read | point p50 / p99, cold | point p50 / p99, warm | scan 1,000 rows |
|---|---|---|---|---|---|---|
| 100 MB | upload at checkpoint | 341 ms | 0.19 ms | 35 / 286 µs | 5 / 39 µs | 4.6 ms |
| 100 MB | remote SSTs | 193 ms | 70 ms | 23 / 73 ms | 23 / 72 ms * | 52 ms |
| 500 MB | upload at checkpoint | 972 ms | 0.12 ms | 25 / 71 µs | 6 / 10 µs | 7.1 ms |
| 500 MB | remote SSTs | 269 ms | 70 ms | 47 / 74 ms | 8 / 39 µs | 147 ms |
| 1,000 MB | upload at checkpoint | 1,852 ms | 0.12 ms | 24 / 264 µs | 10 / 195 µs | 3.8 ms |
| 1,000 MB | remote SSTs | 271 ms | 67 ms | 49 / 75 ms | 14 / 101 µs | 99 ms |

\* At 100 MB the load left a compaction owed. It ran right after the wake
and rewrote the tables being read, so the "warm" reads were cold again.

Writes cost the same in both modes: the load made 40–440 SST PUTs and
uploaded the same number of bytes either way. With remote SSTs, compaction
outputs are uploaded as they are written rather than at the next checkpoint.
Uploads are paced by the per-partition PUT quota in both modes.

### Reading the numbers

- **Wake time no longer depends on database size.** At 1 GB a wake takes
  42 ms instead of 2 s on loopback, and 271 ms instead of 1.9 s at 20 ms per
  request. It downloads under 1 MB instead of the whole database. The
  remaining cost is a few metadata files plus the WAL tail.
- **Local disk is fixed:** 64 MB (`RemoteCacheBytes`, allocated in full at
  open) instead of the whole database. That is what would let one node hold
  many more sleeping tenants.
- **The first read of each block costs a ranged GET, in sequence.** A cold
  point read makes two or three of them (index, filter, data block): about
  50 ms at 20 ms per request. A cold 1,000-row scan takes about 100–150 ms.
  Once a block is cached, reads are as fast as local ones.
- **An owed compaction now runs after the wake, in the background.** It
  costs GETs and PUTs, and it replaces cached tables with new, uncached ones.

## Pebble findings

1. **Open waited for compactions (fixed for both modes).** `pebble.Open`
   starts compactions, and `scanObsoleteFiles` then waits for all of them to
   finish. A tenant suspended with an L0 backlog therefore made the next wake
   wait for a whole compaction:
   - with remote SSTs, 1.8 s at 100 MB instead of 26 ms;
   - with uploads, 903 ms instead of 222 ms.

   `openEngine` now installs a compaction scheduler that grants nothing
   until `Open` has returned. That also guarantees no remote object is
   created before `SetCreatorID`.
2. **Catalog bug in Pebble v2.1.7.** `Catalog.Checkpoint` copies the catalog
   file under its live name (say `REMOTE-OBJ-CATALOG-000002`) but restarts
   the checkpoint's marker at iteration 1. When the checkpoint is reopened,
   the first catalog rewrite creates `…-000002` over the current file and
   then removes the "previous" file, which is the same file. The marker is
   left pointing at nothing. `fixCatalogMarker` renames the file to match
   its marker before open. This should be reported upstream.
3. **The catalog lists objects still being written.** Pebble adds an object
   to its catalog when creation starts, and any version edit can write the
   catalog to disk in the meantime. A checkpoint's catalog can therefore
   list outputs that aren't finished. We skip entries whose upload we
   haven't seen finish; nothing can be in a version before it has finished.
4. **Pebble may reuse a file number with different contents.** After a
   failed flush whose upload had in fact landed, Pebble retried under the
   same object name with different bytes. Put-if-absent refused the
   overwrite, and Pebble moved on to a new number. With Pebble's own
   overwrite-on-create, the object would have been silently replaced. The
   orphaned first attempt is left for the sweep.
5. **Pebble never reports checkpointed objects as deleted.**
   `CheckpointState` protects every remote object it checkpoints for the
   rest of the engine's life. Those objects leave through the manifest
   diff; objects that were never checkpointed leave through the
   obsolete-callback.
6. **Invariant builds (and `-race`) check that reference markers exist** when
   a table is opened. The adapter reports markers as present.

## Tests

- `remote_test.go`:
  - a wake that downloads no SSTs and reads with ranged GETs only;
  - restarts with GC (every stored object is listed or scheduled);
  - a fenced owner that keeps flushing under its own epoch;
  - the sweep;
  - restore.
- A `remote-ssts` configuration in the metamorphic test.
- `TestFaultInjectionRemoteSSTs`, where faults also hit flushes and
  compactions.
- `TestMinIORemoteSSTs` against a real MinIO.

## Not done

- **Uploads buffer the whole SST in memory.** Pebble's compaction target
  sizes keep this to a few MiB per upload, but it should become a streaming
  multipart upload with put-if-absent on completion.
- **The GC schedule lives in memory.** An owner that stops before the grace
  period ends loses it. The sweep covers earlier epochs; the same gap exists
  for uploaded SSTs today.
- **Pebble closes the writer on abort**, which uploads a partial object. It
  is never referenced and the sweep removes it, but it costs a PUT.
- **The manifest format number was not raised for `"remote"`.** Builds from
  before this change fail to open such a partition (no remote storage
  configured) rather than misreading it.
- **The Pebble options are experimental and can change between releases.**
  They are confined to `engine.go` and `engine_remote.go`.

## Options worth trying next

- **Hybrid placement.** `CreateOnSharedLower` keeps L0–L4 local and puts
  only L5–L6 (most of the data) remote. Recent data stays local and cheap to
  read, and wake stays small. Our checkpoint already handles a mix of local
  and remote SSTs.
- **Warm the cache in the background.** Prefetch index and filter blocks
  after a wake, so that a cold point read costs one GET, not three.
- **Per-tenant policy.** Choose the mode per tenant by size and wake
  frequency.
