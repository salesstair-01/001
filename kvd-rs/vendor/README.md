# Vendored SlateDB

`slatedb` and `slatedb-txn-obj` 0.17.0 (Apache-2.0, from crates.io), used
through `[patch.crates-io]` in `../Cargo.toml`. They are patched so that a
database opens in fewer sequential object-store requests, which is most of
a wake: `cargo run --release --example opentrace` prints the requests of a
takeover. 12 sequential round trips (with kvd's first read) became 8; at
5 ms per request, 79 ms became 51 ms.

| change | file | round trips saved |
|---|---|---|
| A sequenced object's latest version and the GC boundary are read concurrently, then compared (`BoundaryObject::read`). Writes still check the boundary after creating a version. | `slatedb-txn-obj/src/lib.rs`, `object_store.rs` | 1 per manifest read |
| The fencer probes for the next WAL id while it claims the writer epoch; `fence_and_init` re-checks the id against the fenced manifest, as before. | `slatedb/src/fence.rs` | 1 |
| No manifest refresh after the WAL is fenced: the epoch write is a conditional create, so the manifest is the latest as of this writer's epoch; a newer writer's fencing WAL collides with this writer's next WAL write, and the manifest poller refreshes as soon as the database starts. | `slatedb/src/fence.rs` | 2 |

Tried and dropped: not reading back the empty fencing WAL during replay.
The replay needs to reach it (the last data WAL is only sealed once the
next file is seen), and its HEAD is what detects WAL objects deleted under
an unchanged epoch (`wal_replay_not_found_should_remain_not_found_when_writer_epoch_unchanged`).

`make slatedb-test` runs SlateDB's own library and integration tests on
these copies (CI runs it in the owner job); they all pass. kvd's tests
(fencing, takeover during commits, seeded faults, metamorphic) cover the
open path too.

To upgrade SlateDB: copy the new release over these directories and
re-apply the changes above (each is marked by a comment in the code).
