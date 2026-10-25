# Object stores

The owner needs an S3-compatible store whose `PutObject` honours
`If-None-Match: *` atomically. All fencing (WAL segments, manifests, the
owner epoch, D4) rests on that. It also needs ranged GETs, HEAD, ordered
LIST with start-after, and DELETE. `TestS3Compat` checks exactly these
things against whatever `MINIO_ENDPOINT` points at:

```sh
MINIO_ENDPOINT=host:port MINIO_ACCESS_KEY=... MINIO_SECRET_KEY=... \
  go test ./internal/objstore -run 'S3Compat|MinIO' -v
MINIO_ENDPOINT=... go test ./internal/kv -run MinIO -v    # KV owner on the store
MINIO_ENDPOINT=... make crash-test                         # kill -9 during writes
```

`MINIO_BUCKET` and `MINIO_REGION` are also read. `kvd` and `s2zctl` read
the same variables.

## Why not MinIO any more

MinIO's community edition went in stages:

- May 2025: the admin console was removed;
- October 2025: binary and image releases stopped;
- December 2025: maintenance mode;
- 25 April 2026: the repository was archived.

Its last release tag is `RELEASE.2025-10-15T17-29-55Z`. It still works, and
CI still runs a pinned build, but it no longer receives fixes.

## Ceph RGW: tested, passes

Ceph is governed by the Ceph Foundation, part of the Linux Foundation, and
runs on Kubernetes via Rook (CNCF graduated). We tested Squid 19.2.3 from
Ubuntu 24.04's packages, as a single-node cluster started by
`scripts/ceph/mini.sh`: one monitor, one manager, one memstore OSD, and
radosgw.

### Results

| Check | Result |
|---|---|
| `TestS3Compat` (all eight properties) | pass |
| Put-if-absent race (16 writers per key, 120 keys over 6 runs) | exactly one winner each time; stored bytes are the winner's |
| `TestMinIO`, `TestMinIOTakeoverAndFencing`, `TestMinIORemoteSSTs` | pass |
| Crash test (5 × `kill -9` of the KV owner) | 754 acknowledged commits, 0 lost (MinIO: 529 in the same test) |
| Driver matrix (Go, PyMySQL, mysql2, Connector/J) via `sqld` and `proxyd` | 4/4 and 4/4 |
| ORM suite (SQLAlchemy + Alembic) via `sqld` and `proxyd` | 16/16 and 16/16 |
| Mean 4 KiB put-if-absent / GET, loopback | 3.5 ms / 1.7 ms (MinIO: 2.8 ms / 1.2 ms) |

### `remotebench` on Ceph, loopback, compared with MinIO

| size | mode | cold wake: Ceph | cold wake: MinIO | point p50 at wake: Ceph | point p50 at wake: MinIO |
|---|---|---|---|---|---|
| 100 MB | upload at checkpoint | 141 ms | 222 ms | 18 µs | 32 µs |
| 100 MB | remote SSTs | 35 ms | 26 ms | 2.1 ms | 1.9 ms |
| 500 MB | upload at checkpoint | 920 ms | 1,035 ms | 34 µs | 45 µs |
| 500 MB | remote SSTs | 33 ms | 44 ms | 2.6 ms | 3.3 ms |

Ceph is on a par with MinIO for everything the owner does.

### Why the conditional write is atomic

Reading the source (v19.2.3):

1. `rgw_rest_s3.cc` (`RGWPutObj_ObjStore_S3::get_params`) reads
   `If-None-Match`.
2. `AtomicObjectProcessor::complete` (`rgw_putobj_processor.cc`) marks the
   object atomic, passes the header on, and sets `PUT_OBJ_CREATE`.
3. `RGWRados::Object::prepare_atomic_modification` (`rgw_rados.cc`) first
   refuses quickly if the object exists. It then adds `op.create(true)`, an
   exclusive create of the RADOS head object. RADOS serialises operations
   on one object at its primary OSD, so only one racing create can succeed.
4. `write_meta` turns the loser's `EEXIST` into `ERR_PRECONDITION_FAILED`,
   which is HTTP 412.

### Running Ceph for the owner

- **`tcp_nodelay=1` on the Beast frontend.** Without it, small responses
  wait out delayed ACKs: a 4 KiB GET took 43 ms instead of 1.7 ms, and the
  crash test managed only 57 commits instead of 754.
- **Keep buckets unversioned, in a single zone.** The atomicity above is
  for the plain, unversioned path within one cluster. Multi-site
  active-active could accept the same create in two zones.
- **Use the RADOS backend** (the default). RGW's other backends (DBStore,
  Motr, DAOS) do a weaker check.
- **The `mini.sh` settings are for tests only:** one replica, memory-backed
  storage, cephx off inside the cluster. A real deployment is a normal Ceph
  cluster (cephadm or Rook) with RGW in front.

### Development, CI and release testing

| Where | What | How |
|---|---|---|
| Development | MicroCeph: Canonical's snap of Ceph Squid, one node, three loop-file OSDs, RGW on :8000 | `make ceph-up`, then `make test-s3` (or `eval "$(scripts/ceph/microceph.sh env)"` before running anything); `make ceph-down` removes it. macOS and Windows: inside a Multipass VM (see the script's header). |
| CI | The same MicroCeph script on GitHub's Ubuntu runner: the compatibility and KV tests and the crash test | the `ceph` job |
| Release testing | A full cluster (cephadm or Rook), at least three OSD hosts with 3× replication, RGW configured as above | the same suites, pointed at it with `MINIO_ENDPOINT` and the credentials |
| No snapd or systemd (containers) | `scripts/ceph/mini.sh`: Ubuntu's Ceph packages, one memstore OSD | the results above were measured this way |

MicroCeph generates RGW's configuration itself and has no option for extra
frontend settings. `microceph.sh` therefore appends `tcp_nodelay=1` to the
generated frontend line and restarts RGW, and does it again on every `up`,
because MicroCeph rewrites the line whenever RGW is reconfigured.

On a full cluster, set it as part of the RGW service:

- cephadm: `rgw_frontend_extra_args: ["tcp_nodelay=1"]` in the service spec;
- Rook: the equivalent frontend setting on the object store.

## Other candidates (not run here)

| Store | Governance | Conditional writes | Notes |
|---|---|---|---|
| RustFS 1.0 | RustFS, Inc. (CLA), Apache-2.0 | Implemented. The precondition is checked under the object's namespace write lock at commit (`crates/ecstore/src/set_disk/ops/object.rs`), so it is atomic by design | Single binary, the closest to MinIO. 1.0 went GA in September 2026. Single vendor, no foundation. |
| Apache Ozone | Apache Software Foundation | Docs say "planned"; a July 2026 project blog says implemented | Needs a check of which release ships it |
| OpenStack Swift (s3api) | OpenInfra Foundation | `PUT` with `If-None-Match: *` merged | Not evaluated further |
| SeaweedFS | Independent, Apache-2.0 | Documented (wiki mirror), including atomicity | Not run here |
| Garage, versitygw | Independent | Unknown | Not run here |
| AWS S3, Cloudflare R2, GCS, and others | Hosted | AWS since 2024 (`If-None-Match: *` → 412) | `TestS3Compat` applies as-is |

## Recommendation

- **Production:** "any S3-compatible store that passes `TestS3Compat`".
  - Ceph RGW for self-hosted deployments: tested here, governed by the
    Linux Foundation, configured as above.
  - The cloud providers' S3 for hosted deployments.
- **Development and CI:** keep the pinned MinIO for now, and run the Ceph
  job alongside it. RustFS is the likely successor for a single-binary local
  store once it has been run against the suite.
