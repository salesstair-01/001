.PHONY: build kvd test fault-runs metamorphic kvsuite test-s3 crash-test restore-test wake-demo cold-start slt drivers orm ceph-up ceph-down proto

# Every binary into bin/: the Go ones (sqld, dird, proxyd, tools) and kvd,
# the KV owner, from kvd-rs/ (Rust, on SlateDB).
build: kvd
	go build -o bin/ ./cmd/...

kvd:
	cd kvd-rs && cargo build --release
	mkdir -p bin && cp kvd-rs/target/release/kvd bin/kvd.new && mv bin/kvd.new bin/kvd

# Unit and integration tests. They need no object store: kvd runs on a
# local directory (-store-dir).
test: kvd
	cd kvd-rs && cargo test --release
	go test ./...

# The owner's fault-injection runs and metamorphic test, at length.
fault-runs:
	cd kvd-rs && KVD_FAULT_SEEDS=200 cargo test --release --test durability faults
metamorphic:
	cd kvd-rs && KVD_META_SEEDS=100 KVD_META_OPS=500 cargo test --release --test metamorphic

# The KV suites over gRPC against kvd (on S3 when MINIO_ENDPOINT is set).
kvsuite: kvd
	go test ./internal/kvsuite -count=1 -v

# Object-store tests against MicroCeph (or whatever MINIO_ENDPOINT points at).
test-s3: kvd
	eval "$$(scripts/ceph/microceph.sh env)" && \
	go test ./internal/objstore -run 'S3Compat|MinIO' -v && go test ./internal/kvsuite -count=1 -v

# kill -9 loop against kvd; needs MINIO_* (an object store).
crash-test: build
	./bin/crashtest -kvd bin/kvd -sqld bin/sqld -kills 5

# Point-in-time restore end to end through sqld; needs MINIO_*.
restore-test: build
	scripts/kvd-rs/restore-test.sh

wake-demo: build
	./bin/wakedemo -bin ./bin -idle 3s -rounds 3

cold-start: build
	./bin/coldstart -rounds 20 -warm 0
	./bin/coldstart -rounds 20 -warm 1

# sqllogictest subset (scripts/slt/files.txt); writes scripts/slt/results.md.
slt: kvd
	scripts/slt/fetch.sh
	go run ./cmd/slt -parallel 4 -summary scripts/slt/results.md $$(grep -v '^#' scripts/slt/files.txt | sed 's|^|scripts/slt/test/|')

# Driver matrix against DRV_HOST:DRV_PORT (see scripts/drivers/README.md).
drivers:
	scripts/drivers/run.sh

# ORM suites against ORM_HOST:ORM_PORT (see scripts/orm/README.md).
orm:
	scripts/orm/run.sh

# Development object store: single-node MicroCeph with RGW on :8000 (scripts/ceph/microceph.sh).
ceph-up:
	scripts/ceph/microceph.sh up
	eval "$$(scripts/ceph/microceph.sh env)" && go run ./cmd/s3bucket
ceph-down:
	scripts/ceph/microceph.sh down

# Regenerate the Go protobuf code (internal/kvrpc/kvpb, internal/directory/dirpb)
# from proto/s2z; kvd-rs generates its own at build time. Needs buf and
# protoc-gen-go on PATH:
#   go install github.com/bufbuild/buf/cmd/buf@v1.57.2
#   go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12
proto:
	cd proto && buf lint && buf generate
