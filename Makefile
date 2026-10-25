.PHONY: proto kvd-rs crash-test-rs build test test-minio ceph-up ceph-down test-s3 fault-1000 crash-test wake-demo wake-time cold-start slt drivers orm

build:
	go build -o bin/ ./cmd/...

# Unit and integration tests; the MinIO tests skip without MINIO_ENDPOINT.
test:
	go test ./...

test-minio:
	MINIO_ENDPOINT=$${MINIO_ENDPOINT:-127.0.0.1:9000} go test ./...

# Slice 2 bar: the fault tests pass 1,000 seeded runs.
fault-1000:
	go test ./internal/kv/ -run TestFaultInjection -fault-runs=1000 -timeout 30m

crash-test:
	./scripts/crash-test.sh -kills 5

wake-demo: build
	./bin/wakedemo -bin ./bin -idle 3s -rounds 3

wake-time: build
	./bin/waketime -sizes 100,1024,5120

cold-start: build
	./bin/coldstart -rounds 20 -warm 0
	./bin/coldstart -rounds 20 -warm 1

# sqllogictest subset (scripts/slt/files.txt); writes scripts/slt/results.md.
slt:
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
ceph-down:
	scripts/ceph/microceph.sh down

# Object-store tests against MicroCeph (or whatever MINIO_ENDPOINT points at).
test-s3:
	eval "$$(scripts/ceph/microceph.sh env)" && \
	go test ./internal/objstore -run 'S3Compat|MinIO' -v && go test ./internal/kv -run MinIO -v

# Regenerate the Go protobuf code (internal/kvrpc/kvpb, internal/directory/dirpb)
# from proto/s2z. Needs buf and protoc-gen-go on PATH:
#   go install github.com/bufbuild/buf/cmd/buf@v1.57.2
#   go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12
# The Rust KV owner on SlateDB (kvd-rs/, docs/slatedb-plan.md).
kvd-rs:
	cd kvd-rs && cargo build --release

# The crash test with the Rust owner (needs MINIO_* pointing at an object store).
crash-test-rs: kvd-rs
	go build -o bin/ ./cmd/sqld ./cmd/crashtest
	./bin/crashtest -kvd kvd-rs/target/release/kvd -sqld bin/sqld

proto:
	cd proto && buf lint && buf generate
