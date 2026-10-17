.PHONY: build test test-minio fault-1000 crash-test wake-demo wake-time cold-start slt drivers orm

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
