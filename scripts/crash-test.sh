#!/usr/bin/env bash
# Slice 1 crash script: write loop, random kill -9 of kvd, then verify every
# acknowledged row. Needs MinIO on $MINIO_ENDPOINT (default 127.0.0.1:9000),
# e.g. `docker compose up -d minio`.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p bin
go build -o bin/ ./cmd/kvd ./cmd/sqld ./cmd/crashtest
exec ./bin/crashtest -kvd bin/kvd -sqld bin/sqld "$@"
