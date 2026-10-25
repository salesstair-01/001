#!/usr/bin/env bash
# Crash script: write loop, random kill -9 of kvd, then verify every
# acknowledged row. Needs an object store on $MINIO_ENDPOINT, e.g.
# `make ceph-up && eval "$(scripts/ceph/microceph.sh env)"`.
set -euo pipefail
cd "$(dirname "$0")/.."
make -s kvd
go build -o bin/ ./cmd/sqld ./cmd/crashtest
exec ./bin/crashtest -kvd bin/kvd -sqld bin/sqld "$@"
