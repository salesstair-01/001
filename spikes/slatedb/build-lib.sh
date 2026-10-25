#!/usr/bin/env bash
# Builds SlateDB's UniFFI library (the Go binding links against it) from the
# tag the binding was generated from, and prints the directory holding it:
#
#   L=$(spikes/slatedb/build-lib.sh)
#   CGO_LDFLAGS="-L$L" go build -C spikes/slatedb -o slatespike .
#   LD_LIBRARY_PATH=$L spikes/slatedb/slatespike smoke
#
# Needs git, a C toolchain and rustup (the repository pins its Rust version).
set -euo pipefail
VERSION=${SLATEDB_VERSION:-v0.17.0} # must match slatedb.io/slatedb-go in go.mod
DIR=$(cd "$(dirname "$0")" && pwd)/.slatedb
if [[ ! -d $DIR/.git ]]; then
	git clone -q --depth 1 --branch "$VERSION" https://github.com/slatedb/slatedb "$DIR" 2>/dev/null
fi
(cd "$DIR" && cargo build -q -p slatedb-uniffi --release >&2)
echo "$DIR/target/release"
