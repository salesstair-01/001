#!/usr/bin/env bash
# Downloads the sqllogictest files listed in files.txt into scripts/slt/test.
set -eu
commit=04b0d198bf9dff62ddfbf0e5d04f5ae63a649ba3
here="$(cd "$(dirname "$0")" && pwd)"
grep -v '^#' "$here/files.txt" | while read -r f; do
  [ -n "$f" ] || continue
  out="$here/test/$f"
  [ -s "$out" ] && continue
  mkdir -p "$(dirname "$out")"
  curl -fsSL -o "$out" "https://raw.githubusercontent.com/dolthub/sqllogictest/$commit/test/$f"
done
