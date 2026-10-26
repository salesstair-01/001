#!/usr/bin/env bash
# Fetches MySQL 8.0.46's own test suite (mysql-test/t, r, include) and its
# error names (share/messages_to_clients.txt) into scripts/mtr/mysql-server.
# They are GPL-2.0 and stay out of this repository (.gitignore).
set -eu
tag=mysql-8.0.46
here="$(cd "$(dirname "$0")" && pwd)"
dir="$here/mysql-server"
if [ -d "$dir/.git" ] && [ -f "$dir/share/messages_to_clients.txt" ]; then
  exit 0
fi
rm -rf "$dir"
git -c advice.detachedHead=false clone -q --depth 1 --filter=blob:none --sparse --branch "$tag" \
  https://github.com/mysql/mysql-server.git "$dir"
git -C "$dir" sparse-checkout set --no-cone \
  /mysql-test/t/ /mysql-test/r/ /mysql-test/include/ /share/messages_to_clients.txt
