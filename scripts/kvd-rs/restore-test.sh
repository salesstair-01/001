#!/usr/bin/env bash
# Point-in-time restore with the Rust owner: write rows, note the time,
# change them, restore the partition as of that time into a new partition,
# and check the restored data through sqld. Needs MINIO_* (e.g.
# `eval "$(scripts/ceph/microceph.sh env)"`), bin/sqld and
# kvd-rs/target/release/kvd.
set -euo pipefail
cd "$(dirname "$0")/../.."
K=kvd-rs/target/release/kvd
W=$(mktemp -d)
SRC=$(( $(date +%s%N) % 1000000000 + 2000000 ))
DST=$(( SRC + 1 ))
pids=()
trap 'kill "${pids[@]}" 2>/dev/null || true' EXIT

q() { mysql -h127.0.0.1 -P"$1" -uroot app -N -e "$2"; }
expect() { # expect <what> <want> <got>
  if [ "$2" != "$3" ]; then echo "FAIL: $1: want [$2], got [$3]"; exit 1; fi
  echo "ok: $1"
}

$K -listen 127.0.0.1:7271 > "$W/kvd.log" 2>&1 & pids+=($!)
sleep 1
./bin/sqld -listen 127.0.0.1:3471 -kv 127.0.0.1:7271 -partition $SRC > "$W/sqld-src.log" 2>&1 & pids+=($!)
for _ in $(seq 30); do q 3471 "select 1" >/dev/null 2>&1 && break; sleep 1; done

q 3471 "create table t (id int auto_increment primary key, v varchar(10), key(v)); insert into t(v) values ('a'),('b'),('c')"
sleep 1
AT=$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)
sleep 1
q 3471 "update t set v='bb' where id=2; delete from t where id=3; insert into t(v) values ('d'); create table later (i int primary key)"
expect "source after the changes" "1 a,2 bb,4 d" "$(q 3471 "select id, v from t order by id" | tr '\t\n' ' ,' | sed 's/,$//')"

$K restore-points -partition $SRC
$K restore -src $SRC -dst $DST -at "$AT"
./bin/sqld -listen 127.0.0.1:3472 -kv 127.0.0.1:7271 -partition $DST > "$W/sqld-dst.log" 2>&1 & pids+=($!)
for _ in $(seq 30); do q 3472 "select 1" >/dev/null 2>&1 && break; sleep 1; done

expect "restored rows" "1 a,2 b,3 c" "$(q 3472 "select id, v from t order by id" | tr '\t\n' ' ,' | sed 's/,$//')"
expect "restored secondary index" "2" "$(q 3472 "select id from t where v='b'")"
expect "tables as of the restore time" "t" "$(q 3472 "show tables")"
q 3472 "insert into t(v) values ('e')"
expect "AUTO_INCREMENT continues from the restored counter" "4" "$(q 3472 "select id from t where v='e'")"
expect "the source is unchanged" "1 a,2 bb,4 d" "$(q 3471 "select id, v from t order by id" | tr '\t\n' ' ,' | sed 's/,$//')"
if $K restore -src $SRC -dst $DST -at "$AT" >/dev/null 2>&1; then
  echo "FAIL: a restore into a used partition must be refused"; exit 1
fi
echo "ok: a restore into a used partition is refused"
echo PASS
