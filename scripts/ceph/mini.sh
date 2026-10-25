#!/usr/bin/env bash
# Starts a minimal single-node Ceph cluster with the RGW S3 gateway, for
# running the owner's tests against Ceph instead of MinIO:
#
#   scripts/ceph/mini.sh start [dir]   # mon + mgr + one OSD + radosgw on :8000
#   scripts/ceph/mini.sh stop  [dir]
#
#   MINIO_ENDPOINT=127.0.0.1:8000 MINIO_ACCESS_KEY=s2zkey MINIO_SECRET_KEY=s2zsecret \
#     go test ./internal/objstore -run S3Compat -v
#
# Not for production: one OSD keeping its data in memory (memstore), pools
# with a single replica, and cephx disabled inside the cluster (S3 requests
# are still signed and checked by RGW). Needs the Ubuntu packages ceph-mon,
# ceph-osd, ceph-mgr, radosgw and ceph-common (Squid, 19.2).
set -euo pipefail

cmd=${1:-start}
D=$(realpath -m "${2:-${CEPH_MINI_DIR:-/tmp/ceph-mini}}")
PORT=${CEPH_RGW_PORT:-8000}
MEM_BYTES=${CEPH_MEMSTORE_BYTES:-6442450944} # 6 GiB
CONF=$D/ceph.conf
# The ceph CLI's Python bindings are built for the distribution's Python,
# which may not be the default python3.
PY=$(ls /usr/bin/python3.12 2>/dev/null || echo /usr/bin/python3)
ceph() { "$PY" /usr/bin/ceph -c "$CONF" "$@"; }

stop() {
	for p in radosgw ceph-osd ceph-mgr ceph-mon; do
		pkill -x "$p" 2>/dev/null || true
	done
	# Wait for them to exit (radosgw can take several seconds), then force:
	# a survivor would rejoin the next cluster on the same address.
	for _ in $(seq 20); do
		pgrep -x 'radosgw|ceph-osd|ceph-mgr|ceph-mon' >/dev/null || return 0
		sleep 0.5
	done
	pkill -9 -x 'radosgw|ceph-osd|ceph-mgr|ceph-mon' 2>/dev/null || true
	sleep 1
}

if [[ $cmd == stop ]]; then
	stop
	exit 0
fi
[[ $cmd == start ]] || { echo "usage: $0 start|stop [dir]" >&2; exit 2; }

stop
rm -rf "$D"
mkdir -p "$D"/{run,log,mon/ceph-a,mgr/ceph-x,osd,rgw/ceph-rgw.a}
FSID=$(uuidgen)
cat >"$CONF" <<EOF
[global]
fsid = $FSID
mon_host = [v2:127.0.0.1:3300,v1:127.0.0.1:6789]
public_network = 127.0.0.0/8
auth_cluster_required = none
auth_service_required = none
auth_client_required = none
run_dir = $D/run
admin_socket = $D/run/\$name.asok
log_file = $D/log/\$name.log
mon_data = $D/mon/\$cluster-\$id
mgr_data = $D/mgr/\$cluster-\$id
osd_data = $D/osd/\$cluster-\$id
rgw_data = $D/rgw/\$cluster-\$id
keyring = $D/keyring
osd_objectstore = memstore
memstore_device_bytes = $MEM_BYTES
osd_pool_default_size = 1
osd_pool_default_min_size = 1
osd_pool_default_pg_num = 8
osd_pool_default_pg_autoscale_mode = off
mon_allow_pool_size_one = true
mon_warn_on_pool_no_redundancy = false
osd_crush_chooseleaf_type = 0
mon_max_pg_per_osd = 1000

[client.rgw.a]
# Without tcp_nodelay, small responses wait out delayed ACKs: a 4 KiB GET
# took 43 ms on loopback instead of under 2 ms.
rgw_frontends = beast endpoint=127.0.0.1:$PORT tcp_nodelay=1
# radosgw asks for the "secure" monitor connection mode, which needs cephx;
# with auth disabled it must be allowed plain crc as well.
ms_mon_client_mode = crc secure
EOF

# Monitor.
ceph-authtool --create-keyring "$D/keyring" --gen-key -n mon. --cap mon 'allow *' >/dev/null
monmaptool --create --fsid "$FSID" --addv a '[v2:127.0.0.1:3300,v1:127.0.0.1:6789]' "$D/monmap" >/dev/null
ceph-mon -c "$CONF" --mkfs -i a --monmap "$D/monmap" --keyring "$D/keyring"
ceph-mon -c "$CONF" -i a
for _ in $(seq 30); do ceph -s >/dev/null 2>&1 && break; sleep 1; done
ceph mon enable-msgr2 >/dev/null 2>&1 || true
ceph config set mon mon_allow_pool_size_one true

# Manager and the one OSD.
ceph-mgr -c "$CONF" -i x
uuid=$(uuidgen)
id=$(ceph osd new "$uuid")
mkdir -p "$D/osd/ceph-$id"
ceph-osd -c "$CONF" -i "$id" --mkfs --osd-uuid "$uuid" >/dev/null 2>&1
ceph-osd -c "$CONF" -i "$id"
for _ in $(seq 60); do
	[[ $(ceph osd stat -f json 2>/dev/null | "$PY" -c 'import json,sys; print(json.load(sys.stdin).get("num_up_osds",0))') == 1 ]] && break
	sleep 1
done

# The S3 gateway and a user.
radosgw -c "$CONF" -n client.rgw.a
for _ in $(seq 60); do curl -s -o /dev/null "http://127.0.0.1:$PORT" && break; sleep 1; done
radosgw-admin -c "$CONF" user create --uid=s2z --display-name=s2z \
	--access-key=s2zkey --secret=s2zsecret >/dev/null
ceph -s | sed 's/^/  /'
echo "RGW on 127.0.0.1:$PORT (access key s2zkey, secret s2zsecret); data and logs in $D"
