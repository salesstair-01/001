#!/usr/bin/env bash
# Development object store: a single-node MicroCeph (Canonical's snap of
# Ceph Squid) with three loop-file OSDs and the RGW S3 gateway on :8000.
#
#   scripts/ceph/microceph.sh up       # install (once), bootstrap, enable RGW, create the user
#   scripts/ceph/microceph.sh env      # print the variables the tests and kvd read
#   scripts/ceph/microceph.sh status
#   scripts/ceph/microceph.sh down     # remove MicroCeph and all its data
#
#   eval "$(scripts/ceph/microceph.sh env)"
#   go test ./internal/objstore -run 'S3Compat|MinIO' -v && go test ./internal/kvsuite -v
#
# Needs Linux with snapd and systemd (Ubuntu 22.04+); on macOS or Windows
# run it inside a Multipass VM and point MINIO_ENDPOINT at the VM's address:
#
#   multipass launch 24.04 --name ceph --cpus 2 --memory 4G --disk 30G
#   multipass transfer scripts/ceph/microceph.sh ceph:
#   multipass exec ceph -- ./microceph.sh up
#
# Uses sudo. Release testing still runs against a full cluster (cephadm or
# Rook) configured as in docs/object-stores.md; this is for day-to-day work.
set -euo pipefail

CHANNEL=${MICROCEPH_CHANNEL:-squid/stable} # Ceph 19.2, as tested
PORT=${CEPH_RGW_PORT:-8000}
OSD_SIZE=${MICROCEPH_OSD_SIZE:-4G}
ACCESS=${S3_ACCESS_KEY:-s2zkey}
SECRET=${S3_SECRET_KEY:-s2zsecret}
CONF=/var/snap/microceph/current/conf/radosgw.conf

ceph() { sudo microceph.ceph "$@"; }

wait_for() { # seconds, description, command...
	local n=$1 what=$2
	shift 2
	for _ in $(seq "$n"); do
		"$@" >/dev/null 2>&1 && return 0
		sleep 1
	done
	echo "timed out waiting for $what" >&2
	return 1
}

# MicroCeph writes the RGW frontend line itself and has no option for extra
# frontend settings. Without tcp_nodelay, small responses wait out delayed
# ACKs (a 4 KiB GET took 43 ms instead of 1.7 ms), so add it to the line and
# restart the gateway. MicroCeph only rewrites the line when RGW is
# reconfigured; `up` puts it back each time.
ensure_nodelay() {
	if ! sudo grep -q '^rgw frontends = beast.*tcp_nodelay=1' "$CONF"; then
		sudo sed -i '/^rgw frontends = beast/ s/$/ tcp_nodelay=1/' "$CONF"
		sudo snap restart microceph.rgw >/dev/null
	fi
}

rgw_up() { curl -s -o /dev/null "http://127.0.0.1:$PORT"; }

up() {
	if ! snap list microceph >/dev/null 2>&1; then
		sudo snap install microceph --channel="$CHANNEL"
		sudo snap refresh --hold microceph >/dev/null # no surprise upgrades mid-work
	fi
	if ! sudo microceph status >/dev/null 2>&1; then
		sudo microceph cluster bootstrap
	fi
	if ! ceph osd stat 2>/dev/null | grep -q '^[1-9][0-9]* osds'; then
		sudo microceph disk add "loop,$OSD_SIZE,3"
	fi
	wait_for 300 "3 OSDs up" bash -c 'sudo microceph.ceph osd stat | grep -q "3 osds: 3 up"'
	if ! sudo microceph status | grep -q rgw; then
		sudo microceph enable rgw --port "$PORT"
	fi
	wait_for 300 "the RGW config" sudo test -f "$CONF"
	ensure_nodelay
	wait_for 300 "RGW on :$PORT" rgw_up
	if ! sudo microceph.radosgw-admin user info --uid=s2z >/dev/null 2>&1; then
		sudo microceph.radosgw-admin user create --uid=s2z --display-name=s2z \
			--access-key="$ACCESS" --secret="$SECRET" >/dev/null
	fi
	ceph -s | sed 's/^/  /'
	echo "RGW on :$PORT (access key $ACCESS). Run: eval \"\$($0 env)\""
}

case ${1:-up} in
up) up ;;
env)
	echo "export MINIO_ENDPOINT=${CEPH_RGW_HOST:-127.0.0.1}:$PORT"
	echo "export MINIO_ACCESS_KEY=$ACCESS"
	echo "export MINIO_SECRET_KEY=$SECRET"
	;;
status)
	sudo microceph status
	ceph -s
	sudo grep '^rgw frontends' "$CONF"
	;;
down) sudo snap remove --purge microceph ;;
*)
	echo "usage: $0 up|env|status|down" >&2
	exit 2
	;;
esac
