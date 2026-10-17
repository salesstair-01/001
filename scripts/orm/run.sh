#!/usr/bin/env bash
# Runs the ORM compatibility suites. Usage: run.sh [sqlalchemy]
# Connection: ORM_HOST (127.0.0.1), ORM_PORT (3306), ORM_USER (root),
# ORM_PASSWORD, ORM_DB (ormtest; dropped and re-created).
set -u
here="$(cd "$(dirname "$0")" && pwd)"
deps="$here/.deps"
suites=("$@")
[ ${#suites[@]} -eq 0 ] && suites=(sqlalchemy)
user="$(python3 -c 'import sys, urllib.parse; print(urllib.parse.quote(sys.argv[1], safe=""))' "${ORM_USER:-root}")"
pass="$(python3 -c 'import sys, urllib.parse; print(urllib.parse.quote(sys.argv[1], safe=""))' "${ORM_PASSWORD:-}")"
auth="$user"
[ -n "$pass" ] && auth="$user:$pass"
failed=0
for s in "${suites[@]}"; do
  case "$s" in
    sqlalchemy)
      [ -d "$deps/py/sqlalchemy" ] || pip install -q --target "$deps/py" -r "$here/sqlalchemy/requirements.txt" >/dev/null 2>&1
      echo "== SQLAlchemy + Alembic"
      PYTHONPATH="$deps/py" ORM_URL="mysql+pymysql://$auth@${ORM_HOST:-127.0.0.1}:${ORM_PORT:-3306}/${ORM_DB:-ormtest}" \
        python3 "$here/sqlalchemy/suite.py" 2> >(grep -v '^INFO' >&2) || failed=1 ;;
    *) echo "unknown suite $s"; failed=1 ;;
  esac
done
exit $failed
