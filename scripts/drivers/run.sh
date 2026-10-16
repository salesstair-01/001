#!/usr/bin/env bash
# Runs the driver-matrix scenario. Usage: run.sh [go] [python] [node] [java]
# Connection: DRV_HOST, DRV_PORT, DRV_USER, DRV_PASSWORD, DRV_DB (app).
set -u
here="$(cd "$(dirname "$0")" && pwd)"
deps="$here/.deps"
mkdir -p "$deps"
drivers=("$@")
[ ${#drivers[@]} -eq 0 ] && drivers=(go python node java)
failed=0
for d in "${drivers[@]}"; do
  case "$d" in
    go)
      (cd "$here/../.." && go run ./scripts/drivers/go) ;;
    python)
      [ -d "$deps/py/pymysql" ] || pip install -q --target "$deps/py" "pymysql==1.1.1" >/dev/null 2>&1
      PYTHONPATH="$deps/py" python3 "$here/python/drivers.py" ;;
    node)
      [ -d "$deps/node/node_modules/mysql2" ] || (mkdir -p "$deps/node" && cd "$deps/node" && npm init -y >/dev/null && npm install -s mysql2@3.11.0 >/dev/null)
      NODE_PATH="$deps/node/node_modules" node "$here/node/drivers.js" ;;
    java)
      jar="$deps/mysql-connector-j-8.4.0.jar"
      [ -f "$jar" ] || mvn -q dependency:copy -Dartifact=com.mysql:mysql-connector-j:8.4.0 -DoutputDirectory="$deps" >/dev/null 2>&1
      java -cp "$jar" "$here/java/Drivers.java" 2>&1 | grep -v "Picked up JAVA_TOOL_OPTIONS" ;;
    *) echo "unknown driver $d"; false ;;
  esac
  [ "${PIPESTATUS[0]}" -eq 0 ] || failed=1
done
exit $failed
