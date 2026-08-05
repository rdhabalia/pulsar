#!/usr/bin/env bash
# StreamLake demo — compile + run the standalone ingestion program to load Person + Employee to a
# configurable target size (GB). Re-runnable: append more by raising the target and passing a start id.
#
# Env: PULSAR_HOME (required), SL_NAMESPACE (default public/default),
#      SL_PERSON_GB (default 500), SL_EMP_GB (default 500), SL_ROWS_PER_PAGE (default 1000),
#      SL_PERSON_START / SL_EMP_START (default 0, for re-runs), SL_SERVICE_URL, SL_ADMIN_URL.
set -euo pipefail
PULSAR_HOME="${PULSAR_HOME:?set PULSAR_HOME to the unpacked Pulsar dir}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
INGEST_DIR="$(cd "$SCRIPT_DIR/../ingest" && pwd)"

NS="${SL_NAMESPACE:-public/default}"
TENANT="${NS%%/*}"
NAMESPACE="${NS##*/}"
SERVICE_URL="${SL_SERVICE_URL:-pulsar://localhost:6650}"
ADMIN_URL="${SL_ADMIN_URL:-http://localhost:8080}"
PERSON_GB="${SL_PERSON_GB:-500}"
EMP_GB="${SL_EMP_GB:-500}"
ROWS_PER_PAGE="${SL_ROWS_PER_PAGE:-1000}"
PERSON_START="${SL_PERSON_START:-0}"
EMP_START="${SL_EMP_START:-0}"

OUT="$INGEST_DIR/out"
mkdir -p "$OUT"
# Strip macOS AppleDouble (._*) files that break jar/classpath reads on Linux (no-op if none).
find "$PULSAR_HOME/lib" -name '._*' -delete 2>/dev/null || true

# The client jars are Java 17 bytecode. Prefer JAVA_HOME (the JDK the broker uses), NOT a bare `javac`
# on PATH which may be an older JDK 8 (-> "class file has wrong version 61.0, should be 52.0").
JAVAC="${JAVA_HOME:+$JAVA_HOME/bin/}javac"
JAVA="${JAVA_HOME:+$JAVA_HOME/bin/}java"
vstr="$("$JAVAC" -version 2>&1 | awk 'NR==1{print $2}')"   # e.g. 17.0.5  or  1.8.0_301
jver="${vstr%%.*}"
[ "$jver" = "1" ] && jver="$(printf '%s' "$vstr" | cut -d. -f2)"   # 1.8 -> 8
case "$jver" in ''|*[!0-9]*) jver=0;; esac
if [ "$jver" -lt 17 ]; then
  echo "ERROR: '$JAVAC' is Java ${vstr:-unknown}; the StreamLake client needs JDK 17+." >&2
  echo "Point JAVA_HOME at a JDK 17+ (the one the broker uses) and re-run, e.g.:" >&2
  echo "  export JAVA_HOME=\"\$(dirname \$(dirname \$(readlink -f \$(command -v java))))\"" >&2
  echo "  $0" >&2
  exit 1
fi
echo "==> compiling StreamLakeIngest with javac (Java $jver) against $PULSAR_HOME/lib…"
"$JAVAC" -cp "$PULSAR_HOME/lib/*" -d "$OUT" "$INGEST_DIR/StreamLakeIngest.java"

run() {  # run <table> <target-gb> <start-id>
  local table="$1" gb="$2" start="$3"
  echo "==> ingesting $table to ~${gb}GB (startId=$start)…"
  java -Xmx2g -cp "$PULSAR_HOME/lib/*:$OUT" StreamLakeIngest \
    --service-url "$SERVICE_URL" --admin-url "$ADMIN_URL" \
    --tenant "$TENANT" --namespace "$NAMESPACE" --table "$table" \
    --target-gb "$gb" --rows-per-page "$ROWS_PER_PAGE" --start-id "$start"
}

run Person   "$PERSON_GB" "$PERSON_START"
run Employee "$EMP_GB"    "$EMP_START"
echo "Ingestion complete."
