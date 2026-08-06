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
THREADS="${SL_THREADS:-8}"              # parallel producers (parallelizes Arrow encode + send)
CLIENT_MEM_MB="${SL_CLIENT_MEM_MB:-512}" # pulsar client memory limit (more in-flight = faster)
MAX_PENDING="${SL_MAX_PENDING:-1000}"  # bounded in-flight sends per producer (backpressure; lower if
                                       # the broker can't keep up -> "Message send timed out")
INGEST_XMX="${SL_INGEST_XMX:-4g}"       # ingest JVM heap

OUT="$INGEST_DIR/out"
mkdir -p "$OUT"
# Strip macOS AppleDouble (._*) files that break jar/classpath reads on Linux (no-op if none).
find "$PULSAR_HOME/lib" -name '._*' -delete 2>/dev/null || true

# The client jars are Java 17 bytecode, so we need a JDK 17+ (a bare `javac` may be JDK 8 ->
# "class file has wrong version 61.0, should be 52.0"). Auto-resolve one, in order: $JAVA_HOME, then
# the JDK that backs the `java` on PATH (the broker already runs on 17+), then a bare `javac` if 17+.
JAVAC=""; JAVA=""; JDK_VER=0
_try_jdk() {  # _try_jdk <home-or-empty> ; sets JAVAC/JAVA/JDK_VER if that home's javac is >= 17
  local home="$1" jc jr v m
  jc="${home:+$home/bin/}javac"; jr="${home:+$home/bin/}java"
  command -v "$jc" >/dev/null 2>&1 || return 1
  v="$("$jc" -version 2>&1 | awk 'NR==1{print $2}')"; m="${v%%.*}"
  [ "$m" = "1" ] && m="$(printf '%s' "$v" | cut -d. -f2)"   # 1.8.0 -> 8
  case "$m" in ''|*[!0-9]*) return 1;; esac
  [ "$m" -ge 17 ] || return 1
  JAVAC="$jc"; JAVA="$jr"; JDK_VER="$m"; return 0
}
_jhome_from_path=""
_jbin="$(command -v java 2>/dev/null || true)"
[ -n "$_jbin" ] && _jhome_from_path="$(dirname "$(dirname "$(readlink -f "$_jbin")")")"
_try_jdk "${JAVA_HOME:-}" || _try_jdk "$_jhome_from_path" || _try_jdk "" || {
  echo "ERROR: no JDK 17+ found (the StreamLake client jars are Java 17 bytecode)." >&2
  echo "Install a JDK 17+ or 'export JAVA_HOME=/path/to/jdk17' and re-run." >&2
  exit 1
}
echo "==> compiling StreamLakeIngest with javac (Java $JDK_VER) against $PULSAR_HOME/lib…"
"$JAVAC" -cp "$PULSAR_HOME/lib/*" -d "$OUT" "$INGEST_DIR/StreamLakeIngest.java"

# JVM flags Apache Arrow + Netty need on JDK 17+ for off-heap memory (same set bin/pulsar uses).
# Without java.nio opened, Arrow fails: "sun.misc.Unsafe or java.nio.DirectByteBuffer.<init> not available".
ARROW_OPTS="-Dio.netty.tryReflectionSetAccessible=true \
-Dorg.apache.pulsar.shade.io.netty.tryReflectionSetAccessible=true \
--add-opens=java.base/java.lang=ALL-UNNAMED \
--add-opens=java.base/java.nio=ALL-UNNAMED \
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED"

run() {  # run <table> <target-gb> <start-id>
  local table="$1" gb="$2" start="$3"
  echo "==> ingesting $table to ~${gb}GB (startId=$start, threads=$THREADS)…"
  # shellcheck disable=SC2086
  "$JAVA" -Xmx"$INGEST_XMX" $ARROW_OPTS -cp "$PULSAR_HOME/lib/*:$OUT" StreamLakeIngest \
    --service-url "$SERVICE_URL" --admin-url "$ADMIN_URL" \
    --tenant "$TENANT" --namespace "$NAMESPACE" --table "$table" \
    --target-gb "$gb" --rows-per-page "$ROWS_PER_PAGE" --start-id "$start" \
    --threads "$THREADS" --client-mem-mb "$CLIENT_MEM_MB" --max-pending "$MAX_PENDING"
}

run Person   "$PERSON_GB" "$PERSON_START"
run Employee "$EMP_GB"    "$EMP_START"
echo "Ingestion complete."
