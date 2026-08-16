#!/usr/bin/env bash
# =============================================================================
# StreamLake 500GB demo — ONE command: wipe -> configure (2 disks) -> start
# standalone (max query concurrency) -> set retention + register -> ingest.
#
#   Data disk (bookie journal+ledgers+zk) : /grid/x/dfs-data/tmp/test   (SL_XDISK)
#   Broker temporal / join-spill disk     : /grid/y/dfs-data/tmp/test   (SL_YDISK)
#
# Prereq: a JDK 17+ (broker + ingest are Java 17 bytecode).
#   export JAVA_HOME=/path/to/jdk17
#   ./sl-500g.sh
#
# Tunables (env, with defaults):
#   SL_XDISK=/grid/x/dfs-data/tmp/test   SL_YDISK=/grid/y/dfs-data/tmp/test
#   SL_PERSON_GB=500  SL_EMP_GB=500      # per-table target sizes
#   SL_THREADS=16                        # ingest producer threads
#   SL_HEAP=24g  SL_DIRECT_MEM=32g       # standalone JVM (broker+bookie share it)
#   SL_DECODE_CONCURRENCY=0              # 0=auto=min(64,cores*2)=max useful
#   SL_KEEP_DATA=false                   # true = do NOT wipe the disks (resume)
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
export PULSAR_HOME="${PULSAR_HOME:-$(cd "$SCRIPT_DIR/../.." && pwd)}"
: "${JAVA_HOME:?export JAVA_HOME=/path/to/jdk17+  (broker + ingest need Java 17+)}"

XDISK="${SL_XDISK:-/grid/x/dfs-data/tmp/test}"     # bookie journal + ledgers + zk
YDISK="${SL_YDISK:-/grid/y/dfs-data/tmp/test}"     # streamLakeBroker temporal / join spill
PERSON_GB="${SL_PERSON_GB:-500}"
EMP_GB="${SL_EMP_GB:-500}"
THREADS="${SL_THREADS:-16}"
# Ingest-throughput knobs. The broker writes ONE page-index footer per page, synchronized, to a single
# bookie -> that per-topic serialized write is the ceiling. Bigger pages = fewer footer writes = faster
# ingest (trade: coarser pruning granularity). sendTimeout is 0 in the ingest, so raising these only
# adds backpressure, never publish-timeout failures.
ROWS_PER_PAGE="${SL_ROWS_PER_PAGE:-2000}"       # 1000=finest pruning; 2000-4000 = faster ingest
MAX_PENDING="${SL_MAX_PENDING:-4000}"           # in-flight pages per producer (blocks when full)
CLIENT_MEM_MB="${SL_CLIENT_MEM_MB:-2048}"       # ingest client memory limit
export SL_WRITE_CACHE_MB="${SL_WRITE_CACHE_MB:-4096}"   # bookie write cache (absorbs write bursts)
export SL_HEAP="${SL_HEAP:-24g}"
export SL_DIRECT_MEM="${SL_DIRECT_MEM:-32g}"
export SL_DECODE_CONCURRENCY="${SL_DECODE_CONCURRENCY:-0}"
NS="${SL_NAMESPACE:-public/default}"
ADMIN="$PULSAR_HOME/bin/pulsar-admin"

echo "=========================================================================="
echo " PULSAR_HOME = $PULSAR_HOME"
echo " data disk   = $XDISK   (bookie journal+ledgers, zk)"
echo " temporal    = $YDISK   (streamLakeBroker join spill)"
echo " sizes       = Person ${PERSON_GB}GB, Employee ${EMP_GB}GB"
echo " memory      = heap $SL_HEAP / direct $SL_DIRECT_MEM ; decodeConcurrency=$SL_DECODE_CONCURRENCY (0=auto)"
echo "=========================================================================="

# ---- 0. stop any prior standalone, then WIPE both disks (clean state) --------
"$SCRIPT_DIR/sl-start.sh" stop 2>/dev/null || true
sleep 3
if [ "${SL_KEEP_DATA:-false}" != "true" ]; then
  echo "==> wiping $XDISK and $YDISK …"
  rm -rf "$XDISK" "$YDISK"
fi
mkdir -p "$XDISK/zk" "$XDISK/bk" "$YDISK"

# ---- 1. configure: bookie data on X disk, broker temporal/spill on Y disk ----
SL_STORAGE_DIR="$XDISK" SL_QUERY_LOCAL_DIR="$YDISK" "$SCRIPT_DIR/sl-configure.sh"

# ---- 2. start standalone (zk + bookie + broker) on the X disk ----------------
SL_ZK_DIR="$XDISK/zk" SL_BK_DIR="$XDISK/bk" "$SCRIPT_DIR/sl-start.sh" start
echo "==> waiting for the broker to become healthy…"
until "$ADMIN" brokers healthcheck >/dev/null 2>&1; do sleep 2; done
echo "    broker healthy."

# ---- 3. infinite retention + register Person & Employee ----------------------
# (StreamLake data ledgers must NEVER be trimmed, or scans fail with LedgerNotExist.)
SL_NAMESPACE="$NS" "$SCRIPT_DIR/sl-register.sh"

# ---- 4. ingest in the BACKGROUND (500GB takes hours) -------------------------
ING_LOG="$PULSAR_HOME/logs/ingest.log"
echo "==> starting ingestion in background -> $ING_LOG"
SL_NAMESPACE="$NS" SL_PERSON_GB="$PERSON_GB" SL_EMP_GB="$EMP_GB" SL_THREADS="$THREADS" \
  SL_ROWS_PER_PAGE="$ROWS_PER_PAGE" SL_MAX_PENDING="$MAX_PENDING" SL_CLIENT_MEM_MB="$CLIENT_MEM_MB" \
  nohup "$SCRIPT_DIR/sl-ingest.sh" > "$ING_LOG" 2>&1 &
echo "$!" > "$PULSAR_HOME/data/ingest.pid"

cat <<EOF

==========================  SERVER UP + INGESTING  ==========================
 Monitor ingest : tail -f $ING_LOG
 Table sizes    : $ADMIN streamlake info $NS Person   --json
                  $ADMIN streamlake info $NS Employee --json
 Query (once some ledgers are segmented):
   $ADMIN streamlake query $NS \\
     "SELECT p.name, e.salary FROM Person p JOIN Employee e ON p.personId = e.personId \\
      WHERE p.personId BETWEEN 1000000 AND 1001000" --json
 Stop everything: $SCRIPT_DIR/sl-start.sh stop
=============================================================================
EOF
