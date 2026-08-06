#!/usr/bin/env bash
# StreamLake demo — ONE command on the host. It:
#   1. asks for your local storage disk (and the two table sizes),
#   2. configures zk + bookie (journal/ledgers) + broker with no manual edits,
#   3. starts zk + bookie + broker (standalone),
#   4. registers Person + Employee as StreamLake tables,
#   5. ingests the configured amount of data,
#   6. prints each table's on-storage layout (ledgers, rows, event-time range),
#   7. shows the inner-join / group-by / order-by / scan queries to play with.
#
# Non-interactive: set SL_STORAGE_DIR, SL_PERSON_GB, SL_EMP_GB (and optionally SL_NAMESPACE) to skip
# the prompts. Re-run with different/larger sizes any time.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PULSAR_HOME="${PULSAR_HOME:-$(cd "$SCRIPT_DIR/../.." && pwd)}"
export PULSAR_HOME
if [ ! -x "$PULSAR_HOME/bin/pulsar" ]; then
  echo "PULSAR_HOME=$PULSAR_HOME has no bin/pulsar. cd into the unpacked apache-pulsar-<ver> dir, or" >&2
  echo "set PULSAR_HOME, then re-run." >&2
  exit 1
fi
# macOS-created AppleDouble files (._*) break jar/classpath reads on Linux; strip them once up front.
find "$PULSAR_HOME" -name '._*' -delete 2>/dev/null || true

# ---- interactive (or env) config ----
STORAGE_DIR="${SL_STORAGE_DIR:-}"
if [ -z "$STORAGE_DIR" ]; then
  read -r -p "Local disk path for StreamLake storage (zk + bookie journal/ledgers) [/mnt/nvme]: " STORAGE_DIR
  STORAGE_DIR="${STORAGE_DIR:-/mnt/nvme}"
fi
PERSON_GB="${SL_PERSON_GB:-}"
if [ -z "$PERSON_GB" ]; then
  read -r -p "Person table size in GB [500]: " PERSON_GB
  PERSON_GB="${PERSON_GB:-500}"
fi
EMP_GB="${SL_EMP_GB:-}"
if [ -z "$EMP_GB" ]; then
  read -r -p "Employee table size in GB [500]: " EMP_GB
  EMP_GB="${EMP_GB:-500}"
fi
NS="${SL_NAMESPACE:-public/default}"

echo
echo "==> PULSAR_HOME = $PULSAR_HOME"
echo "==> storage     = $STORAGE_DIR   (zk + bookie journal + ledgers)"
echo "==> sizes       = Person ${PERSON_GB}GB, Employee ${EMP_GB}GB"
echo "==> namespace   = $NS"
echo

# ---- 1. configure storage + broker keys (idempotent, no manual edits) ----
SL_JOURNAL_DIR="$STORAGE_DIR/bk/journal" \
SL_LEDGER_DIRS="$STORAGE_DIR/bk/ledgers" \
SL_QUERY_LOCAL_DIR="$STORAGE_DIR/streamlake" \
  "$SCRIPT_DIR/sl-configure.sh"

# ---- 2. start zk + bookie + broker, with all data on the disk ----
SL_ZK_DIR="$STORAGE_DIR/zk" SL_BK_DIR="$STORAGE_DIR/bk" "$SCRIPT_DIR/sl-start.sh" start
echo "==> waiting for the broker to become healthy…"
until "$PULSAR_HOME/bin/pulsar-admin" brokers healthcheck >/dev/null 2>&1; do sleep 2; done
echo "    broker healthy."

# ---- 3. register the two StreamLake tables (spill joins to the big disk, not /tmp) ----
SL_NAMESPACE="$NS" SL_QUERY_LOCAL_DIR="$STORAGE_DIR/streamlake" "$SCRIPT_DIR/sl-register.sh"

# ---- 4. ingest ----
SL_NAMESPACE="$NS" SL_PERSON_GB="$PERSON_GB" SL_EMP_GB="$EMP_GB" "$SCRIPT_DIR/sl-ingest.sh"

# ---- 5. storage layout after ingestion ----
echo
echo "===================== ingestion summary ====================="
SL_NAMESPACE="$NS" "$SCRIPT_DIR/sl-info.sh"

# ---- 6. the queries to play with ----
echo "======================= queries ============================="
SL_NAMESPACE="$NS" "$SCRIPT_DIR/sl-queries.sh"
echo
echo "Stop the server later with:  $SCRIPT_DIR/sl-start.sh stop"
