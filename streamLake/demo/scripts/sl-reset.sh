#!/usr/bin/env bash
# StreamLake demo — SAFE full reset in one command: stop the server + any load generator, wait for the
# processes to exit (so files are closed), then wipe BOTH the data (bookie ledgers/journal + zk) AND the
# standalone metadata ($PULSAR_HOME/data) so the next start is a clean, empty cluster.
#
# Prompts before deleting; pass --yes or SL_YES=1 to skip. Needs the SAME storage disk you started with.
#
# Usage:
#   PULSAR_HOME=/opt/apache-pulsar-<ver> SL_STORAGE_DIR=/mnt/nvme ./sl-reset.sh [--yes]
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PULSAR_HOME="${PULSAR_HOME:-$(cd "$SCRIPT_DIR/../.." && pwd)}"
export PULSAR_HOME
[ -x "$PULSAR_HOME/bin/pulsar" ] || { echo "PULSAR_HOME=$PULSAR_HOME has no bin/pulsar." >&2; exit 1; }

YES=0
[ "${1:-}" = "--yes" ] && YES=1
[ "${SL_YES:-}" = "1" ] && YES=1

STORAGE_DIR="${SL_STORAGE_DIR:-}"
if [ -z "$STORAGE_DIR" ]; then
  read -r -p "Storage disk you configured (zk + bookie data) [/mnt/nvme]: " STORAGE_DIR
  STORAGE_DIR="${STORAGE_DIR:-/mnt/nvme}"
fi

# --- 1. stop the broker (via its pid file) ---
echo "==> stopping the server…"
"$SCRIPT_DIR/sl-start.sh" stop >/dev/null 2>&1 || true

# --- 2. stop any leftover load generator / standalone (by PID; TERM, then KILL) ---
_pids() { ps -eo pid=,args= 2>/dev/null | grep -Ei 'StreamLakeIngest|PulsarStandalone' \
            | grep -v grep | awk '{print $1}'; }
echo "==> ensuring no StreamLake/standalone processes remain…"
for sig in TERM TERM KILL; do
  pids="$(_pids || true)"
  [ -z "$pids" ] && break
  for pid in $pids; do echo "    kill -$sig $pid"; kill "-$sig" "$pid" 2>/dev/null || true; done
  sleep 2
done
if [ -n "$(_pids || true)" ]; then
  echo "WARNING: processes still running: $(_pids | tr '\n' ' '). Kill them, then re-run." >&2
  exit 1
fi

# --- 3. compute wipe targets: the storage dir's subdirs + $PULSAR_HOME/data + configured bookie dirs ---
targets=("$STORAGE_DIR/zk" "$STORAGE_DIR/bk" "$STORAGE_DIR/streamlake" "$PULSAR_HOME/data")
bkconf="$PULSAR_HOME/conf/bookkeeper.conf"
if [ -f "$bkconf" ]; then
  jd="$(grep -E '^journalDirectory=' "$bkconf" | tail -1 | cut -d= -f2- || true)"
  ld="$(grep -E '^ledgerDirectories=' "$bkconf" | tail -1 | cut -d= -f2- || true)"
  [ -n "$jd" ] && targets+=("$jd")
  if [ -n "$ld" ]; then
    IFS=',' read -ra _lds <<< "$ld"
    for d in "${_lds[@]}"; do targets+=("$d"); done
  fi
fi

# refuse to delete dangerous paths (must be absolute with >= 2 path segments, never "/" or $HOME)
safe_to_delete() {
  local p="$1" slashes
  [ -n "$p" ] || return 1
  [ "${p#/}" != "$p" ] || return 1
  [ "$p" != "/" ] && [ "$p" != "$HOME" ] || return 1
  slashes="${p//[!\/]/}"
  [ "${#slashes}" -ge 2 ] || return 1
  return 0
}

echo "==> the following will be permanently deleted:"
del=()
for t in "${targets[@]}"; do
  if safe_to_delete "$t"; then echo "    $t"; del+=("$t"); else echo "    (skipping unsafe path: '$t')"; fi
done

# --- 4. confirm + wipe ---
if [ "$YES" != "1" ]; then
  read -r -p "Proceed? This is IRREVERSIBLE. [y/N]: " ans
  case "$ans" in y|Y|yes|YES) ;; *) echo "aborted."; exit 1;; esac
fi
for t in "${del[@]}"; do rm -rf "$t"; done
echo "Reset complete — cluster is empty. Start fresh with:"
echo "  SL_STORAGE_DIR=$STORAGE_DIR streamlake-demo/scripts/sl-demo.sh"
