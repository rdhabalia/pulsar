#!/usr/bin/env bash
# StreamLake demo — batch-configure a Pulsar standalone (zk + bookie + broker) with NO manual edits.
# Sets the bookie NVMe journal + multi-TB HDD ledger dirs, the StreamLake query-broker local dir, and
# the StreamLake broker defaults. Idempotent: re-running rewrites the managed keys.
#
# Usage (either give SL_STORAGE_DIR and let the dirs derive, or set each dir explicitly):
#   PULSAR_HOME=/opt/pulsar SL_STORAGE_DIR=/grid/x/dfs-data/tmp/test ./sl-configure.sh
# or
#   PULSAR_HOME=/opt/pulsar \
#   SL_JOURNAL_DIR=/mnt/nvme/bk/journal \
#   SL_LEDGER_DIRS=/data1/bk/ledgers,/data2/bk/ledgers \
#   SL_QUERY_LOCAL_DIR=/mnt/nvme/streamlake \
#   ./sl-configure.sh
set -euo pipefail

PULSAR_HOME="${PULSAR_HOME:?set PULSAR_HOME to the unpacked Pulsar dir}"
# One-var convenience (matches sl-demo.sh): derive the three dirs from SL_STORAGE_DIR unless each is set.
SL_STORAGE_DIR="${SL_STORAGE_DIR:-}"
if [ -n "$SL_STORAGE_DIR" ]; then
  SL_JOURNAL_DIR="${SL_JOURNAL_DIR:-$SL_STORAGE_DIR/bk/journal}"
  SL_LEDGER_DIRS="${SL_LEDGER_DIRS:-$SL_STORAGE_DIR/bk/ledgers}"
  SL_QUERY_LOCAL_DIR="${SL_QUERY_LOCAL_DIR:-$SL_STORAGE_DIR/streamlake}"
fi
SL_JOURNAL_DIR="${SL_JOURNAL_DIR:-/mnt/nvme/bk/journal}"      # small, fast NVMe (write-ahead log)
SL_LEDGER_DIRS="${SL_LEDGER_DIRS:-/data/bk/ledgers}"          # large, cheap HDD (multi-TB bulk data)
SL_QUERY_LOCAL_DIR="${SL_QUERY_LOCAL_DIR:-/mnt/nvme/streamlake}"  # query-broker spill/scratch (NVMe)
SL_SEGMENT_CACHE="${SL_SEGMENT_CACHE:-512}"

conf="$PULSAR_HOME/conf/standalone.conf"
bkconf="$PULSAR_HOME/conf/bookkeeper.conf"

set_key() {  # set_key <file> <key> <value>  — replaces or appends "key=value"
  local file="$1" key="$2" val="$3"
  mkdir -p "$(dirname "$file")"
  if grep -qE "^[# ]*${key}=" "$file"; then
    # portable in-place edit (BSD/GNU sed)
    sed -i.bak -E "s|^[# ]*${key}=.*|${key}=${val}|" "$file" && rm -f "${file}.bak"
  else
    printf '\n%s=%s\n' "$key" "$val" >> "$file"
  fi
}

# Fail LOUDLY (with guidance) instead of silently mis-pointing the bookie if the storage dirs are not
# creatable -- e.g. SL_STORAGE_DIR was unset so these fell back to the /mnt/nvme /data defaults.
if ! mkdir -p "$SL_JOURNAL_DIR" ${SL_LEDGER_DIRS//,/ } "$SL_QUERY_LOCAL_DIR" 2>/dev/null; then
  echo "ERROR: cannot create the storage dirs:" >&2
  echo "         journal : $SL_JOURNAL_DIR" >&2
  echo "         ledgers : $SL_LEDGER_DIRS" >&2
  echo "         query   : $SL_QUERY_LOCAL_DIR" >&2
  echo "       Set SL_STORAGE_DIR=/your/data/disk (or SL_JOURNAL_DIR / SL_LEDGER_DIRS /" >&2
  echo "       SL_QUERY_LOCAL_DIR) to a writable path and re-run. (The values above are the" >&2
  echo "       built-in defaults, used because nothing was provided.)" >&2
  exit 1
fi

# --- bookie storage: NVMe journal + multi-TB HDD ledger dirs ---
for f in "$bkconf" "$conf"; do
  set_key "$f" journalDirectory   "$SL_JOURNAL_DIR"
  set_key "$f" ledgerDirectories  "$SL_LEDGER_DIRS"
  # DbLedgerStorage scales to many ledgers (our page-index/segment ledgers):
  set_key "$f" ledgerStorageClass "org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorage"
  # Faster ingest for the demo (single bookie on NVMe): skip per-write journal fsync + bigger caches.
  # journalSyncData=false trades some crash-durability for a large throughput gain -- fine for a demo,
  # NOT for production. Set SL_JOURNAL_SYNC=true to keep fsync.
  set_key "$f" journalSyncData                    "${SL_JOURNAL_SYNC:-false}"
  set_key "$f" dbStorage_writeCacheMaxSizeMb      "${SL_WRITE_CACHE_MB:-1024}"
  set_key "$f" dbStorage_readAheadCacheMaxSizeMb  "256"
  # Disable bookie entry-log GC/compaction for the demo. StreamLake runs with infinite retention and
  # never deletes ledgers, so compaction has nothing to reclaim -- but its GarbageCollectorThread still
  # scans every entry log and competes for disk I/O with query reads, which on a single bookie stalls
  # topic load / queries ("GarbageCollectorThread ... Extracted entry log meta" while a query times out).
  # 0 disables each. Set SL_DISABLE_BK_GC=false to restore defaults.
  if [ "${SL_DISABLE_BK_GC:-true}" = "true" ]; then
    set_key "$f" minorCompactionInterval "0"
    set_key "$f" majorCompactionInterval "0"
    set_key "$f" gcWaitTime              "86400000"
  fi
done

# --- BookKeeper client read headroom on a single busy bookie ---
# StreamLake opens read many small metadata entries (page-index footers, catalog) at topic load. Give the
# bookie client a longer op timeout so a bookie briefly busy (e.g. startup entry-log index rebuild after
# an unclean stop) does not trip the default 30s and fail the topic load with "Bookie operation timeout".
# (The broker also now reads these in bounded batches so it never floods the bookie in the first place.)
set_key "$conf" bookkeeperClientTimeoutInSeconds "${SL_BK_CLIENT_TIMEOUT:-120}"

# --- data-ledger sizing: keep StreamLake "row groups" bounded ---
# A StreamLake data ledger is a row group; its per-page column stats live in one segment whose per-column
# array collapses to a coarse whole-segment stat once it exceeds segmentColumnMaxBytes (2MB). By default a
# managed ledger will not roll until managedLedgerMinLedgerRolloverTimeMinutes (10) even after hitting
# managedLedgerMaxEntriesPerLedger (50000); under fast ingest that makes ~300k-page ledgers whose key
# column (e.g. personId INT64 -> ~5MB) COLLAPSES, so every query must read that ledger's whole page-index
# range (GBs of footers) instead of pruning per page. Rolling at 50k pages keeps each column's stats
# (~800KB) under the cap -> no collapse -> queries prune to a handful of pages. SL_LEDGER_ROLL_MINUTES
# overrides (default 0 = roll as soon as the entry/size cap is hit).
set_key "$conf" managedLedgerMinLedgerRolloverTimeMinutes "${SL_LEDGER_ROLL_MINUTES:-0}"

# --- broker: enable topic-level policies + system topics (StreamLake tuning is TOPIC-POLICY level,
#     applied per topic via StreamingLakeConfig -- not broker-global keys; see demo.md §7) ---
set_key "$conf" systemTopicEnabled            "true"
set_key "$conf" topicLevelPoliciesEnabled     "true"
set_key "$conf" brokerDeleteInactiveTopicsEnabled "false"
# StreamLake hash-join spill dir (server-side): send join partition/spill files to the big local disk
# instead of the JVM temp dir (/tmp, often small or RAM-backed -> "No space left on device" on large
# joins). The broker creates it if missing. This is a broker ServiceConfiguration key, applied here.
set_key "$conf" streamLakeJoinSpillDir        "$SL_QUERY_LOCAL_DIR"
# StreamLake query parallelism: number of parallel Arrow-decode workers per scan / per join side.
# 0 = auto = min(64, availableProcessors x 2) -- the measured sweet spot (1 GB scan: ~1.7x at cores x 2;
# oversubscribing beyond cores x 2 regresses). SL_DECODE_CONCURRENCY overrides (e.g. 64 to pin the cap).
set_key "$conf" streamLakeQueryDecodeConcurrency "${SL_DECODE_CONCURRENCY:-0}"

echo "Configured:"
echo "  journal (NVMe)   : $SL_JOURNAL_DIR"
echo "  ledgers (HDD/TB) : $SL_LEDGER_DIRS"
echo "  query local dir  : $SL_QUERY_LOCAL_DIR"
echo "  standalone.conf  : $conf"
echo "  bookkeeper.conf  : $bkconf"
