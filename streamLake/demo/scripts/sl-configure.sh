#!/usr/bin/env bash
# StreamLake demo — batch-configure a Pulsar standalone (zk + bookie + broker) with NO manual edits.
# Sets the bookie NVMe journal + multi-TB HDD ledger dirs, the StreamLake query-broker local dir, and
# the StreamLake broker defaults. Idempotent: re-running rewrites the managed keys.
#
# Usage:
#   PULSAR_HOME=/opt/pulsar \
#   SL_JOURNAL_DIR=/mnt/nvme/bk/journal \
#   SL_LEDGER_DIRS=/data1/bk/ledgers,/data2/bk/ledgers \
#   SL_QUERY_LOCAL_DIR=/mnt/nvme/streamlake \
#   ./sl-configure.sh
set -euo pipefail

PULSAR_HOME="${PULSAR_HOME:?set PULSAR_HOME to the unpacked Pulsar dir}"
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

mkdir -p "$SL_JOURNAL_DIR" ${SL_LEDGER_DIRS//,/ } "$SL_QUERY_LOCAL_DIR"

# --- bookie storage: NVMe journal + multi-TB HDD ledger dirs ---
for f in "$bkconf" "$conf"; do
  set_key "$f" journalDirectory   "$SL_JOURNAL_DIR"
  set_key "$f" ledgerDirectories  "$SL_LEDGER_DIRS"
  # DbLedgerStorage scales to many ledgers (our page-index/segment ledgers):
  set_key "$f" ledgerStorageClass "org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorage"
done

# --- broker: enable StreamLake query role + local scratch dir + topic-level policies ---
set_key "$conf" systemTopicEnabled            "true"
set_key "$conf" topicLevelPoliciesEnabled     "true"
set_key "$conf" brokerDeleteInactiveTopicsEnabled "false"
set_key "$conf" streamingLakeQueryLocalDir    "$SL_QUERY_LOCAL_DIR"   # (broker config; see demo.md §7)
set_key "$conf" streamingLakeSegmentCacheMaxEntries "$SL_SEGMENT_CACHE"

echo "Configured:"
echo "  journal (NVMe)   : $SL_JOURNAL_DIR"
echo "  ledgers (HDD/TB) : $SL_LEDGER_DIRS"
echo "  query local dir  : $SL_QUERY_LOCAL_DIR"
echo "  standalone.conf  : $conf"
echo "  bookkeeper.conf  : $bkconf"
