#!/usr/bin/env bash
# StreamLake demo — print each table's on-storage layout after ingestion: data-ledger counts
# (total / segmented / open), the catalog / page-index / segment ledgers, ingested rows and the
# event-time range. Uses the external `pulsar-admin streamlake info` command.
#
# Env: PULSAR_HOME (required), SL_NAMESPACE (default public/default).
set -euo pipefail
PULSAR_HOME="${PULSAR_HOME:?set PULSAR_HOME to the unpacked Pulsar dir}"
NS="${SL_NAMESPACE:-public/default}"
ADMIN="$PULSAR_HOME/bin/pulsar-admin"

for t in Person Employee; do
  echo "================ $t ================"
  "$ADMIN" streamlake info "$NS" "$t"
  echo
  echo "  (data-ledger chain, from the managed ledger:)"
  "$ADMIN" topics stats-internal "persistent://$NS/$t" \
    | grep -E '"ledgerId"|"entries"|"size"' | head -30 || true
  echo
done
