#!/usr/bin/env bash
# StreamLake demo — register Person + Employee as StreamLake tables on a running server.
# Creates the topics, sets infinite retention (StreamLake data ledgers must never be trimmed), and
# applies the StreamLake table policy (schema + tuning) via the external `pulsar-admin streamlake
# register` command. Idempotent.
#
# Env: PULSAR_HOME (required), SL_NAMESPACE (default public/default), SL_RF (default 1 for one bookie),
#      SL_PI_MAX (page-index rollover), SL_SEG_MAX (segment rollover).
set -euo pipefail
PULSAR_HOME="${PULSAR_HOME:?set PULSAR_HOME to the unpacked Pulsar dir}"
NS="${SL_NAMESPACE:-public/default}"
RF="${SL_RF:-1}"
PI_MAX="${SL_PI_MAX:-1000000}"
SEG_MAX="${SL_SEG_MAX:-200000}"
ADMIN="$PULSAR_HOME/bin/pulsar-admin"

echo "==> setting infinite retention on $NS (StreamLake data must be retained)…"
"$ADMIN" namespaces set-retention "$NS" --size -1 --time -1 || true

for t in Person Employee; do
  echo "==> creating topic persistent://$NS/$t (if absent)…"
  "$ADMIN" topics create "persistent://$NS/$t" 2>/dev/null || true
done

echo "==> registering Person (personId:INT64, name:STRING, age:INT32)…"
"$ADMIN" streamlake register "$NS" Person \
  --schema "personId:INT64,name:STRING,age:INT32" \
  --rf "$RF" --page-index-max-entries "$PI_MAX" --segment-max-entries "$SEG_MAX"

echo "==> registering Employee (empId:INT64, personId:INT64, salary:INT64)…"
"$ADMIN" streamlake register "$NS" Employee \
  --schema "empId:INT64,personId:INT64,salary:INT64" \
  --rf "$RF" --page-index-max-entries "$PI_MAX" --segment-max-entries "$SEG_MAX"

echo "Registered Person + Employee in namespace $NS."
