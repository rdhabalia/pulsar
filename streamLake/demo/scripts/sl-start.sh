#!/usr/bin/env bash
# StreamLake demo — start / stop the standalone (zk + bookie + broker) after sl-configure.sh.
# Usage:  PULSAR_HOME=/opt/pulsar ./sl-start.sh start|stop|status
set -euo pipefail
PULSAR_HOME="${PULSAR_HOME:?set PULSAR_HOME}"
cmd="${1:-start}"

case "$cmd" in
  start)
    echo "Starting Pulsar standalone (zk + bookie + broker)…"
    # --num-bookies 1 for a single-host demo; the fork stores StreamLake page-index/segment/catalog
    # ledgers in the same bookie. For >1 bookie set page-index/segment RF accordingly (see demo.md §7).
    nohup "$PULSAR_HOME/bin/pulsar" standalone \
      --num-bookies 1 \
      --zookeeper-dir "$PULSAR_HOME/data/zk" \
      --bookkeeper-dir "$PULSAR_HOME/data/bk" \
      > "$PULSAR_HOME/logs/standalone.out" 2>&1 &
    echo $! > "$PULSAR_HOME/data/standalone.pid"
    echo "PID $(cat "$PULSAR_HOME/data/standalone.pid"); logs: $PULSAR_HOME/logs/standalone.out"
    echo "Wait for readiness:  until $PULSAR_HOME/bin/pulsar-admin brokers healthcheck; do sleep 2; done"
    ;;
  stop)
    if [ -f "$PULSAR_HOME/data/standalone.pid" ]; then
      kill "$(cat "$PULSAR_HOME/data/standalone.pid")" && rm -f "$PULSAR_HOME/data/standalone.pid"
      echo "Stopped."
    else echo "No pid file."; fi
    ;;
  status)
    "$PULSAR_HOME/bin/pulsar-admin" brokers healthcheck && echo "healthy" || echo "not ready"
    ;;
  *) echo "usage: $0 start|stop|status"; exit 1;;
esac
