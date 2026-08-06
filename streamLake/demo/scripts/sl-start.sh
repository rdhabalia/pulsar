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
    # SL_ZK_DIR / SL_BK_DIR point zk + bookie (journal+ledgers) data at your storage disk.
    ZK_DIR="${SL_ZK_DIR:-$PULSAR_HOME/data/zk}"
    BK_DIR="${SL_BK_DIR:-$PULSAR_HOME/data/bk}"
    mkdir -p "$ZK_DIR" "$BK_DIR" "$PULSAR_HOME/logs" "$PULSAR_HOME/data"
    # Heap + direct memory for the single standalone JVM (broker + embedded bookie share it). Direct
    # memory feeds BookKeeper/Netty read+write buffers, so it is set >= heap. Override with SL_HEAP /
    # SL_DIRECT_MEM (e.g. SL_HEAP=8g on a smaller box). PULSAR_MEM is read by bin/pulsar at launch.
    SL_HEAP="${SL_HEAP:-24g}"
    SL_DIRECT_MEM="${SL_DIRECT_MEM:-32g}"
    export PULSAR_MEM="${PULSAR_MEM:--Xms${SL_HEAP} -Xmx${SL_HEAP} -XX:MaxDirectMemorySize=${SL_DIRECT_MEM}}"
    echo "  memory  : PULSAR_MEM='$PULSAR_MEM'"
    # --no-stream-storage / --no-functions-worker: the StreamLake demo does not use the BookKeeper
    # stream storage (table service, port 4181) or Pulsar Functions. Leaving stream storage on makes the
    # embedded bookie register in ZK under /stream/servers/available; after an unclean stop, that stale
    # ephemeral znode blocks the next start ("Failed to initialize a registration state service ...
    # ephemeral znode ... expired"). Disabling it avoids that entirely (and starts faster).
    nohup "$PULSAR_HOME/bin/pulsar" standalone \
      --num-bookies 1 \
      --no-stream-storage \
      --no-functions-worker \
      --zookeeper-dir "$ZK_DIR" \
      --bookkeeper-dir "$BK_DIR" \
      > "$PULSAR_HOME/logs/standalone.out" 2>&1 &
    echo $! > "$PULSAR_HOME/data/standalone.pid"
    echo "PID $(cat "$PULSAR_HOME/data/standalone.pid"); logs: $PULSAR_HOME/logs/standalone.out"
    echo "  zk data : $ZK_DIR"
    echo "  bk data : $BK_DIR"
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
