#!/usr/bin/env bash
# StreamLake demo — build a deployable tar of Pulsar (with the StreamLake fork) for a remote Linux host.
# Run from the repo root (pulsar/pulsar). Produces streamlake-demo.tar.gz containing the Pulsar
# distribution + this demo/ folder.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"   # .../pulsar/pulsar
cd "$REPO_ROOT"

echo "[1/3] Building the Pulsar distribution (this compiles the StreamLake fork)…"
# The server distribution assembly (adjust if your build uses a different module/profile):
mvn -q -pl distribution/server -am install -DskipTests -Dcheckstyle.skip -Dspotbugs.skip -Drat.skip \
  || ./gradlew :distribution:server:assemble -x test 2>/dev/null \
  || { echo "Build the distribution with your normal command, then re-run with SL_DIST=<path-to-unpacked-dist>"; }

DIST="${SL_DIST:-$(ls -d distribution/server/target/apache-pulsar-*/ 2>/dev/null | head -1)}"
if [ -z "${DIST:-}" ] || [ ! -d "$DIST" ]; then
  echo "Could not locate the unpacked distribution. Set SL_DIST=<dir> and re-run." >&2
  exit 1
fi

echo "[2/3] Staging demo assets into the distribution…"
cp -r "$REPO_ROOT/streamLake/demo" "$DIST/streamlake-demo"

echo "[3/3] Packaging streamlake-demo.tar.gz…"
tar -C "$(dirname "$DIST")" -czf "$REPO_ROOT/streamlake-demo.tar.gz" "$(basename "$DIST")"
echo "Done: $REPO_ROOT/streamlake-demo.tar.gz"
echo "Copy to the host:  scp streamlake-demo.tar.gz user@host:/opt/  &&  ssh user@host 'cd /opt && tar xzf streamlake-demo.tar.gz'"
