#!/usr/bin/env bash
# StreamLake demo — build a deployable tar of Pulsar (with the StreamLake fork) for a remote Linux host.
# Run from anywhere; it locates the repo root. Produces streamlake-demo.tar.gz containing the Pulsar
# SERVER distribution (broker + bookie + zk + pulsar-admin, with the streamlake CLI) plus this demo/
# folder (docs + scripts + the ingestion program).
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"   # .../pulsar/pulsar
cd "$REPO_ROOT"

echo "[1/4] Building the Pulsar server distribution (compiles the StreamLake fork)…"
./gradlew :distribution:pulsar-server-distribution:assemble \
  -x test -x checkstyleMain -x checkstyleTest --no-daemon --console=plain

TARBALL="$(ls -t distribution/server/build/distributions/apache-pulsar-*-bin.tar.gz 2>/dev/null | head -1)"
if [ -z "${TARBALL:-}" ] || [ ! -f "$TARBALL" ]; then
  echo "Could not find the server distribution tarball under distribution/server/build/distributions." >&2
  exit 1
fi
echo "      built: $TARBALL"

echo "[2/4] Unpacking the distribution into a work dir…"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
tar -C "$WORK" -xzf "$TARBALL"
DIST_DIR="$(ls -d "$WORK"/apache-pulsar-*/ | head -1)"
[ -n "$DIST_DIR" ] || { echo "unpack failed" >&2; exit 1; }

echo "[3/4] Staging the demo assets (docs + scripts + ingest) into the distribution…"
cp -r "$REPO_ROOT/streamLake/demo" "$DIST_DIR/streamlake-demo"

echo "[4/4] Packaging streamlake-demo.tar.gz…"
tar -C "$WORK" -czf "$REPO_ROOT/streamlake-demo.tar.gz" "$(basename "$DIST_DIR")"
echo "Done: $REPO_ROOT/streamlake-demo.tar.gz"
echo
echo "Ship + unpack + run the whole demo on the host:"
echo "  scp streamlake-demo.tar.gz user@HOST:/opt/"
echo "  ssh user@HOST 'cd /opt && tar xzf streamlake-demo.tar.gz'"
echo "  ssh -t user@HOST 'cd /opt/apache-pulsar-*/ && streamlake-demo/scripts/sl-demo.sh'"
