#!/usr/bin/env bash
# Compile and run the Streaming Lake end-to-end test with zero external dependencies.
set -euo pipefail
cd "$(dirname "$0")"
rm -rf out && mkdir -p out
echo "==> compiling"
javac -d out $(find src -name '*.java')
echo "==> running end-to-end test"
java -cp out io.streamlake.StreamingLakeEndToEndTest
