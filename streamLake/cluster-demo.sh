#!/usr/bin/env bash
#
# Start the Streaming Lake engine (bookie + broker model), produce records to a Person
# topic, consume them, and run two query consumers that write a full-scan file and a
# filtered (departmentId > X AND departmentId < Y AND salary > Z) file.
#
# Usage:
#   ./cluster-demo.sh [numRecords] [deptX] [deptY] [salaryZ] [outDir]
# Defaults:
#   numRecords=1000  deptX=5  deptY=15  salaryZ=100  outDir=demo-output
#
# Example:
#   ./cluster-demo.sh 1000 5 15 100
#   ./cluster-demo.sh 5000 8 18 200 /tmp/sl-out
#
set -euo pipefail
cd "$(dirname "$0")"

NUM=${1:-1000}
DEPT_X=${2:-5}
DEPT_Y=${3:-15}
SALARY_Z=${4:-100}
OUT=${5:-demo-output}

echo "==> compiling Streaming Lake engine"
rm -rf out && mkdir -p out
javac -d out $(find src -name '*.java')

echo "==> starting engine + running producer/consumer/query demo"
java -cp out io.streamlake.StreamingLakeClusterDemo "$NUM" "$DEPT_X" "$DEPT_Y" "$SALARY_Z" "$OUT"

echo ""
echo "==> output files:"
ls -l "$OUT"
echo ""
echo "==> head of filtered output:"
head -8 "$OUT/filtered.txt"
