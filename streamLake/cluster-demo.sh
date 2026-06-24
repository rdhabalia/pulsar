#!/usr/bin/env bash
#
# Start the Streaming Lake engine (bookie + broker model), produce records to a Person
# topic, consume them, and run two query consumers that write a full-scan file and a
# filtered (departmentId > X AND departmentId < Y AND salary > Z) file.
#
# Usage:
#   ./cluster-demo.sh [numRecords] [deptX] [deptY] [salaryZ] [startDay] [endDay] [outDir]
# Defaults:
#   numRecords=1000 deptX=5 deptY=15 salaryZ=100 startDay=0 endDay=1 outDir=demo-output
#
# startDay/endDay are date-partition bounds as day offsets (records span 5 days: 0..4).
# The filtered query is:
#   date_partition in [startDay, endDay] AND deptId > X AND deptId < Y AND salary > Z
#
# Example:
#   ./cluster-demo.sh 1000 5 15 100 0 1            # query first 2 days
#   ./cluster-demo.sh 5000 8 18 200 1 3 /tmp/sl-out
#
set -euo pipefail
cd "$(dirname "$0")"

NUM=${1:-1000}
DEPT_X=${2:-5}
DEPT_Y=${3:-15}
SALARY_Z=${4:-100}
START_DAY=${5:-0}
END_DAY=${6:-1}
OUT=${7:-demo-output}

echo "==> compiling Streaming Lake engine"
rm -rf out && mkdir -p out
javac -d out $(find src -name '*.java')

echo "==> starting engine + running producer/consumer/query demo"
java -cp out io.streamlake.StreamingLakeClusterDemo "$NUM" "$DEPT_X" "$DEPT_Y" "$SALARY_Z" "$START_DAY" "$END_DAY" "$OUT"

echo ""
echo "==> output files:"
ls -l "$OUT"
echo ""
echo "==> head of filtered output:"
head -8 "$OUT/filtered.txt"
