#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# streamlake-demo.sh -- run the StreamLake end-to-end demo on an in-process standalone
# cluster (real broker + real BookKeeper bookie, started and stopped automatically).
#
# It creates a StreamLake (columnar) topic with a Person schema (departmentId, salary),
# publishes 1000 records across 10 date partitions, then:
#   1. a normal consumer drains the whole topic           -> $OUT_DIR/all-persons.txt
#   2. a StreamLake predicate query (date range + deptId + salary, using date-partition
#      pruning + bookie PAGE_PRUNE + selective decode)     -> $OUT_DIR/filtered-persons.txt
#
# Usage:
#   ./streamlake-demo.sh [OUTPUT_DIR]      # default OUTPUT_DIR=/tmp/streamlake-out
#
# Prereq: the vendored BookKeeper must be installed to mavenLocal once:
#   ./build-all.sh --bk-only

set -euo pipefail
cd "$(dirname "$0")"

export STREAMLAKE_OUT_DIR="${1:-/tmp/streamlake-out}"
mkdir -p "$STREAMLAKE_OUT_DIR"

echo ">> Starting in-process standalone cluster and running the StreamLake demo..."
echo ">> Output dir: $STREAMLAKE_OUT_DIR"

./gradlew :pulsar-broker:test \
  --tests "org.apache.pulsar.broker.service.streaminglake.StreamLakeClusterDemo" \
  --console=plain

echo
echo ">> Done. Cluster stopped. Output files:"
echo "   $STREAMLAKE_OUT_DIR/all-persons.txt       (every Person, full consumer)"
echo "   $STREAMLAKE_OUT_DIR/filtered-persons.txt  (predicate query result)"
