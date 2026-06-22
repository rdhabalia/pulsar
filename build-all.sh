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
# build-all.sh — build the vendored Apache BookKeeper, then Apache Pulsar,
# so that Pulsar's artifacts are built against the BookKeeper that lives in
# ./bookkeeper (rather than a released BookKeeper from Maven Central).
#
# How it works:
#   1. BookKeeper (./bookkeeper, Maven, version 4.18.0-SNAPSHOT) is built and
#      installed into the local Maven repository (~/.m2/repository).
#   2. Pulsar (this repo, Gradle) resolves BookKeeper from mavenLocal() — see
#      settings.gradle.kts — with the version pinned to 4.18.0-SNAPSHOT in
#      gradle/libs.versions.toml. Building Pulsar then picks up the local build.
#
# Usage:
#   ./build-all.sh                 # build BookKeeper + Pulsar (skips tests)
#   ./build-all.sh --bk-only       # build & install BookKeeper only
#   ./build-all.sh --pulsar-only   # build Pulsar only (assumes BK already installed)
#   ./build-all.sh --with-tests    # do not skip tests
#
# Any extra args after the recognized flags are passed through to the Gradle build.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BOOKKEEPER_DIR="${SCRIPT_DIR}/bookkeeper"

BUILD_BK=true
BUILD_PULSAR=true
SKIP_TESTS=true
EXTRA_ARGS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --bk-only)      BUILD_PULSAR=false ;;
    --pulsar-only)  BUILD_BK=false ;;
    --with-tests)   SKIP_TESTS=false ;;
    -h|--help)
      sed -n '20,40p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
      exit 0 ;;
    *)              EXTRA_ARGS+=("$1") ;;
  esac
  shift
done

log() { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }

# ---------------------------------------------------------------------------
# 1. Build & install BookKeeper into the local Maven repository
# ---------------------------------------------------------------------------
if [[ "${BUILD_BK}" == "true" ]]; then
  if [[ ! -f "${BOOKKEEPER_DIR}/pom.xml" ]]; then
    echo "ERROR: ${BOOKKEEPER_DIR}/pom.xml not found — is the bookkeeper/ directory present?" >&2
    exit 1
  fi
  log "Building Apache BookKeeper (Maven install -> mavenLocal) from ${BOOKKEEPER_DIR}"
  MVN_ARGS=(-B -ntp clean install
    -Dspotbugs.skip=true -Dcheckstyle.skip=true -Drat.skip=true
    -Dlicense.skip=true -Denforcer.skip=true)
  if [[ "${SKIP_TESTS}" == "true" ]]; then
    MVN_ARGS+=(-DskipTests -Dspotless.check.skip=true)
  fi
  ( cd "${BOOKKEEPER_DIR}" && mvn "${MVN_ARGS[@]}" )
  log "BookKeeper 4.18.0-SNAPSHOT installed to local Maven repository"
fi

# ---------------------------------------------------------------------------
# 2. Build Pulsar (Gradle) against the locally-installed BookKeeper
# ---------------------------------------------------------------------------
if [[ "${BUILD_PULSAR}" == "true" ]]; then
  log "Building Apache Pulsar (Gradle) against local BookKeeper"
  GRADLE_ARGS=(clean build)
  if [[ "${SKIP_TESTS}" == "true" ]]; then
    GRADLE_ARGS+=(-x test)
  fi
  if [[ ${#EXTRA_ARGS[@]} -gt 0 ]]; then
    GRADLE_ARGS+=("${EXTRA_ARGS[@]}")
  fi
  ( cd "${SCRIPT_DIR}" && ./gradlew "${GRADLE_ARGS[@]}" )
  log "Pulsar build complete"
fi

log "All requested builds finished successfully"
