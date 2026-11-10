#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

# Builds and tests the sample statistics provider plugin (provider/) against the Presto version of the sibling
# ../presto checkout, with Maven in a container. Presto SNAPSHOT artifacts resolve from the Maven repository that
# presto/scripts/build_presto_java_package.sh fills. The plugin directory is provider/target/plugin.
#
# Environment:
#   PRESTO_DIR             Presto checkout (default: <repo root>/../presto)
#   PRESTO_JAVA_VERSION    Presto artifact version (default: the version in $PRESTO_DIR/pom.xml)
#   MVN_REPOSITORY         Maven repository (default: presto/scripts/.mvn_cache/repository)
#   MAVEN_ARGS             extra Maven arguments (e.g. -DskipTests)
#   DOCKER_RUN_ARGS        docker run options (default: --rm)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(git -C "${SCRIPT_DIR}" rev-parse --show-toplevel)"
PRESTO_DIR="${PRESTO_DIR:-${REPO_ROOT}/../presto}"
MVN_REPOSITORY="${MVN_REPOSITORY:-${REPO_ROOT}/presto/scripts/.mvn_cache/repository}"

if [[ -z "${PRESTO_JAVA_VERSION:-}" ]]; then
  PRESTO_JAVA_VERSION=$(python3 -c '
import sys, xml.etree.ElementTree as ElementTree
root = ElementTree.parse(sys.argv[1]).getroot()
print(root.find("{http://maven.apache.org/POM/4.0.0}version").text)' "${PRESTO_DIR}/pom.xml")
fi
echo "Building the sample statistics provider against Presto ${PRESTO_JAVA_VERSION}"

mkdir -p "${MVN_REPOSITORY}"
# shellcheck disable=SC2086
docker run ${DOCKER_RUN_ARGS:---rm} \
  --user "$(id -u):$(id -g)" \
  -e HOME=/tmp \
  -v "${SCRIPT_DIR}/provider:/provider" \
  -v "${MVN_REPOSITORY}:/m2" \
  -w /provider \
  maven:3.9-eclipse-temurin-17 \
  mvn -B --no-transfer-progress package \
    -Dmaven.repo.local=/m2 \
    -Dpresto.version="${PRESTO_JAVA_VERSION}" \
    ${MAVEN_ARGS:-}

echo "Plugin directory: ${SCRIPT_DIR}/provider/target/plugin"
ls "${SCRIPT_DIR}/provider/target/plugin"
