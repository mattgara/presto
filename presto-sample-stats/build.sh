#!/bin/bash

# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
# SPDX-License-Identifier: Apache-2.0

# Builds and tests the plugin against a Presto version, with Maven in a container. The plugin directory to mount
# into the coordinator is target/plugin.
#
# Environment:
#   PRESTO_VERSION      Presto artifact version to build against (required unless PRESTO_DIR is set)
#   PRESTO_DIR          a Presto checkout whose pom.xml version is used when PRESTO_VERSION is not set
#   MVN_REPOSITORY      Maven repository; Presto -SNAPSHOT versions resolve only from a repository that a Presto
#                       build filled (default: ~/.m2/repository)
#   MAVEN_ARGS          extra Maven arguments (e.g. -DskipTests)
#   DOCKER_RUN_ARGS     docker run options (default: --rm)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MVN_REPOSITORY="${MVN_REPOSITORY:-${HOME}/.m2/repository}"

if [[ -z "${PRESTO_VERSION:-}" ]]; then
  [[ -n "${PRESTO_DIR:-}" ]] || { echo "ERROR: set PRESTO_VERSION or PRESTO_DIR" >&2; exit 1; }
  PRESTO_VERSION=$(python3 -c '
import sys, xml.etree.ElementTree as ElementTree
root = ElementTree.parse(sys.argv[1]).getroot()
print(root.find("{http://maven.apache.org/POM/4.0.0}version").text)' "${PRESTO_DIR}/pom.xml")
fi
echo "Building the sample statistics provider against Presto ${PRESTO_VERSION}"

mkdir -p "${MVN_REPOSITORY}"
docker run ${DOCKER_RUN_ARGS:---rm} \
  --user "$(id -u):$(id -g)" \
  -e HOME=/tmp \
  -v "${SCRIPT_DIR}:/provider" \
  -v "${MVN_REPOSITORY}:/m2" \
  -w /provider \
  maven:3.9-eclipse-temurin-17 \
  mvn -B --no-transfer-progress package \
    -Dmaven.repo.local=/m2 \
    -Dpresto.version="${PRESTO_VERSION}" \
    ${MAVEN_ARGS:-}

echo "Plugin directory: ${SCRIPT_DIR}/target/plugin"
ls "${SCRIPT_DIR}/target/plugin"
