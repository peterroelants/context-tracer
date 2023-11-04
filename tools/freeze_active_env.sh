#!/usr/bin/env bash

# This script exports the current local active conda environment to a file

ROOT_PATH=$(dirname "$(dirname "$(readlink -f "$0")")")  # Root directory path
cd "${ROOT_PATH}"

set -eu

EXPORT_PATH="${ROOT_PATH}/env/frozen/env_local.frozen.yml"
echo -e "\nExporting environment to ${EXPORT_PATH}"

conda env export --no-builds | grep -v "^prefix: " > "${EXPORT_PATH}"
