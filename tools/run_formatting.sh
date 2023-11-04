#!/usr/bin/env bash
# Run with `./tools/run_format.sh`

ROOT_PATH=$(dirname "$(dirname "$(readlink -f "$0")")")  # Root directory path
cd "${ROOT_PATH}"

set -eu

echo -e "\nRun ruff code formatting"
ruff check \
    --respect-gitignore \
    --fix-only \
    --show-fixes \
    --exit-zero \
    "${ROOT_PATH}/src" \
    "${ROOT_PATH}/tests" \
    "${ROOT_PATH}/notebooks"

echo -e "\nRun Black code formatting"
black \
    "${ROOT_PATH}/src" \
    "${ROOT_PATH}/tests" \
    "${ROOT_PATH}"/notebooks/**/*.ipynb
