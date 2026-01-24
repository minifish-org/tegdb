#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

"${REPO_ROOT}/ci_precheck.sh"

if ! command -v markdownlint >/dev/null 2>&1; then
    echo "markdownlint is required but not installed."
    echo "Install markdownlint-cli or add it to your PATH before committing."
    exit 1
fi

markdownlint \
    --config "${REPO_ROOT}/.markdownlint.yml" \
    "${REPO_ROOT}/README.md" \
    "${REPO_ROOT}/docs" \
    "${REPO_ROOT}/AGENTS.md"
