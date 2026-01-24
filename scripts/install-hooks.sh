#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HOOKS_DIR="${REPO_ROOT}/.git/hooks"
HOOK_TARGET="${REPO_ROOT}/scripts/pre-commit.sh"
HOOK_LINK="${HOOKS_DIR}/pre-commit"

if [ ! -d "${HOOKS_DIR}" ]; then
    echo "Git hooks directory not found. Is this a git repository?"
    exit 1
fi

if [ ! -f "${HOOK_TARGET}" ]; then
    echo "Missing ${HOOK_TARGET}."
    exit 1
fi

if [ -e "${HOOK_LINK}" ] && [ ! -L "${HOOK_LINK}" ]; then
    echo "${HOOK_LINK} already exists and is not a symlink."
    echo "Remove it or replace it manually."
    exit 1
fi

ln -sf "${HOOK_TARGET}" "${HOOK_LINK}"
chmod +x "${HOOK_TARGET}"
chmod +x "${HOOK_LINK}"

echo "Installed pre-commit hook at ${HOOK_LINK}"
