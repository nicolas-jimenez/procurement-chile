#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Push code (the git repo) from local to Bouchet.
#
# Preferred workflow is git:  git push origin main, then `ssh bouchet 'git pull'`.
# This rsync path exists for fast iteration on uncommitted changes.
#
# Usage:
#   bash code/jobs/sync_to_yale.sh              # use transfer node if available
#   YALE_HOST=bouchet bash code/jobs/sync_to_yale.sh
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"   # code/jobs/ -> repo root
cd "${REPO_ROOT}"

REMOTE_ROOT="${YALE_ROOT:-/nfs/roberts/project/pi_rp269/nj229/procurement-chile}"
REMOTE_REPO="${REMOTE_ROOT}/procurement-chile"

# Pick host: user-supplied > transfer node > login node
if [ -n "${YALE_HOST:-}" ]; then
    REMOTE_HOST="${YALE_HOST}"
elif ssh -o ConnectTimeout=5 -o BatchMode=yes bouchet-transfer "true" 2>/dev/null; then
    REMOTE_HOST="bouchet-transfer"
    echo "(Using transfer node: transfer-bouchet.ycrc.yale.edu)"
elif ssh -o ConnectTimeout=5 -o BatchMode=yes bouchet "true" 2>/dev/null; then
    REMOTE_HOST="bouchet"
    echo "(Transfer node unavailable; using login node)"
else
    echo "ERROR: Cannot reach bouchet-transfer or bouchet."
    echo "Open a multiplexed session first:  ssh bouchet-transfer  (or ssh bouchet)"
    exit 1
fi

echo "=== Syncing ${REPO_ROOT}/ -> ${REMOTE_HOST}:${REMOTE_REPO}/ ==="
rsync -av --delete \
    --exclude ".git/" \
    --exclude "__pycache__/" \
    --exclude ".pytest_cache/" \
    --exclude "*.pyc" \
    --exclude ".DS_Store" \
    --exclude ".env" \
    --exclude "output/" \
    --exclude "outputs/" \
    --exclude "data/" \
    --exclude "venv/" \
    --exclude "logs/" \
    ./ "${REMOTE_HOST}:${REMOTE_REPO}/"

echo ""
echo "Done. To apply changes: ssh bouchet 'cd ${REMOTE_REPO} && git status'"
