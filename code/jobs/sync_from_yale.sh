#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Pull lightweight outputs from Bouchet back to the local Dropbox.
# Does NOT copy raw data or large intermediate parquets — only the things you
# want to look at (CSVs, JSONs, figures, tables, slurm logs).
#
# Uses the dedicated transfer node when possible. Destination is the local
# Dropbox (OUTPUT_ROOT from config.py) with a dated subdirectory so you don't
# clobber earlier pulls.
#
# Usage:
#   bash code/jobs/sync_from_yale.sh                           # all, today's date
#   bash code/jobs/sync_from_yale.sh 2026-04-21                # all, specific date
#   bash code/jobs/sync_from_yale.sh 2026-04-21 choice_function
#   bash code/jobs/sync_from_yale.sh 2026-04-21 did
#   bash code/jobs/sync_from_yale.sh 2026-04-21 bids
#   bash code/jobs/sync_from_yale.sh 2026-04-21 logs
#   bash code/jobs/sync_from_yale.sh 2026-04-21 figures
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"   # code/jobs/ -> repo root

DATE_SUFFIX="${1:-$(date +%Y-%m-%d)}"
SCOPE="${2:-all}"

REMOTE_ROOT="/nfs/roberts/project/pi_rp269/nj229/procurement-chile"

# Resolve destination: use the local Dropbox (PROCUREMENT_CHILE_DB) so results
# land next to existing output/. If env var unset, fall back to repo-relative.
LOCAL_DB="${PROCUREMENT_CHILE_DB:-}"
if [ -z "${LOCAL_DB}" ] && [ -f "${REPO_ROOT}/.env" ]; then
    LOCAL_DB=$(grep -E '^PROCUREMENT_CHILE_DB=' "${REPO_ROOT}/.env" | head -1 | cut -d= -f2- | tr -d '"')
fi
if [ -z "${LOCAL_DB}" ]; then
    echo "ERROR: PROCUREMENT_CHILE_DB not set and not in .env" >&2
    exit 1
fi

LOCAL_OUTPUT="${LOCAL_DB}/output/yale"
mkdir -p "${LOCAL_OUTPUT}"

# Pick transfer host
if [ -n "${YALE_TRANSFER_HOST:-}" ]; then
    REMOTE_HOST="${YALE_TRANSFER_HOST}"
elif ssh -o ConnectTimeout=5 -o BatchMode=yes bouchet-transfer "true" 2>/dev/null; then
    REMOTE_HOST="bouchet-transfer"
    echo "(Using transfer node: transfer-bouchet.ycrc.yale.edu)"
elif ssh -o ConnectTimeout=5 -o BatchMode=yes bouchet "true" 2>/dev/null; then
    REMOTE_HOST="bouchet"
    echo "(Transfer node unavailable; using login node)"
else
    echo "ERROR: Cannot reach bouchet-transfer or bouchet." >&2
    echo "Open a multiplexed session first:  ssh bouchet-transfer  (or ssh bouchet)" >&2
    exit 1
fi

RSYNC_OPTS=(-avz --human-readable)
INCLUDE_LIGHTWEIGHT=(
    --include="*/"
    --include="*.json"
    --include="*.csv"
    --include="*.md"
    --include="*.tex"
    --include="*.txt"
    --include="*.png"
    --include="*.pdf"
    --include="*.svg"
    --include="*.html"
    --include="*.log"
)
EXCLUDE_ALL=( --exclude="*" )

sync_subdir() {
    local subdir="$1"
    local remote_dir="${REMOTE_ROOT}/output/${subdir}/"
    local local_dir="${LOCAL_OUTPUT}/${subdir}_${DATE_SUFFIX}/"

    echo "=== Syncing output/${subdir} -> ${subdir}_${DATE_SUFFIX}/ ==="
    mkdir -p "${local_dir}"
    rsync "${RSYNC_OPTS[@]}" \
        "${INCLUDE_LIGHTWEIGHT[@]}" \
        "${EXCLUDE_ALL[@]}" \
        "${REMOTE_HOST}:${remote_dir}" \
        "${local_dir}" 2>/dev/null || echo "  (no ${subdir} outputs yet)"
    echo ""
}

sync_all() {
    for sub in choice_function did bids bunching descriptives product_mix simultaneousbids summary_stats diagnostics; do
        sync_subdir "${sub}"
    done
}

sync_logs() {
    echo "=== Syncing slurm logs -> logs_${DATE_SUFFIX}/ ==="
    mkdir -p "${LOCAL_OUTPUT}/logs_${DATE_SUFFIX}"
    rsync "${RSYNC_OPTS[@]}" \
        "${REMOTE_HOST}:${REMOTE_ROOT}/logs/slurm-*.out" \
        "${REMOTE_HOST}:${REMOTE_ROOT}/logs/slurm-*.err" \
        "${LOCAL_OUTPUT}/logs_${DATE_SUFFIX}/" 2>/dev/null || echo "  (no logs found)"
}

sync_figures() {
    echo "=== Syncing all figures -> figures_${DATE_SUFFIX}/ ==="
    mkdir -p "${LOCAL_OUTPUT}/figures_${DATE_SUFFIX}"
    rsync "${RSYNC_OPTS[@]}" \
        --include="*/" \
        --include="*.png" --include="*.pdf" --include="*.svg" \
        --exclude="*" \
        "${REMOTE_HOST}:${REMOTE_ROOT}/output/" \
        "${LOCAL_OUTPUT}/figures_${DATE_SUFFIX}/" 2>/dev/null || echo "  (no figures)"
}

case "${SCOPE}" in
    all)
        sync_all
        echo ""
        sync_logs
        ;;
    logs)       sync_logs ;;
    figures)    sync_figures ;;
    *)          sync_subdir "${SCOPE}" ;;
esac

echo ""
echo "=== Done. Outputs saved to ${LOCAL_OUTPUT}/ with suffix ${DATE_SUFFIX} ==="
