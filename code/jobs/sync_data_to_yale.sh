#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Push cleaned data from the local Dropbox to Bouchet.
# This is the script to run after updating data/clean locally.
#
# NOT --delete by default (adds/overwrites only). Pass DELETE=1 for mirror.
#
# Usage:
#   bash code/jobs/sync_data_to_yale.sh                 # main datasets only
#   bash code/jobs/sync_data_to_yale.sh all             # everything in data/
#   DELETE=1 bash code/jobs/sync_data_to_yale.sh        # mirror (danger)
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

SCOPE="${1:-main}"

# Resolve local Dropbox root
LOCAL_DB="${PROCUREMENT_CHILE_DB:-}"
if [ -z "${LOCAL_DB}" ]; then
    ENV_FILE="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/.env"   # code/jobs/ -> repo root/.env
    [ -f "${ENV_FILE}" ] && LOCAL_DB=$(grep -E '^PROCUREMENT_CHILE_DB=' "${ENV_FILE}" | head -1 | cut -d= -f2- | tr -d '"')
fi
if [ -z "${LOCAL_DB}" ]; then
    echo "ERROR: PROCUREMENT_CHILE_DB not set." >&2
    exit 1
fi

REMOTE_ROOT="/nfs/roberts/project/pi_rp269/nj229/procurement-chile"

if [ -n "${YALE_TRANSFER_HOST:-}" ]; then
    REMOTE_HOST="${YALE_TRANSFER_HOST}"
elif ssh -o ConnectTimeout=5 -o BatchMode=yes bouchet-transfer "true" 2>/dev/null; then
    REMOTE_HOST="bouchet-transfer"
    echo "(Using transfer node)"
else
    REMOTE_HOST="bouchet"
    echo "(Falling back to login node — slower)"
fi

RSYNC_OPTS=(-av --human-readable --partial --inplace --stats)
[ "${DELETE:-0}" = "1" ] && RSYNC_OPTS+=(--delete)

if [ "${SCOPE}" = "main" ]; then
    echo "=== Syncing main datasets to ${REMOTE_HOST}:${REMOTE_ROOT}/data/ ==="
    rsync "${RSYNC_OPTS[@]}" \
        --include='clean/' \
        --include='clean/combined_sii_merged_filtered.parquet' \
        --include='clean/combined_panel.parquet' \
        --include='clean/chilecompra_panel.parquet' \
        --include='clean/compra_agil_panel.parquet' \
        --include='clean/licitaciones_sii_merged.parquet' \
        --include='clean/compra_agil_sii_merged.parquet' \
        --include='clean/comunas_centroids.csv' \
        --include='clean/comuna_distance_matrix.csv' \
        --include='clean/comuna_distances_long.csv' \
        --include='clean/rut_unidad_sector_crosswalk.parquet' \
        --include='clean/rut_unidad_sector_crosswalk.csv' \
        --include='raw/' \
        --include='raw/other/' \
        --include='raw/other/utm_clp_2022_2025.csv' \
        --exclude='*' \
        "${LOCAL_DB}/data/" "${REMOTE_HOST}:${REMOTE_ROOT}/data/"
else
    echo "=== Syncing EVERYTHING in data/ to ${REMOTE_HOST}:${REMOTE_ROOT}/data/ ==="
    rsync "${RSYNC_OPTS[@]}" \
        "${LOCAL_DB}/data/" "${REMOTE_HOST}:${REMOTE_ROOT}/data/"
fi

echo ""
echo "=== Done ==="
