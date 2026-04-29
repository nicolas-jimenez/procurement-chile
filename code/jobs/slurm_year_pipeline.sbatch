#!/usr/bin/env bash
#SBATCH --job-name=procchile-oc-year
#SBATCH --array=0-4
#SBATCH --output=/nfs/roberts/project/pi_rp269/nj229/procurement-chile/logs/slurm-%A_%a.out
#SBATCH --error=/nfs/roberts/project/pi_rp269/nj229/procurement-chile/logs/slurm-%A_%a.err
#SBATCH --time=7-00:00:00
#SBATCH --cpus-per-task=4
#SBATCH --mem=16G
#SBATCH --partition=week
# ─────────────────────────────────────────────────────────────────────────────
# Download ordenes de compra detail JSONs + parse to parquet, one year per
# SLURM array task. Requires Stage 1 manifest (order_codes.csv) to be present
# in data/raw/chilecompra/ordenes_compra/manifests/ before submitting.
#
# Array index → year:  0=2022  1=2023  2=2024  3=2025  4=2026
#
# Upload Stage 1 manifest from Mac before submitting:
#   rsync -av data/raw/chilecompra/ordenes_compra/manifests/ \
#     bouchet-transfer:${PROJECT_ROOT}/data/raw/chilecompra/ordenes_compra/manifests/
#
# Usage (all 5 years in parallel):
#   sbatch code/jobs/slurm_year_pipeline.sbatch
#
# Usage (single year, e.g. 2023):
#   sbatch --array=1 code/jobs/slurm_year_pipeline.sbatch
#
# Requires:
#   - MERCADO_PUBLICO_TICKET set in the environment or passed at submit time:
#       sbatch --export=ALL,MERCADO_PUBLICO_TICKET=... code/jobs/slurm_year_pipeline.sbatch
# ─────────────────────────────────────────────────────────────────────────────
source "$(dirname "${BASH_SOURCE[0]:-$0}")/_preamble.sh"

module load R/4.4.2-gfbf-2024a

if [[ -z "${MERCADO_PUBLICO_TICKET:-}" && -z "${CHILECOMPRA_API_TICKET:-}" ]]; then
    echo "Missing API ticket. Set MERCADO_PUBLICO_TICKET or CHILECOMPRA_API_TICKET." >&2
    exit 2
fi

# ── Year mapping ──────────────────────────────────────────────────────────────
YEARS=(2022 2023 2024 2025 2026)
YEAR="${YEARS[$SLURM_ARRAY_TASK_ID]}"

echo "R                  : $(which Rscript) ($(Rscript --version 2>&1 | head -n 1))"
echo "Year               : ${YEAR}  (array task ${SLURM_ARRAY_TASK_ID})"
echo ""

# ── Step 1: download detail JSONs for this year ───────────────────────────────
bash code/download/run_details_by_year.sh "${YEAR}"

# ── Step 2: parse JSONs → parquet ─────────────────────────────────────────────
python code/download/parse_details_to_parquet.py "${YEAR}"
