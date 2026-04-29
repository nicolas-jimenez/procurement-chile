#!/usr/bin/env bash
#SBATCH --job-name=procchile-oc
#SBATCH --array=0-4                  # one task per year: 2022..2026
#SBATCH --output=/nfs/roberts/project/pi_rp269/nj229/procurement-chile/logs/slurm-%A_%a.out
#SBATCH --error=/nfs/roberts/project/pi_rp269/nj229/procurement-chile/logs/slurm-%A_%a.err
#SBATCH --time=7-00:00:00            # week partition max; ~2.2M orders × 0.3s ≈ 7 days/year
#SBATCH --mem=16G
#SBATCH --cpus-per-task=4
#SBATCH --partition=week
# ─────────────────────────────────────────────────────────────────────────────
# slurm_year_pipeline.sbatch  —  Yale Bouchet HPC
#
# Runs Stage 2 (detail downloads) + parse for ordenes de compra, one year per
# SLURM array task. Stage 1 (order_codes.csv) must be uploaded to Bouchet
# before submitting — see sync instructions below.
#
# Before submitting:
#   rsync -av /Users/martinferraritagle/Dropbox/procurement-chile-db/data/raw/chilecompra/ordenes_compra/manifests/ \
#     bouchet-transfer:/nfs/roberts/project/pi_rp269/nj229/procurement-chile/data/raw/chilecompra/ordenes_compra/manifests/
#
# Submit all 5 years at once (from Bouchet login node, inside repo dir):
#   sbatch sandbox/code/download/slurm_year_pipeline.sbatch
#
# Submit a single year (e.g. 2023, array index 1):
#   sbatch --array=1 sandbox/code/download/slurm_year_pipeline.sbatch
#
# Array index → year mapping:
#   0 → 2022,  1 → 2023,  2 → 2024,  3 → 2025,  4 → 2026
# ─────────────────────────────────────────────────────────────────────────────

set -euo pipefail

# ── Bouchet paths ─────────────────────────────────────────────────────────────
PROJECT_ROOT="/nfs/roberts/project/pi_rp269/nj229/procurement-chile"
REPO_ROOT="${PROJECT_ROOT}/procurement-chile"
VENV="${PROJECT_ROOT}/venv"
export PROCUREMENT_CHILE_DB="$PROJECT_ROOT"

# ── Year mapping ──────────────────────────────────────────────────────────────
YEARS=(2022 2023 2024 2025 2026)
YEAR="${YEARS[$SLURM_ARRAY_TASK_ID]}"

# ── API ticket ────────────────────────────────────────────────────────────────
export MERCADO_PUBLICO_TICKET="${MERCADO_PUBLICO_TICKET:-C9C6EB55-72C8-41BC-BB3E-CA11AA8698B8}"

# 0.25s sleep → ~240 req/min → ~6.4 days per year of 2.2M orders (fits week partition).
# Increase if you still see quota responses; decrease only if the API allows it.
export SLEEP_SECONDS="${SLEEP_SECONDS:-0.25}"
export TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-120}"

# ── Load Bouchet modules + activate venv ─────────────────────────────────────
module purge 2>/dev/null || true
module load R/4.4.2-gfbf-2024a
module load Python/3.12.3-GCCcore-13.3.0
# shellcheck disable=SC1091
source "${VENV}/bin/activate"

export OMP_NUM_THREADS="${SLURM_CPUS_PER_TASK:-4}"
export MKL_NUM_THREADS="${OMP_NUM_THREADS}"
export OPENBLAS_NUM_THREADS="${OMP_NUM_THREADS}"
export PYTHONUNBUFFERED=1

# ── Paths ─────────────────────────────────────────────────────────────────────
SCRIPT_DIR="${REPO_ROOT}/sandbox/code/download"
LOG_DIR="${PROJECT_ROOT}/logs"
mkdir -p "$LOG_DIR"

echo "=================================================================="
echo "  Job            : ${SLURM_JOB_ID}  (array task ${SLURM_ARRAY_TASK_ID} → ${YEAR})"
echo "  Host           : $(hostname)"
echo "  CPUs           : ${SLURM_CPUS_PER_TASK}"
echo "  PROJECT_ROOT   : ${PROJECT_ROOT}"
echo "  REPO_ROOT      : ${REPO_ROOT}"
echo "  Python         : $(which python) ($(python --version 2>&1))"
echo "  R              : $(which Rscript) ($(Rscript --version 2>&1 | head -1))"
echo "  Started        : $(date -Is)"
echo "=================================================================="

# ── Run pipeline for this year ────────────────────────────────────────────────
# No --evict on cluster: Dropbox xattr eviction is Mac-only.
# Delete detail_json/YEAR/ manually after verifying the parquet if space is tight.
bash "$SCRIPT_DIR/run_year_pipeline.sh" "$YEAR" \
    2>&1 | tee "$LOG_DIR/pipeline_${YEAR}_${SLURM_JOB_ID}.log"

echo ""
echo "=================================================================="
echo "  DONE: Year ${YEAR} — $(date -Is)"
echo "=================================================================="
