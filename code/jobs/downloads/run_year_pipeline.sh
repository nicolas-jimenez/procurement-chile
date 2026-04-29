#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# run_year_pipeline.sh  <YEAR>  [--evict]  [--max-details N]
#
# Full pipeline for one calendar year — works locally and on SLURM clusters.
#   1. Download all detail JSONs for YEAR  (via run_details_by_year.sh)
#   2. Parse JSONs → data/clean/ordenes_compra_YEAR.parquet
#   3. With --evict: mark JSONs as Dropbox online-only to free local disk
#
# Required env vars:
#   MERCADO_PUBLICO_TICKET   API ticket
#   PROCUREMENT_CHILE_DB     Root of the data directory
#
# Optional env vars:
#   SLEEP_SECONDS            API call sleep (default: 0.3)
#   TIMEOUT_SECONDS          Request timeout (default: 60)
#
# Usage (local):
#   bash run_year_pipeline.sh 2022 --evict
#   bash run_year_pipeline.sh 2022 --max-details 500   # test run
#
# Usage (SLURM): use slurm_year_pipeline.sbatch instead.
#
# To run all years sequentially:
#   for Y in 2022 2023 2024 2025 2026; do
#     bash sandbox/code/download/run_year_pipeline.sh $Y --evict \
#       2>&1 | tee -a sandbox/logs/pipeline_$Y.log
#   done
# ─────────────────────────────────────────────────────────────────────────────

set -euo pipefail

# ── Arguments ─────────────────────────────────────────────────────────────────
YEAR="${1:-}"
EVICT_FLAG=""
MAX_DETAILS_FLAG=""

args=("$@")
for ((i=0; i<${#args[@]}; i++)); do
    case "${args[$i]}" in
        --evict)       EVICT_FLAG="--evict" ;;
        --max-details) MAX_DETAILS_FLAG="--max-details ${args[$((i+1))]}" ;;
    esac
done

if [[ -z "$YEAR" ]]; then
    echo "Usage: $0 <YEAR> [--evict] [--max-details N]"
    exit 1
fi

# ── Env vars with defaults ────────────────────────────────────────────────────
: "${MERCADO_PUBLICO_TICKET:?ERROR: MERCADO_PUBLICO_TICKET is not set}"
: "${PROCUREMENT_CHILE_DB:?ERROR: PROCUREMENT_CHILE_DB is not set}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PARQUET="$PROCUREMENT_CHILE_DB/data/clean/ordenes_compra_${YEAR}.parquet"
# On Bouchet PROJECT_ROOT is set by the sbatch; locally fall back to sandbox/logs
LOG_DIR="${PROJECT_ROOT:-${PROCUREMENT_CHILE_DB}}/logs"
mkdir -p "$LOG_DIR"

echo "============================================================"
echo "  PIPELINE: Ordenes de Compra — $YEAR"
[[ -n "${SLURM_JOB_ID:-}"        ]] && echo "  SLURM job   : ${SLURM_JOB_ID}"
[[ -n "${SLURM_ARRAY_TASK_ID:-}" ]] && echo "  SLURM array : ${SLURM_ARRAY_TASK_ID}"
echo "  Evict JSONs : ${EVICT_FLAG:-no}"
[[ -n "$MAX_DETAILS_FLAG"    ]] && echo "  Max details : $MAX_DETAILS_FLAG  [TEST MODE]"
echo "  DB root     : $PROCUREMENT_CHILE_DB"
echo "  Started     : $(date '+%Y-%m-%d %H:%M:%S')"
echo "============================================================"
echo ""

# ── Step 1: Download details ──────────────────────────────────────────────────
echo "▶ Step 1/2 — Downloading detail JSONs for $YEAR ..."
# shellcheck disable=SC2086
bash "$SCRIPT_DIR/run_details_by_year.sh" "$YEAR" $MAX_DETAILS_FLAG
echo "✓ Step 1 complete — $(date '+%H:%M:%S')"
echo ""

# ── Step 2: Parse to parquet ──────────────────────────────────────────────────
if [ -f "$PARQUET" ] && [ -z "$MAX_DETAILS_FLAG" ]; then
    echo "▶ Step 2/2 — Parquet already exists, skipping parse."
    echo "  $PARQUET"
    echo "  Delete it manually to force re-parse."
else
    echo "▶ Step 2/2 — Parsing JSONs → parquet ..."
    # shellcheck disable=SC2086
    python "$SCRIPT_DIR/parse_details_to_parquet.py" "$YEAR" $EVICT_FLAG
    echo "✓ Step 2 complete — $(date '+%H:%M:%S')"
fi

echo ""
echo "============================================================"
echo "  PIPELINE DONE: $YEAR — $(date '+%Y-%m-%d %H:%M:%S')"
if [ -f "$PARQUET" ]; then
    SIZE=$(python -c "import os; s=os.path.getsize('$PARQUET'); print(f'{s/1e6:.0f} MB')")
    echo "  Parquet : $PARQUET  ($SIZE)"
fi
echo "============================================================"
