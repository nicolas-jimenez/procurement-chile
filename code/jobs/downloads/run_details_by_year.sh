#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# run_details_by_year.sh  <YEAR>  [--max-details N]
#
# Stage 2 detail downloader for a single calendar year.
# Works locally (Mac/Linux) and on SLURM clusters — paths come from env vars.
#
# Required env vars:
#   MERCADO_PUBLICO_TICKET   API ticket
#   PROCUREMENT_CHILE_DB     Root of the data directory (Dropbox or cluster fs)
#
# Optional env vars:
#   SLEEP_SECONDS            Pause between API calls (default: 0.3)
#   TIMEOUT_SECONDS          Request timeout in seconds (default: 60)
#
# Usage:
#   bash run_details_by_year.sh 2022
#   bash run_details_by_year.sh 2022 --max-details 500   # test run
#
# On SLURM, use slurm_year_pipeline.sbatch instead of calling this directly.
# ─────────────────────────────────────────────────────────────────────────────

set -euo pipefail

# ── Arguments ─────────────────────────────────────────────────────────────────
YEAR="${1:-}"
MAX_DETAILS_FLAG=""
for arg in "$@"; do
    case "$arg" in
        --max-details) ;;   # key; value captured below
    esac
done

# Parse --max-details N
i=1
for arg in "$@"; do
    if [[ "$arg" == "--max-details" ]]; then
        shift_val="${*:$((i+1)):1}"
        MAX_DETAILS_FLAG="--max-details $shift_val"
    fi
    ((i++)) || true
done
# Simpler re-parse
MAX_DETAILS_FLAG=""
args=("$@")
for ((i=0; i<${#args[@]}; i++)); do
    if [[ "${args[$i]}" == "--max-details" ]]; then
        MAX_DETAILS_FLAG="--max-details ${args[$((i+1))]}"
    fi
done

if [[ -z "$YEAR" ]]; then
    echo "Usage: $0 <YEAR> [--max-details N]"
    exit 1
fi

if ! [[ "$YEAR" =~ ^[0-9]{4}$ ]]; then
    echo "ERROR: YEAR must be 4 digits (e.g. 2022)"
    exit 1
fi

# ── Env vars with defaults ────────────────────────────────────────────────────
: "${MERCADO_PUBLICO_TICKET:?ERROR: MERCADO_PUBLICO_TICKET is not set}"
: "${PROCUREMENT_CHILE_DB:?ERROR: PROCUREMENT_CHILE_DB is not set}"
SLEEP_SECONDS="${SLEEP_SECONDS:-0.3}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-60}"

export MERCADO_PUBLICO_TICKET
export PROCUREMENT_CHILE_DB

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
R_SCRIPT="$SCRIPT_DIR/01_download_ordenes_compra_api.R"
OUTPUT_ROOT="$PROCUREMENT_CHILE_DB/data/raw/chilecompra/ordenes_compra"
MANIFEST="$OUTPUT_ROOT/manifests/order_codes.csv"

# ── Safety checks ─────────────────────────────────────────────────────────────
if [ ! -f "$MANIFEST" ]; then
    echo "ERROR: Stage 1 manifest not found at $MANIFEST"
    echo "Run run_stage1.sh first."
    exit 1
fi

IN_SCOPE=$(python3 -c "
import csv
with open('$MANIFEST') as f:
    n = sum(1 for r in csv.DictReader(f) if (r.get('requested_date') or '')[:4] == '$YEAR')
print(n)
")

ALREADY=$(find "$OUTPUT_ROOT/detail_json/$YEAR" -name "*.json" 2>/dev/null | wc -l | tr -d ' ')

echo "============================================================"
echo "  Stage 2 details — $YEAR"
[[ -n "${SLURM_JOB_ID:-}" ]] && echo "  SLURM job ID : ${SLURM_JOB_ID}"
echo "  Order codes  : $IN_SCOPE  (already done: $ALREADY)"
echo "  Remaining    : $(( IN_SCOPE - ALREADY ))"
[[ -n "$MAX_DETAILS_FLAG" ]] && echo "  Max details  : $MAX_DETAILS_FLAG  [TEST MODE]"
echo "  Output root  : $OUTPUT_ROOT"
echo "  Started      : $(date '+%Y-%m-%d %H:%M:%S')"
echo "============================================================"
echo ""

if [ "$IN_SCOPE" -eq 0 ]; then
    echo "No order codes found for $YEAR. Exiting."
    exit 0
fi

# ── Run ───────────────────────────────────────────────────────────────────────
# shellcheck disable=SC2086
Rscript "$R_SCRIPT" \
    --details-only \
    --start-date  "${YEAR}-01-01" \
    --end-date    "${YEAR}-12-31" \
    --output-root "$OUTPUT_ROOT" \
    --sleep-seconds   "$SLEEP_SECONDS" \
    --timeout-seconds "$TIMEOUT_SECONDS" \
    $MAX_DETAILS_FLAG

echo ""
echo "Download complete for $YEAR — $(date '+%Y-%m-%d %H:%M:%S')"
