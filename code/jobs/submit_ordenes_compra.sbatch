#!/usr/bin/env bash
#SBATCH --job-name=procchile-oc
#SBATCH --output=/nfs/roberts/project/pi_rp269/nj229/procurement-chile/logs/slurm-%j.out
#SBATCH --error=/nfs/roberts/project/pi_rp269/nj229/procurement-chile/logs/slurm-%j.err
#SBATCH --time=24:00:00
#SBATCH --cpus-per-task=4
#SBATCH --mem=16G
#SBATCH --partition=day
# ─────────────────────────────────────────────────────────────────────────────
# Submit the Mercado Público purchase-order API downloader.
#
# Default mode: runs BOTH phases (phase 1 = daily discovery, phase 2 = detail
# downloads) sequentially within a single job.  Pass --daily-only to run only
# phase 1 and automatically chain a phase-2 dependency job.
#
# Usage (full two-phase, single job):
#   sbatch code/jobs/submit_ordenes_compra.sbatch \
#     --start-date 2015-01-01 --end-date 2026-04-22
#
# Usage (phase 1 only + auto-chain phase 2 as dependent job):
#   sbatch code/jobs/submit_ordenes_compra.sbatch \
#     --daily-only --start-date 2015-01-01 --end-date 2026-04-22
#
# Usage (phase 2 only, using existing order-codes manifest):
#   sbatch code/jobs/submit_ordenes_compra.sbatch \
#     --details-only --start-date 2015-01-01 --end-date 2026-04-22
#
# Requires:
#   - code/jobs/downloads/01_download_ordenes_compra_api.R in the repo
#   - API ticket in $MERCADO_PUBLICO_TICKET or $CHILECOMPRA_API_TICKET,
#     or passed at submit time:
#       sbatch --export=ALL,MERCADO_PUBLICO_TICKET=... code/jobs/submit_ordenes_compra.sbatch ...
# ─────────────────────────────────────────────────────────────────────────────
PROJECT_ROOT="${PROJECT_ROOT:-/nfs/roberts/project/pi_rp269/nj229/procurement-chile}"
REPO_ROOT="${REPO_ROOT:-${PROJECT_ROOT}/procurement-chile}"
source "${REPO_ROOT}/code/jobs/_preamble.sh"

module load R/4.4.2-gfbf-2024a

if [[ -z "${MERCADO_PUBLICO_TICKET:-}" && -z "${CHILECOMPRA_API_TICKET:-}" ]]; then
    echo "Missing API ticket. Set MERCADO_PUBLICO_TICKET or CHILECOMPRA_API_TICKET." >&2
    exit 2
fi

echo "R                  : $(which Rscript) ($(Rscript --version 2>&1 | head -n 1))"

# Detect if --daily-only was passed so we can chain phase 2 as a dependency.
HAS_DAILY_ONLY=false
for arg in "$@"; do
    [[ "$arg" == "--daily-only" ]] && HAS_DAILY_ONLY=true
done

echo "Command            : Rscript code/jobs/downloads/01_download_ordenes_compra_api.R $*"
echo ""

Rscript code/jobs/downloads/01_download_ordenes_compra_api.R "$@"

# If phase 1 ran with --daily-only, automatically submit phase 2 as a
# dependency so it starts as soon as this job finishes successfully.
if $HAS_DAILY_ONLY; then
    echo ""
    echo "Phase 1 complete. Submitting phase 2 (details-only) as dependent job..."

    # Replace --daily-only with --details-only in the argument list.
    PHASE2_ARGS=()
    for arg in "$@"; do
        if [[ "$arg" == "--daily-only" ]]; then
            PHASE2_ARGS+=("--details-only")
        else
            PHASE2_ARGS+=("$arg")
        fi
    done

    PHASE2_JOB=$(sbatch \
        --job-name=procchile-oc-p2 \
        --dependency=afterok:${SLURM_JOB_ID} \
        --export=ALL \
        "$(dirname "${BASH_SOURCE[0]:-$0}")/submit_ordenes_compra.sbatch" \
        "${PHASE2_ARGS[@]}" \
        | awk '{print $NF}')

    echo "Phase 2 job submitted: ${PHASE2_JOB}"
    echo "Monitor with: squeue -u nj229 -j ${PHASE2_JOB}"
fi
