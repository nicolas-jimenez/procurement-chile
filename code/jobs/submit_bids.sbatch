#!/usr/bin/env bash
#SBATCH --job-name=bids
#SBATCH --output=/nfs/roberts/project/pi_rp269/nj229/procurement-chile/logs/slurm-%j.out
#SBATCH --error=/nfs/roberts/project/pi_rp269/nj229/procurement-chile/logs/slurm-%j.err
#SBATCH --time=12:00:00
#SBATCH --cpus-per-task=8
#SBATCH --mem=64G
#SBATCH --partition=day
# ─────────────────────────────────────────────────────────────────────────────
# Bid-markup regression pipeline (code/analysis/bids/).
#
# Usage:
#   sbatch code/jobs/submit_bids.sbatch                         # all, --sample all
#   sbatch code/jobs/submit_bids.sbatch --sample municipalidades
#   sbatch code/jobs/submit_bids.sbatch --sample obras
# ─────────────────────────────────────────────────────────────────────────────
source "$(dirname "${BASH_SOURCE[0]:-$0}")/_preamble.sh"

STAGE_DIR="code/analysis/bids"
SAMPLE="all"
while [[ $# -gt 0 ]]; do
    case "$1" in
        --sample) SAMPLE="$2"; shift 2 ;;
        *) echo "Unknown argument: $1" >&2; exit 1 ;;
    esac
done

mapfile -t SCRIPTS < <(ls "${STAGE_DIR}"/[0-9][0-9]_*.py | sort)

for s in "${SCRIPTS[@]}"; do
    echo ""
    echo ">>> ${s} --sample ${SAMPLE}  ($(date -Is))"
    # Only scripts that accept --sample should get it; the others ignore extra args.
    python "${s}" --sample "${SAMPLE}" 2>&1 || {
        # Retry without --sample for scripts that don't understand it.
        python "${s}"
    }
done
