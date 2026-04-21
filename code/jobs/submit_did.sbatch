#!/usr/bin/env bash
#SBATCH --job-name=did
#SBATCH --output=/nfs/roberts/project/pi_rp269/nj229/procurement-chile/logs/slurm-%j.out
#SBATCH --error=/nfs/roberts/project/pi_rp269/nj229/procurement-chile/logs/slurm-%j.err
#SBATCH --time=12:00:00
#SBATCH --cpus-per-task=8
#SBATCH --mem=64G
#SBATCH --partition=day
# ─────────────────────────────────────────────────────────────────────────────
# Compra Ágil DiD pipeline end-to-end.
# Runs scripts 01–08 from code/analysis/did/ in order.
#
# Usage:
#   sbatch code/jobs/submit_did.sbatch            # run all scripts (01..08)
#   sbatch code/jobs/submit_did.sbatch 02 05      # run specific prefixes only
# ─────────────────────────────────────────────────────────────────────────────
source "$(dirname "${BASH_SOURCE[0]:-$0}")/_preamble.sh"

STAGE_DIR="code/analysis/did"
PREFIXES=("$@")

if [[ ${#PREFIXES[@]} -eq 0 ]]; then
    mapfile -t SCRIPTS < <(ls "${STAGE_DIR}"/[0-9][0-9]_*.py | sort)
else
    SCRIPTS=()
    for p in "${PREFIXES[@]}"; do
        mapfile -t found < <(ls "${STAGE_DIR}/${p}_"*.py 2>/dev/null || true)
        SCRIPTS+=("${found[@]}")
    done
fi

if [[ ${#SCRIPTS[@]} -eq 0 ]]; then
    echo "No scripts matched." >&2
    exit 1
fi

for s in "${SCRIPTS[@]}"; do
    echo ""
    echo ">>> ${s}  ($(date -Is))"
    python "${s}"
done
echo ""
echo ">>> DiD pipeline done at $(date -Is)"
