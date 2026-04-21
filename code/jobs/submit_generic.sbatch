#!/usr/bin/env bash
#SBATCH --job-name=procchile-generic
#SBATCH --output=/nfs/roberts/project/pi_rp269/nj229/procurement-chile/logs/slurm-%j.out
#SBATCH --error=/nfs/roberts/project/pi_rp269/nj229/procurement-chile/logs/slurm-%j.err
#SBATCH --time=12:00:00
#SBATCH --cpus-per-task=8
#SBATCH --mem=64G
#SBATCH --partition=day
# ─────────────────────────────────────────────────────────────────────────────
# Generic launcher — run any python script from the repo.
#
# Usage:
#   sbatch code/jobs/submit_generic.sbatch code/analysis/did/02_run_did.py
#   sbatch code/jobs/submit_generic.sbatch code/analysis/bids/01_build_bid_sample.py --sample municipalidades
#
# Customise resources at submission time:
#   sbatch --mem=128G --cpus-per-task=16 --time=24:00:00 code/jobs/submit_generic.sbatch ...
# ─────────────────────────────────────────────────────────────────────────────
source "$(dirname "${BASH_SOURCE[0]:-$0}")/_preamble.sh"

if [[ $# -lt 1 ]]; then
    echo "Usage: sbatch code/jobs/submit_generic.sbatch <path/to/script.py> [args...]" >&2
    exit 2
fi

SCRIPT="$1"; shift
echo "Command            : python ${SCRIPT} $*"
echo ""

python "${SCRIPT}" "$@"
