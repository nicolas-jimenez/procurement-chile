#!/usr/bin/env bash
#SBATCH --job-name=choice_function
#SBATCH --output=/nfs/roberts/project/pi_rp269/nj229/procurement-chile/logs/slurm-%j.out
#SBATCH --error=/nfs/roberts/project/pi_rp269/nj229/procurement-chile/logs/slurm-%j.err
#SBATCH --time=12:00:00
#SBATCH --cpus-per-task=8
#SBATCH --mem=64G
#SBATCH --partition=day
# ─────────────────────────────────────────────────────────────────────────────
# Buyer-level choice function: build sample -> estimate -> summarise.
#
# Stages (run in order by default; pass --only <stage> to run one):
#   build      code/analysis/choice_function/01_build_choice_sample.py
#   estimate   code/analysis/choice_function/02_estimate_choice_functions.py
#   summary    code/analysis/choice_function/03_summarize_choice.py
#
# Usage:
#   sbatch code/jobs/submit_choice_function.sbatch                 # run all
#   sbatch code/jobs/submit_choice_function.sbatch --only build    # one stage
# ─────────────────────────────────────────────────────────────────────────────
source "$(dirname "${BASH_SOURCE[0]:-$0}")/_preamble.sh"

STAGE_DIR="code/analysis/choice_function"
ONLY=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --only) ONLY="$2"; shift 2 ;;
        *) echo "Unknown argument: $1" >&2; exit 1 ;;
    esac
done

run_stage() {
    local name="$1" script="$2"
    echo ""
    echo ">>> Stage ${name}: ${script}"
    echo ">>> Started $(date -Is)"
    python "${script}"
    echo ">>> Finished ${name} at $(date -Is)"
}

if [[ -z "${ONLY}" || "${ONLY}" == "build" ]]; then
    run_stage "build"    "${STAGE_DIR}/01_build_choice_sample.py"
fi
if [[ -z "${ONLY}" || "${ONLY}" == "estimate" ]]; then
    run_stage "estimate" "${STAGE_DIR}/02_estimate_choice_functions.py"
fi
if [[ -z "${ONLY}" || "${ONLY}" == "summary" ]]; then
    run_stage "summary"  "${STAGE_DIR}/03_summarize_choice.py"
fi
