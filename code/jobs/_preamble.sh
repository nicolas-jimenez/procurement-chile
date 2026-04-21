#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Shared preamble sourced by every submit_*.sbatch script.
#
# Does three things:
#   1. module load Python so libpython3.12.so.1.0 is findable at runtime.
#   2. activate the project venv (has pandas, pyarrow, pyfixest, etc.).
#   3. cd into the repo so relative imports and sys.path tricks work.
#
# It also exports PROJECT_ROOT and REPO_ROOT for use inside the sbatch body.
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

PROJECT_ROOT="/nfs/roberts/project/pi_rp269/nj229/procurement-chile"
REPO_ROOT="${PROJECT_ROOT}/procurement-chile"
VENV="${PROJECT_ROOT}/venv"

module purge 2>/dev/null || true
module load Python/3.12.3-GCCcore-13.3.0

# shellcheck disable=SC1091
source "${VENV}/bin/activate"

export PROJECT_ROOT REPO_ROOT
export PYTHONUNBUFFERED=1
# Cap thread parallelism so BLAS/OMP don't thrash with multi-process jobs.
export OMP_NUM_THREADS="${SLURM_CPUS_PER_TASK:-8}"
export MKL_NUM_THREADS="${OMP_NUM_THREADS}"
export OPENBLAS_NUM_THREADS="${OMP_NUM_THREADS}"
export NUMEXPR_NUM_THREADS="${OMP_NUM_THREADS}"

cd "${REPO_ROOT}"

echo "=================================================================="
echo "Job                : ${SLURM_JOB_NAME:-local}  (${SLURM_JOB_ID:-n/a})"
echo "Host               : $(hostname)"
echo "CPUs-per-task      : ${SLURM_CPUS_PER_TASK:-?}"
echo "Memory             : ${SLURM_MEM_PER_NODE:-?} MB"
echo "Partition          : ${SLURM_JOB_PARTITION:-?}"
echo "Started            : $(date -Is)"
echo "PROJECT_ROOT       : ${PROJECT_ROOT}"
echo "REPO_ROOT          : ${REPO_ROOT}"
echo "Python             : $(which python) ($(python --version 2>&1))"
echo "=================================================================="
