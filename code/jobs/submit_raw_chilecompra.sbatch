#!/usr/bin/env bash
#SBATCH --job-name=procchile-raw
#SBATCH --output=/nfs/roberts/project/pi_rp269/nj229/procurement-chile/logs/slurm-%j.out
#SBATCH --error=/nfs/roberts/project/pi_rp269/nj229/procurement-chile/logs/slurm-%j.err
#SBATCH --time=24:00:00
#SBATCH --cpus-per-task=2
#SBATCH --mem=16G
#SBATCH --partition=day
# ---------------------------------------------------------------------------
# Submit the ChileCompra raw bulk downloader for licitaciones and Compra Agil.
#
# Default mode downloads both datasets:
#   - licitaciones: 2007-01 through current month
#   - compra_agil:  2020-04 through current month
#
# Usage:
#   sbatch code/jobs/submit_raw_chilecompra.sbatch
#
# Useful narrower runs:
#   sbatch code/jobs/submit_raw_chilecompra.sbatch \
#     --datasets licitaciones --licitaciones-start-month 2022-01 --licitaciones-end-month 2026-04
#
#   sbatch code/jobs/submit_raw_chilecompra.sbatch \
#     --datasets compra_agil --compra-agil-start-month 2020-04 --compra-agil-end-month 2026-04
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
if [[ -f "${SCRIPT_DIR}/_preamble.sh" ]]; then
    source "${SCRIPT_DIR}/_preamble.sh"
elif [[ -n "${SLURM_SUBMIT_DIR:-}" && -f "${SLURM_SUBMIT_DIR}/code/jobs/_preamble.sh" ]]; then
    source "${SLURM_SUBMIT_DIR}/code/jobs/_preamble.sh"
else
    echo "Could not locate code/jobs/_preamble.sh" >&2
    exit 2
fi

module load R/4.4.2-gfbf-2024a

echo "R                  : $(which Rscript) ($(Rscript --version 2>&1 | head -n 1))"
echo "Command            : Rscript code/download/00_download_licitaciones.R $*"
echo ""

Rscript code/download/00_download_licitaciones.R "$@"
