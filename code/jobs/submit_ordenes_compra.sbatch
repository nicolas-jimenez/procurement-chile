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
# Usage:
#   sbatch code/jobs/submit_ordenes_compra.sbatch --start-date 2015-01-01 --end-date 2026-04-22
#
# Requires:
#   - code/download/01_download_ordenes_compra_api.R in the repo
#   - an API ticket in $MERCADO_PUBLICO_TICKET or $CHILECOMPRA_API_TICKET,
#     or passed inline at submit time:
#       sbatch --export=ALL,MERCADO_PUBLICO_TICKET=... code/jobs/submit_ordenes_compra.sbatch ...
# ─────────────────────────────────────────────────────────────────────────────
source "$(dirname "${BASH_SOURCE[0]:-$0}")/_preamble.sh"

module load R/4.4.2-gfbf-2024a

if [[ -z "${MERCADO_PUBLICO_TICKET:-}" && -z "${CHILECOMPRA_API_TICKET:-}" ]]; then
    echo "Missing API ticket. Set MERCADO_PUBLICO_TICKET or CHILECOMPRA_API_TICKET." >&2
    exit 2
fi

echo "R                  : $(which Rscript) ($(Rscript --version 2>&1 | head -n 1))"
echo "Command            : Rscript code/download/01_download_ordenes_compra_api.R $*"
echo ""

Rscript code/download/01_download_ordenes_compra_api.R "$@"
