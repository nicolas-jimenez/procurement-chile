#!/usr/bin/env bash
#SBATCH --job-name=build_combined
#SBATCH --output=/nfs/roberts/project/pi_rp269/nj229/procurement-chile/logs/slurm-%j.out
#SBATCH --error=/nfs/roberts/project/pi_rp269/nj229/procurement-chile/logs/slurm-%j.err
#SBATCH --time=24:00:00
#SBATCH --cpus-per-task=16
#SBATCH --mem=256G
#SBATCH --partition=day
# ─────────────────────────────────────────────────────────────────────────────
# Heavy-compute rebuild of combined_sii_merged_filtered.parquet from scratch.
# Runs the cleaning pipeline end-to-end.
#
# Raw data must already be present at $PROJECT_ROOT/data/raw/ (licitaciones/,
# compra_agil/, sii/ etc.). If missing, only the downstream scripts that read
# from data/clean/ will work.
# ─────────────────────────────────────────────────────────────────────────────
source "$(dirname "${BASH_SOURCE[0]:-$0}")/_preamble.sh"

STAGE_DIR="code/clean"
mapfile -t SCRIPTS < <(ls "${STAGE_DIR}"/[0-9][0-9]_*.py | sort)

for s in "${SCRIPTS[@]}"; do
    echo ""
    echo ">>> ${s}  ($(date -Is))"
    python "${s}"
done
echo ""
echo ">>> Clean pipeline done at $(date -Is)"
