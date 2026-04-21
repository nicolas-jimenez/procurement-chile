# Slurm jobs on Yale Bouchet

This directory holds `sbatch` scripts for running the procurement-chile
pipeline on Yale's Bouchet HPC cluster. The layout mirrors
`ridehailing-india/jobs/` from the nammayatri project.

## Layout on Bouchet

```
/nfs/roberts/project/pi_rp269/nj229/procurement-chile/
├── procurement-chile/         # git repo (this repo, cloned)
│   ├── code/
│   │   ├── analysis/, clean/, config.py, utils/   # all analysis & cleaning code
│   │   └── jobs/              # sbatch + sync scripts (this directory)
│   └── .env                   # points DATA / OUTPUT at project root
├── data/                      # data/clean and data/raw/other mirror Dropbox
├── output/                    # all pipeline outputs land here
├── logs/                      # slurm stdout/stderr
└── venv/                      # Python 3.12 venv with requirements.txt installed
```

## Path resolution

`code/config.py` reads `.env` and resolves:
- `DROPBOX_ROOT = PROCUREMENT_CHILE_DB`
- `DATA_CLEAN   = DROPBOX_ROOT / data / clean`
- `OUTPUT_ROOT  = DROPBOX_ROOT / output`

On Bouchet `.env` is set to `PROCUREMENT_CHILE_DB=/nfs/roberts/project/pi_rp269/nj229/procurement-chile`
so the same scripts work unchanged locally and on the cluster.

## Quick start

All commands are run from the repo root. The scripts live at `code/jobs/`.

```bash
# 0. Make sure the SSH tunnel is alive (run once per session from a terminal)
ssh bouchet             # login node
ssh bouchet-transfer    # dedicated transfer node (for big rsyncs)

# 1. Push code changes
git push origin main
bash code/jobs/sync_to_yale.sh        # or: ssh bouchet 'cd .../procurement-chile && git pull'

# 2. Submit a job
ssh bouchet 'cd /nfs/roberts/project/pi_rp269/nj229/procurement-chile/procurement-chile \
             && sbatch code/jobs/submit_choice_function.sbatch'

# 3. Monitor
ssh bouchet 'squeue -u nj229'
ssh bouchet 'tail -f /nfs/roberts/project/pi_rp269/nj229/procurement-chile/logs/slurm-<jobid>.out'

# 4. Pull results back to the local Dropbox
bash code/jobs/sync_from_yale.sh              # all lightweight outputs (today)
bash code/jobs/sync_from_yale.sh 2026-04-21   # with date suffix
bash code/jobs/sync_from_yale.sh 2026-04-21 logs
```

## Available sbatch scripts

| Script | Purpose | Resources |
|---|---|---|
| `submit_choice_function.sbatch`  | buyer-level choice function (3 scripts in `code/analysis/choice_function/`) | 8 CPU / 64 G / 12h |
| `submit_did.sbatch`              | DiD pipeline end-to-end (`code/analysis/did/`)                             | 8 CPU / 64 G / 12h |
| `submit_bids.sbatch`             | bid-markup regressions (`code/analysis/bids/`)                             | 8 CPU / 64 G / 12h |
| `submit_build_combined.sbatch`   | rebuild `combined_sii_merged_filtered.parquet` from raw SII + Chilecompra  | 16 CPU / 256 G / 24h |
| `submit_generic.sbatch`          | run any python script: `sbatch code/jobs/submit_generic.sbatch code/analysis/x.py` | 8 CPU / 64 G / 12h |

All sbatch scripts share the same preamble (module load + venv activate + `cd` to repo),
accept positional args (forwarded to the underlying python script), and write logs to
`$PROJECT_ROOT/logs/slurm-<jobid>.{out,err}`.
