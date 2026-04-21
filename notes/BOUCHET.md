# Running procurement-chile on Yale Bouchet

This is the cluster playbook: how to push code and data to Bouchet, run
analysis as Slurm jobs, and pull results back to the local Dropbox.

## One-time setup (already done)

| Step | Status |
|---|---|
| Remote project dir `/nfs/roberts/project/pi_rp269/nj229/procurement-chile/` | ✅ created |
| Git repo cloned as `procurement-chile/procurement-chile/` (SSH remote) | ✅ |
| Python 3.12 venv at `procurement-chile/venv/` with all `requirements.txt` deps | ✅ |
| `.env` on Bouchet points `PROCUREMENT_CHILE_DB` at the project root so `config.py` resolves cleanly | ✅ |
| `data/clean/`, `data/raw/other/`, `output/`, `logs/` created | ✅ |

### Remote layout

```
/nfs/roberts/project/pi_rp269/nj229/procurement-chile/
├── procurement-chile/         # git repo (code/, code/jobs/, .env)
├── data/
│   ├── clean/                 # mirrors Dropbox data/clean/
│   └── raw/other/             # mirrors Dropbox data/raw/other/
├── output/                    # all pipeline outputs
├── logs/                      # slurm-<jobid>.{out,err}
└── venv/                      # Python env
```

## Day-to-day workflow

### 1. Make sure the SSH tunnel is alive

Open a terminal on the laptop (not Claude):

```bash
ssh bouchet             # once per ~4h for interactive / job-control commands
ssh bouchet-transfer    # once per ~4h before any rsync (>1 MB)
```

Then Claude's `ssh -O check bouchet` will succeed and tools work without 2FA.

### 2. Push code changes

```bash
# Preferred: git
git push origin main
ssh bouchet 'cd /nfs/roberts/project/pi_rp269/nj229/procurement-chile/procurement-chile && git pull'

# Fast iteration on uncommitted code:
bash code/jobs/sync_to_yale.sh
```

### 3. Push data changes (only when data/clean was rebuilt locally)

```bash
bash code/jobs/sync_data_to_yale.sh          # just the main curated files
bash code/jobs/sync_data_to_yale.sh all      # everything under data/
DELETE=1 bash code/jobs/sync_data_to_yale.sh # mirror exactly (danger)
```

### 4. Submit jobs

```bash
ssh bouchet 'cd /nfs/roberts/project/pi_rp269/nj229/procurement-chile/procurement-chile \
             && sbatch code/jobs/submit_choice_function.sbatch'

# Override resources at submission time:
ssh bouchet 'cd ... && sbatch --mem=128G --cpus-per-task=16 --time=24:00:00 \
             jobs/submit_generic.sbatch code/analysis/did/02_run_did.py'
```

Available sbatch scripts:

| Script | What it runs | Default resources |
|---|---|---|
| `submit_choice_function.sbatch` | all of `code/analysis/choice_function/` (01→02→03) | 8 CPU / 64 G / 12h |
| `submit_did.sbatch`             | `code/analysis/did/` in order (01–08) | 8 CPU / 64 G / 12h |
| `submit_bids.sbatch`            | `code/analysis/bids/` (with `--sample` passthrough) | 8 CPU / 64 G / 12h |
| `submit_build_combined.sbatch`  | full `code/clean/` rebuild | 16 CPU / 256 G / 24h |
| `submit_generic.sbatch`         | any `python <script.py>` with args | 8 CPU / 64 G / 12h |

All of them `source code/jobs/_preamble.sh`, which loads the Python module,
activates the venv, caps BLAS thread count to `$SLURM_CPUS_PER_TASK`, and
`cd`s into the repo.

### 5. Monitor

```bash
ssh bouchet 'squeue -u nj229'
ssh bouchet 'tail -f /nfs/roberts/project/pi_rp269/nj229/procurement-chile/logs/slurm-<jobid>.out'
ssh bouchet 'scancel <jobid>'
ssh bouchet 'sacct -u nj229 --format=JobID,JobName,Partition,State,Elapsed,MaxRSS -S $(date -Id)'
```

### 6. Pull results back to Dropbox

```bash
bash code/jobs/sync_from_yale.sh                            # all lightweight outputs (today)
bash code/jobs/sync_from_yale.sh 2026-04-21                 # with date suffix
bash code/jobs/sync_from_yale.sh 2026-04-21 choice_function # one subdir
bash code/jobs/sync_from_yale.sh 2026-04-21 logs            # slurm logs only
bash code/jobs/sync_from_yale.sh 2026-04-21 figures         # PNGs + PDFs only
```

Results land under `$PROCUREMENT_CHILE_DB/output/yale/<scope>_<date>/` so
each pull is traceable and doesn't overwrite the previous one.

## Resource guidelines

From the bouchet-ssh skill (Yale `day` partition: 24h max, 64 CPU/node, 990 GiB/node):

| Workload | CPUs | Mem | Time |
|---|---|---|---|
| Cleaning / descriptives | 4–8 | 16–32 G | 4–6 h |
| Regressions / estimation | 8 | 64 G | 12–24 h |
| Structural / huge panels | 16 | 128–256 G | 24 h+ (use `week` partition) |

## Troubleshooting

- **`libpython3.12.so.1.0: cannot open shared object file`** — forgot `module load Python/3.12.3-GCCcore-13.3.0`. The sbatch preamble does this; running python directly via `ssh bouchet` needs you to load the module in the same command.
- **SSH "connection refused" / "Permission denied"** — the 4h multiplex session expired. Re-run `ssh bouchet` / `ssh bouchet-transfer` in a terminal to re-authenticate.
- **`rsync --delete` on data/** — don't. The data directory holds the 20 GB combined file we transferred once; `DELETE=1` in `sync_data_to_yale.sh` will remove anything not locally present.
- **Module environment in sbatch** — Slurm gives jobs a clean environment. Always go through `_preamble.sh`; don't assume the login-session modules are there.
