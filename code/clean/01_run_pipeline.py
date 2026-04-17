"""
01_run_pipeline.py
─────────────────────────────────────────────────────────────────────────────
Master pipeline runner.  Executes all stages in order.

Usage
  python code/clean/01_run_pipeline.py               # run all stages
  python code/clean/01_run_pipeline.py 3 4 5         # run specific stages
  python code/clean/01_run_pipeline.py --list        # list stages

Stages
  01  (independent)  Clean & append licitaciones  → data/clean/chilecompra_panel.parquet
  02  (independent)  Clean & append compra ágil   → data/clean/compra_agil_panel.parquet
  ── stages 01 & 02 can run in parallel ──────────────────────────────────────
  03  (independent)  Merge licitaciones with SII  → data/clean/licitaciones_sii_merged.parquet
  04  (independent)  Merge compra ágil with SII   → data/clean/compra_agil_sii_merged.parquet
  ── stages 03 & 04 can run in parallel ──────────────────────────────────────
  05  Combine SII-merged files                    → data/clean/combined_sii_merged.parquet
  06  Filter estimated-cost outliers              → data/clean/combined_sii_merged_filtered.parquet
  07  Diagnostics (split by dataset)              → data/diagnostics/figures/combined_*.png
  08  Quarterly trend analysis                    → data/diagnostics/figures/trends_*.png
  09  Fill CA sector from RutUnidad crosswalk     → data/clean/rut_unidad_sector_crosswalk.parquet
"""

import subprocess
import sys
import time
from pathlib import Path

CLEAN_DIR = Path(__file__).resolve().parent
ROOT      = CLEAN_DIR.parents[1]

# Stages keyed by string
STAGES = {
    "1":  ("Clean & append licitaciones",         CLEAN_DIR / "02_clean_licitaciones.py"),
    "2":  ("Clean & append compra ágil",          CLEAN_DIR / "03_clean_compra_agil.py"),
    "3":  ("Merge licitaciones with SII",         CLEAN_DIR / "04_merge_sii_licitaciones.py"),
    "4":  ("Merge compra ágil with SII",          CLEAN_DIR / "05_merge_sii_compra_agil.py"),
    "5":  ("Combine SII-merged files",            CLEAN_DIR / "06_combine_sii_merged.py"),
    "6":  ("Filter estimated-cost outliers",      CLEAN_DIR / "07_filter_estimated_cost_outliers.py"),
    "7":  ("Diagnostics (split by dataset)",      CLEAN_DIR / "08_diagnostics.py"),
    "8":  ("Quarterly trend analysis",            CLEAN_DIR / "09_quarterly_trends.py"),
    "9":  ("Fill CA sector from RutUnidad map",   CLEAN_DIR / "10_fill_sector_from_rutunidad.py"),
}

PARALLEL_GROUPS = [
    {"1", "2"},   # licitaciones & compra ágil cleaning
    {"3", "4"},   # parallel SII merges
]

DEFAULT_ORDER = ["1", "2", "3", "4", "5", "6", "7", "8", "9"]


def list_stages():
    print("Available pipeline stages:")
    for key in DEFAULT_ORDER:
        label, path = STAGES[key]
        note = ""
        for grp in PARALLEL_GROUPS:
            if key in grp:
                others = grp - {key}
                note = f"  [can run in parallel with stage(s) {', '.join(sorted(others))}]"
        print(f"  {key:>2}. {label}{note}")
        print(f"       {path}")


def run_stage(key, parallel=False):
    label, script = STAGES[key]
    sep = "=" * 70
    print(f"\n{sep}")
    print(f"STAGE {key}: {label}")
    print(f"Script: {script}")
    print(f"{sep}\n")
    t0 = time.time()
    result = subprocess.run([sys.executable, str(script)], cwd=str(ROOT))
    elapsed = time.time() - t0
    if result.returncode != 0:
        print(f"\n[ERROR] Stage {key} failed with exit code {result.returncode}")
        sys.exit(result.returncode)
    print(f"\n[OK] Stage {key} completed in {elapsed:.0f}s")


def run_parallel(keys):
    """Run a group of stages in parallel using subprocesses."""
    import concurrent.futures
    print(f"\n{'='*70}")
    print(f"Running stages {sorted(keys)} IN PARALLEL")
    print(f"{'='*70}")

    results = {}
    t0 = time.time()

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(keys)) as ex:
        futures = {ex.submit(run_stage, k, parallel=True): k for k in keys}
        for fut in concurrent.futures.as_completed(futures):
            k = futures[fut]
            try:
                fut.result()
                results[k] = "ok"
            except SystemExit as e:
                results[k] = f"failed ({e.code})"

    elapsed = time.time() - t0
    failed = [k for k, v in results.items() if v != "ok"]
    if failed:
        print(f"\n[ERROR] Parallel stages failed: {failed}")
        sys.exit(1)
    print(f"\n[OK] Parallel group {sorted(keys)} completed in {elapsed:.0f}s")


if "--list" in sys.argv:
    list_stages()
    sys.exit(0)

# Determine which stages to run
raw_args = [a for a in sys.argv[1:] if not a.startswith("-")]
requested = raw_args if raw_args else DEFAULT_ORDER
to_run = []
for a in requested:
    if a not in STAGES:
        print(f"[WARN] Unknown stage '{a}', skipping")
    else:
        to_run.append(a)

print(f"Running pipeline stages: {to_run}")
print("Note: Stages 1+2 and 3+4 are independent and can run in parallel.\n")

# Execute stages, collapsing parallel groups when both present
i = 0
while i < len(to_run):
    key = to_run[i]
    # Check if this key belongs to a parallel group whose other members are also queued
    matched_group = None
    for grp in PARALLEL_GROUPS:
        if key in grp:
            present = [k for k in to_run[i:] if k in grp]
            if len(present) > 1:
                matched_group = present
                break

    if matched_group:
        run_parallel(matched_group)
        # Skip past all members of this group
        run_set = set(matched_group)
        while i < len(to_run) and to_run[i] in run_set:
            i += 1
    else:
        run_stage(key)
        i += 1

print(f"\n{'='*70}")
print("Pipeline complete.")
print(f"{'='*70}")
