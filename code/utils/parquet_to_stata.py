"""
Convert data/clean/combined_sii_merged_filtered.parquet to Stata .dta.

Usage
-----
    python code/utils/parquet_to_stata.py --mode full
    python code/utils/parquet_to_stata.py --mode tenders   --n 50000
    python code/utils/parquet_to_stata.py --mode bidders   --n 200000

Modes
-----
    full     : Convert the entire parquet to a single .dta.
    tenders  : Draw a random sample of `--n` tenders (stratified pre/post
               and by dataset), then keep ALL bid-level rows belonging to
               those tenders. Preserves tender-level structure (useful for
               within-tender analyses).
    bidders  : Draw a random sample of `--n` bid-level rows, stratified by
               dataset × pre/post so both periods and both mechanisms are
               represented. Breaks tender-level structure but is faster and
               more compact.

Defaults are large enough to be representative of the full file:
    tenders : 50,000 tenders (~1–3 M rows, depending on n_oferentes)
    bidders : 500,000 rows (~0.8 % of the full panel)

The cutoff for pre vs post-reform is 2024-12-12 (Compra Ágil reform).

Notes
-----
Stata requires:
    * column names without spaces / hyphens
    * names no longer than 32 characters
    * string columns shorter than ~2045 bytes (or use version=117/118 strL)

This script:
    * normalizes column names (spaces / hyphens -> "_")
    * truncates excessively long names
    * uses Stata 118 format (Stata 14+) which supports strL strings
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config import DATA_CLEAN  # noqa: E402

PARQUET_PATH = DATA_CLEAN / "combined_sii_merged_filtered.parquet"
OUT_DIR = DATA_CLEAN

REFORM_DATE = pd.Timestamp("2024-12-12")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Make column names Stata-safe: no spaces/hyphens, <=32 chars, unique."""
    new_cols = []
    seen: dict[str, int] = {}
    for c in df.columns:
        nc = re.sub(r"[^0-9a-zA-Z_]", "_", c.strip())
        if re.match(r"^\d", nc):
            nc = "v_" + nc
        nc = nc[:32]
        # ensure uniqueness after truncation
        base = nc
        i = seen.get(base, 0)
        while nc in new_cols:
            i += 1
            suffix = f"_{i}"
            nc = base[: 32 - len(suffix)] + suffix
        seen[base] = i
        new_cols.append(nc)
    df = df.copy()
    df.columns = new_cols
    return df


def write_stata(df: pd.DataFrame, out_path: Path) -> None:
    """Write with version=118 so strL strings are supported."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df = normalize_columns(df)
    # pandas >=1.4 supports version=118; falls back to max supported otherwise
    df.to_stata(out_path, version=118, write_index=False)
    print(f"  wrote {out_path}  ({len(df):,} rows × {df.shape[1]} cols, "
          f"{out_path.stat().st_size / 1e6:.1f} MB)")


# ---------------------------------------------------------------------------
# Modes
# ---------------------------------------------------------------------------
def mode_full(out_path: Path) -> None:
    print(f"[full] reading {PARQUET_PATH}")
    df = pd.read_parquet(PARQUET_PATH)
    print(f"  loaded {len(df):,} rows × {df.shape[1]} cols")
    write_stata(df, out_path)


def mode_tenders(out_path: Path, n: int, seed: int) -> None:
    """Sample `n` tenders stratified by dataset × pre/post; keep ALL bids in them.

    Memory-safe: never loads the full panel into pandas. Streams row-groups
    via pyarrow, filters each batch against the picked tender keys, and
    concatenates only the matched bid-level rows.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    print(f"[tenders] sampling {n:,} tenders from {PARQUET_PATH}", flush=True)

    # Step 1: stream row-groups to extract unique (dataset, tender_id, fecha_pub).
    # Loading the full keys at once would OOM (65M rows of strings).
    print("  step 1: scanning row-groups for unique tender keys ...", flush=True)
    pf_keys = pq.ParquetFile(PARQUET_PATH)
    n_rg_keys = pf_keys.num_row_groups
    key_parts = []
    for i in range(n_rg_keys):
        sub = pf_keys.read_row_group(
            i, columns=["dataset", "tender_id", "fecha_pub"]
        ).to_pandas()
        sub = sub.drop_duplicates(subset=["dataset", "tender_id"])
        key_parts.append(sub)
        if (i + 1) % 100 == 0 or i == n_rg_keys - 1:
            print(f"    rg {i+1}/{n_rg_keys}", flush=True)
        del sub
    keys = pd.concat(key_parts, ignore_index=True).drop_duplicates(
        subset=["dataset", "tender_id"]
    )
    del key_parts
    keys = keys[keys["fecha_pub"].notna()].copy()
    keys["period"] = np.where(
        pd.to_datetime(keys["fecha_pub"]) < REFORM_DATE, "pre", "post"
    )
    print(f"    {len(keys):,} unique tenders with fecha_pub", flush=True)

    rng = np.random.default_rng(seed)
    per_cell = max(1, n // 4)
    picked_parts = []
    for (ds, period), grp in keys.groupby(["dataset", "period"]):
        take = min(per_cell, len(grp))
        idx = rng.choice(len(grp), size=take, replace=False)
        picked_parts.append(grp.iloc[idx][["dataset", "tender_id"]])
    picked = pd.concat(picked_parts, ignore_index=True)
    print(f"    picked {len(picked):,} tenders across 4 strata", flush=True)
    del keys

    # Build per-dataset key sets for fast filtering
    picked_sets = {
        ds: set(picked.loc[picked["dataset"] == ds, "tender_id"].astype(str))
        for ds in picked["dataset"].unique()
    }

    # Step 2: stream row-groups, write matching rows incrementally to a small
    # scratch parquet, then read it back. Using the OS temp dir keeps this
    # portable across machines.
    print("  step 2: streaming row-groups, writing matching rows to parquet ...", flush=True)
    import tempfile
    scratch_dir = Path(tempfile.mkdtemp(prefix="parquet_to_stata_"))
    out_pq = scratch_dir / ".tmp_tenders_sample.parquet"

    pf = pq.ParquetFile(PARQUET_PATH)
    ref_schema = pf.schema_arrow  # full source schema — avoids null-type lock-in
    writer = pq.ParquetWriter(out_pq, ref_schema, compression="snappy")
    n_rg = pf.num_row_groups
    rows_seen = rows_kept = 0
    for i in range(n_rg):
        tbl_full = pf.read_row_group(i)
        sub = tbl_full.to_pandas()
        del tbl_full
        rows_seen += len(sub)
        ds_col = sub["dataset"].astype(str)
        tid_col = sub["tender_id"].astype(str)
        mask = pd.Series(False, index=sub.index)
        for ds, s in picked_sets.items():
            sel = (ds_col == ds)
            if sel.any():
                mask |= sel & tid_col.isin(s)
        if mask.any():
            kept = sub.loc[mask]
            # Build arrays column-by-column against the source schema to avoid
            # type inference issues on small batches.
            arrays = []
            for field in ref_schema:
                col = kept[field.name] if field.name in kept.columns else pd.Series([None]*len(kept))
                try:
                    arrays.append(pa.array(col, type=field.type, from_pandas=True))
                except Exception:
                    arrays.append(pa.array([None]*len(kept), type=field.type))
            tbl = pa.Table.from_arrays(arrays, schema=ref_schema)
            writer.write_table(tbl)
            rows_kept += int(mask.sum())
            del kept, tbl, arrays
        if (i + 1) % 50 == 0 or i == n_rg - 1:
            print(f"    rg {i+1}/{n_rg}: seen={rows_seen:,}  kept={rows_kept:,}", flush=True)
        del sub, ds_col, tid_col, mask
        import gc; gc.collect()
    writer.close()
    print(f"    intermediate parquet: {rows_kept:,} rows ({out_pq.stat().st_size/1e6:.1f} MB)", flush=True)

    print("  step 3: reading intermediate into pandas and writing .dta ...", flush=True)
    full = pd.read_parquet(out_pq)
    print(f"  kept {len(full):,} bid-level rows for {len(picked):,} tenders", flush=True)
    write_stata(full, out_path)
    try:
        out_pq.unlink()
    except PermissionError:
        pass


def mode_bidders(out_path: Path, n: int, seed: int) -> None:
    """Sample `n` bid rows stratified by dataset × pre/post."""
    print(f"[bidders] sampling {n:,} bid-level rows from {PARQUET_PATH}")
    df = pd.read_parquet(PARQUET_PATH)
    df = df[df["fecha_pub"].notna()].copy()
    df["period"] = np.where(
        pd.to_datetime(df["fecha_pub"]) < REFORM_DATE, "pre", "post"
    )
    per_cell = max(1, n // 4)
    rng = np.random.default_rng(seed)
    parts = []
    for (ds, period), grp in df.groupby(["dataset", "period"]):
        take = min(per_cell, len(grp))
        idx = rng.choice(len(grp), size=take, replace=False)
        parts.append(grp.iloc[idx])
    sample = pd.concat(parts, ignore_index=True).drop(columns=["period"])
    print(f"  sampled {len(sample):,} rows across 4 strata")
    write_stata(sample, out_path)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--mode", choices=["full", "tenders", "bidders"],
                   required=True, help="Which export to produce.")
    p.add_argument("--n", type=int, default=None,
                   help="Sample size (tenders for --mode tenders; rows for "
                        "--mode bidders). Ignored when --mode full.")
    p.add_argument("--out", type=str, default=None,
                   help="Output .dta path. Defaults to data/clean/"
                        "combined_sii_merged_filtered[_sample_tag].dta")
    p.add_argument("--seed", type=int, default=20260413,
                   help="Random seed for sampling.")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    if args.mode == "full":
        out = Path(args.out) if args.out else OUT_DIR / "combined_sii_merged_filtered.dta"
        mode_full(out)
    elif args.mode == "tenders":
        n = args.n if args.n is not None else 50_000
        out = Path(args.out) if args.out else \
            OUT_DIR / f"combined_sii_merged_filtered_tenders{n}.dta"
        mode_tenders(out, n=n, seed=args.seed)
    elif args.mode == "bidders":
        n = args.n if args.n is not None else 500_000
        out = Path(args.out) if args.out else \
            OUT_DIR / f"combined_sii_merged_filtered_bidders{n}.dta"
        mode_bidders(out, n=n, seed=args.seed)
    else:  # pragma: no cover
        raise ValueError(args.mode)
    return 0


if __name__ == "__main__":
    sys.exit(main())
