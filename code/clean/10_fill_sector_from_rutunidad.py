"""
clean/10_fill_sector_from_rutunidad.py
─────────────────────────────────────────────────────────────────────────────
Build a buyer-level sector crosswalk from licitaciones and use it to fill
missing `sector` on Compra Ágil rows.

Inputs:
  data/clean/chilecompra_panel.parquet
  data/clean/combined_sii_merged_filtered.parquet

Outputs:
  data/clean/rut_unidad_sector_crosswalk.parquet
  data/clean/rut_unidad_sector_crosswalk.csv
  data/clean/combined_sii_merged_filtered.parquet   (updated in place)

Method:
  1) From licitaciones panel, compute dominant sector by `RutUnidad`
  2) For Compra Ágil rows with missing `sector`, fill from dominant-sector map
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[2]
DATA_CLEAN = ROOT / "data" / "clean"

LIC_PANEL = DATA_CLEAN / "chilecompra_panel.parquet"
TARGET_FILE = DATA_CLEAN / "combined_sii_merged_filtered.parquet"
CROSSWALK_PARQUET = DATA_CLEAN / "rut_unidad_sector_crosswalk.parquet"
CROSSWALK_CSV = DATA_CLEAN / "rut_unidad_sector_crosswalk.csv"

BATCH_SIZE = 500_000
BAD_STRINGS = {"", "none", "nan", "null", "nat"}


def clean_str_series(s: pd.Series) -> pd.Series:
    """Normalize text keys and convert empty/null-like strings to missing."""
    out = s.astype("string").str.strip()
    out = out.mask(out.str.lower().isin(BAD_STRINGS))
    return out


def build_crosswalk() -> pd.DataFrame:
    print("=" * 70)
    print("STEP 1 — Build rut_unidad → sector crosswalk from licitaciones")
    print("=" * 70)

    if not LIC_PANEL.exists():
        raise FileNotFoundError(f"Missing input: {LIC_PANEL}")

    lic = pd.read_parquet(LIC_PANEL, columns=["RutUnidad", "sector"])
    lic["rut_unidad"] = clean_str_series(lic["RutUnidad"])
    lic["sector"] = clean_str_series(lic["sector"])
    lic = lic.dropna(subset=["rut_unidad", "sector"]).copy()

    counts = (
        lic.groupby(["rut_unidad", "sector"], dropna=False)
        .size()
        .rename("sector_obs_n")
        .reset_index()
    )
    n_sector = (
        counts.groupby("rut_unidad")["sector"]
        .nunique()
        .rename("n_sector_labels")
        .reset_index()
    )

    # Deterministic tie-breaker: highest count, then lexicographic sector.
    cross = (
        counts.sort_values(["rut_unidad", "sector_obs_n", "sector"], ascending=[True, False, True])
        .groupby("rut_unidad", as_index=False)
        .head(1)
        .merge(n_sector, on="rut_unidad", how="left")
    )
    cross["is_ambiguous"] = cross["n_sector_labels"] > 1
    cross = cross.rename(columns={"sector": "sector_from_rutunidad"})
    cross = cross[["rut_unidad", "sector_from_rutunidad", "sector_obs_n", "n_sector_labels", "is_ambiguous"]]

    cross.to_parquet(CROSSWALK_PARQUET, index=False)
    cross.to_csv(CROSSWALK_CSV, index=False)

    print(f"  Lic rows used: {len(lic):,}")
    print(f"  Unique rut_unidad in crosswalk: {cross['rut_unidad'].nunique():,}")
    amb_n = int(cross["is_ambiguous"].sum())
    print(f"  Ambiguous-unit count (>1 observed sector): {amb_n:,} ({100*amb_n/max(len(cross),1):.2f}%)")
    print(f"  Saved: {CROSSWALK_PARQUET}")
    print(f"  Saved: {CROSSWALK_CSV}")

    return cross


def fill_target_file(crosswalk: pd.DataFrame) -> None:
    print("\n" + "=" * 70)
    print("STEP 2 — Fill Compra Ágil sector in combined_sii_merged_filtered")
    print("=" * 70)

    if not TARGET_FILE.exists():
        raise FileNotFoundError(f"Missing target file: {TARGET_FILE}")

    map_series = crosswalk.set_index("rut_unidad")["sector_from_rutunidad"]

    pf = pq.ParquetFile(TARGET_FILE)
    schema = pf.schema_arrow
    names = schema.names

    required = ["dataset", "rut_unidad", "sector"]
    missing = [c for c in required if c not in names]
    if missing:
        raise ValueError(f"Target missing required columns: {missing}")

    idx_dataset = names.index("dataset")
    idx_rut = names.index("rut_unidad")
    idx_sector = names.index("sector")
    sector_type = schema.field(idx_sector).type

    tmp_file = TARGET_FILE.with_name(f"{TARGET_FILE.stem}.tmp.parquet")
    if tmp_file.exists():
        tmp_file.unlink()

    writer = pq.ParquetWriter(tmp_file, schema, compression="snappy")

    rows_total = 0
    ca_rows = 0
    ca_missing_sector_before = 0
    ca_filled = 0

    for i, batch in enumerate(pf.iter_batches(batch_size=BATCH_SIZE), 1):
        n = batch.num_rows
        rows_total += n

        ds = batch.column(idx_dataset).to_pandas()
        rut = batch.column(idx_rut).to_pandas()
        sec = batch.column(idx_sector).to_pandas()

        ds = ds.astype("string")
        rut_clean = clean_str_series(pd.Series(rut))
        sec_clean = clean_str_series(pd.Series(sec))

        is_ca = ds.eq("compra_agil").fillna(False)
        need_fill = is_ca & sec_clean.isna()
        mapped = rut_clean.map(map_series)
        fill_mask = need_fill & mapped.notna()

        sec_new = pd.Series(sec, copy=True)
        sec_new.loc[fill_mask] = mapped.loc[fill_mask]

        ca_rows += int(is_ca.sum())
        ca_missing_sector_before += int(need_fill.sum())
        ca_filled += int(fill_mask.sum())

        sec_arr = pa.array(sec_new.where(pd.notna(sec_new), None), type=sector_type)
        cols = [batch.column(j) if j != idx_sector else sec_arr for j in range(batch.num_columns)]
        writer.write_batch(pa.RecordBatch.from_arrays(cols, names=names))

        if i % 20 == 0:
            print(f"  processed batch {i}  ({rows_total:,} rows)")

    writer.close()

    # Atomic replace (os.replace works without separate unlink permission)
    import os
    os.replace(tmp_file, TARGET_FILE)

    print(f"  Rows processed: {rows_total:,}")
    print(f"  Compra Ágil rows: {ca_rows:,}")
    print(
        f"  Compra Ágil rows missing sector before fill: {ca_missing_sector_before:,} "
        f"({100*ca_missing_sector_before/max(ca_rows,1):.2f}%)"
    )
    print(
        f"  Filled from crosswalk: {ca_filled:,} "
        f"({100*ca_filled/max(ca_missing_sector_before,1):.2f}% of missing)"
    )
    print(f"  Updated: {TARGET_FILE}")


def main() -> None:
    cross = build_crosswalk()
    fill_target_file(cross)
    print("\nDone.")


if __name__ == "__main__":
    main()
