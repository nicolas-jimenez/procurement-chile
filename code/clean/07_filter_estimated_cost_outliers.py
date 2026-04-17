"""
clean/07_filter_estimated_cost_outliers.py
─────────────────────────────────────────────────────────────────────────────
Post-combine outlier filtering for estimated costs.

Input:
  data/clean/combined_sii_merged.parquet

Output:
  data/clean/combined_sii_merged_filtered.parquet

Rule:
  For each dataset separately, set `monto_estimado` outside the
  [P0.05, P99.95] interval to missing.

Cutoffs are estimated from the cleaned source panels:
  licitaciones → data/clean/chilecompra_panel.parquet      (MontoEstimado)
  compra_agil  → data/clean/compra_agil_panel.parquet      (MontoTotalDisponble)
"""

import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config import DATA_CLEAN  # noqa: E402

LIC_PANEL = DATA_CLEAN / "chilecompra_panel.parquet"
CA_PANEL = DATA_CLEAN / "compra_agil_panel.parquet"
IN_FILE = DATA_CLEAN / "combined_sii_merged.parquet"
OUT_FILE = DATA_CLEAN / "combined_sii_merged_filtered.parquet"

LOW_Q = 0.0005   # 0.05%
HIGH_Q = 0.9995  # 99.95%
BATCH_SIZE = 500_000


def _as_float(arr: pa.ChunkedArray) -> pa.ChunkedArray:
    return pc.cast(arr, pa.float64(), safe=False)


def quantile_stats(panel_file: Path, column: str) -> dict:
    if not panel_file.exists():
        raise FileNotFoundError(f"Missing panel file: {panel_file}")
    table = pq.read_table(panel_file, columns=[column])
    vals = _as_float(table[column])
    valid = pc.drop_null(vals)
    n_valid = int(pc.count(vals).as_py() or 0)
    if n_valid == 0:
        raise ValueError(f"No non-null values in {panel_file.name}:{column}")

    qs = pc.quantile(valid, q=[LOW_Q, 0.5, HIGH_Q]).to_pylist()
    return {
        "n_valid": n_valid,
        "min": float(pc.min(valid).as_py()),
        "p_low": float(qs[0]),
        "p50": float(qs[1]),
        "p_high": float(qs[2]),
        "max": float(pc.max(valid).as_py()),
    }


def bsum(mask: pa.Array) -> int:
    """Sum boolean mask with nulls treated as False."""
    return int(pc.sum(pc.fill_null(mask, False)).as_py() or 0)


def main() -> None:
    print("=" * 70)
    print("STEP 1 — Compute outlier cutoffs from clean panels")
    print("=" * 70)

    lic_stats = quantile_stats(LIC_PANEL, "MontoEstimado")
    ca_stats = quantile_stats(CA_PANEL, "MontoTotalDisponble")

    print("  licitaciones (MontoEstimado)")
    print(
        f"    n={lic_stats['n_valid']:,}  min={lic_stats['min']:.2f}  "
        f"p0.05={lic_stats['p_low']:.2f}  p50={lic_stats['p50']:.2f}  "
        f"p99.95={lic_stats['p_high']:.2f}  max={lic_stats['max']:.2f}"
    )
    print("  compra_agil (MontoTotalDisponble)")
    print(
        f"    n={ca_stats['n_valid']:,}  min={ca_stats['min']:.2f}  "
        f"p0.05={ca_stats['p_low']:.2f}  p50={ca_stats['p50']:.2f}  "
        f"p99.95={ca_stats['p_high']:.2f}  max={ca_stats['max']:.2f}"
    )

    print("\n" + "=" * 70)
    print("STEP 2 — Apply cutoffs to combined parquet")
    print("=" * 70)

    if not IN_FILE.exists():
        raise FileNotFoundError(f"Missing combined input: {IN_FILE}")

    pf = pq.ParquetFile(IN_FILE)
    schema = pf.schema_arrow
    names = schema.names
    if "dataset" not in names or "monto_estimado" not in names:
        raise ValueError("combined_sii_merged.parquet missing required columns: dataset, monto_estimado")

    ds_idx = names.index("dataset")
    amt_idx = names.index("monto_estimado")
    amt_type = schema.field(amt_idx).type

    # Write to temp path first, then atomically replace to avoid unlink permission issues
    tmp_out = OUT_FILE.with_suffix(".parquet.tmp")
    if tmp_out.exists():
        try:
            tmp_out.unlink()
        except PermissionError:
            pass
    writer = pq.ParquetWriter(tmp_out, schema, compression="snappy")

    stats = {
        "licitaciones": {"rows": 0, "non_null_before": 0, "set_to_nan": 0},
        "compra_agil": {"rows": 0, "non_null_before": 0, "set_to_nan": 0},
    }

    for i, batch in enumerate(pf.iter_batches(batch_size=BATCH_SIZE), 1):
        ds = batch.column(ds_idx)
        amt = _as_float(batch.column(amt_idx))
        valid_amt = pc.is_valid(amt)

        is_lic = pc.fill_null(pc.equal(ds, "licitaciones"), False)
        is_ca = pc.fill_null(pc.equal(ds, "compra_agil"), False)

        stats["licitaciones"]["rows"] += bsum(is_lic)
        stats["compra_agil"]["rows"] += bsum(is_ca)
        stats["licitaciones"]["non_null_before"] += bsum(pc.and_(is_lic, valid_amt))
        stats["compra_agil"]["non_null_before"] += bsum(pc.and_(is_ca, valid_amt))

        lic_out = pc.and_(
            pc.and_(is_lic, valid_amt),
            pc.or_(pc.less(amt, lic_stats["p_low"]), pc.greater(amt, lic_stats["p_high"])),
        )
        ca_out = pc.and_(
            pc.and_(is_ca, valid_amt),
            pc.or_(pc.less(amt, ca_stats["p_low"]), pc.greater(amt, ca_stats["p_high"])),
        )
        out_mask = pc.or_(lic_out, ca_out)

        stats["licitaciones"]["set_to_nan"] += bsum(lic_out)
        stats["compra_agil"]["set_to_nan"] += bsum(ca_out)

        amt_new = pc.if_else(out_mask, pa.scalar(None, type=pa.float64()), amt)
        if amt_type != pa.float64():
            amt_new = pc.cast(amt_new, amt_type, safe=False)

        cols = [batch.column(j) if j != amt_idx else amt_new for j in range(batch.num_columns)]
        writer.write_batch(pa.RecordBatch.from_arrays(cols, names=names))

        if i % 20 == 0:
            print(f"  processed batch {i}")

    writer.close()

    # Swap tmp -> final (os.replace is atomic and doesn't need unlink perm)
    import os
    os.replace(tmp_out, OUT_FILE)

    print("\n" + "=" * 70)
    print("STEP 3 — Before/after summary")
    print("=" * 70)
    for ds in ["licitaciones", "compra_agil"]:
        before = stats[ds]["non_null_before"]
        dropped = stats[ds]["set_to_nan"]
        after = before - dropped
        pct = 100 * dropped / max(before, 1)
        print(
            f"  {ds}: non-null before={before:,}  set_to_nan={dropped:,} "
            f"({pct:.3f}%)  non-null after={after:,}"
        )

    out_meta = pq.read_metadata(OUT_FILE)
    print(f"\n  Output: {OUT_FILE}")
    print(f"  Rows  : {out_meta.num_rows:,}")
    print(f"  Size  : {OUT_FILE.stat().st_size/1e9:.3f} GB")
    print("\nDone.")


if __name__ == "__main__":
    main()
