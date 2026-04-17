"""
Verify the rebuilt combined_sii_merged_filtered.parquet:
  • shape, dataset row counts, key columns present
  • non-null coverage of bid / product / criterio fields, by dataset & period
  • sanity check on monto_total_oferta and monto_unit_oferta for licit
  • is_single_line distribution for CA
  • crit / criterios coverage pre vs post 2024-12-12

Uses DuckDB so we never load the 21 GB parquet into pandas.
"""
import sys
from pathlib import Path
import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config import DATA_CLEAN  # noqa: E402

F = DATA_CLEAN / "combined_sii_merged_filtered.parquet"
print(f"File: {F}  ({F.stat().st_size/1e9:.2f} GB)")

con = duckdb.connect()
con.execute("PRAGMA memory_limit='2GB'")
con.execute("PRAGMA threads=2")
con.execute(f"CREATE VIEW p AS SELECT * FROM read_parquet('{F}')")

# 1) Shape + columns ---------------------------------------------------------
n_rows = con.execute("SELECT COUNT(*) FROM p").fetchone()[0]
cols = [r[0] for r in con.execute("DESCRIBE p").fetchall()]
print(f"\nRows: {n_rows:,}   Cols: {len(cols)}")

print("\nDataset counts:")
for ds, n in con.execute(
    "SELECT dataset, COUNT(*) FROM p GROUP BY dataset ORDER BY 1"
).fetchall():
    print(f"  {ds:<12} {n:>14,}")

# 2) Required columns sanity --------------------------------------------------
must = [
    "dataset", "tender_id", "rut_buyer", "rut_bidder", "fecha_pub",
    "monto_estimado", "monto_oferta", "monto_total_oferta", "monto_unit_oferta",
    "cantidad_solicitada", "cantidad_ofertada", "codigo_producto",
    "criterios_evaluacion", "n_criterios_eval", "is_single_line",
    "n_oferentes", "oferta_seleccionada", "monto_adjudicado",
    "region", "region_buyer", "same_region", "sector",
]
missing = [c for c in must if c not in cols]
print(f"\nRequired cols missing: {missing if missing else 'none'}")

# 3) Coverage by (dataset, period) -------------------------------------------
print("\nNon-null coverage by dataset × period (pre/post 2024-12-12):")
fields = [
    "monto_oferta", "monto_total_oferta", "monto_unit_oferta",
    "cantidad_solicitada", "cantidad_ofertada", "codigo_producto",
    "criterios_evaluacion", "n_criterios_eval", "is_single_line",
    "n_oferentes", "monto_adjudicado", "sector",
]
for f in fields:
    if f not in cols:
        print(f"  [skip] {f} not in panel")
        continue
    rows = con.execute(f"""
      SELECT dataset,
             CASE WHEN fecha_pub < DATE '2024-12-12' THEN 'pre' ELSE 'post' END AS period,
             COUNT(*) AS n,
             COUNT({f}) AS nn
      FROM p
      WHERE fecha_pub IS NOT NULL
      GROUP BY 1,2
      ORDER BY 1,2
    """).fetchall()
    print(f"\n  {f}")
    for ds, per, n, nn in rows:
        pct = 100 * nn / max(n, 1)
        print(f"    {ds:<12} {per:<5} {nn:>12,}/{n:>12,}  ({pct:5.1f}%)")

# 4) Distribution of n_lines_cot / is_single_line for CA ---------------------
if "is_single_line" in cols:
    print("\nCA is_single_line distribution:")
    for v, n in con.execute("""
      SELECT is_single_line, COUNT(*)
      FROM p WHERE dataset='compra_agil'
      GROUP BY 1 ORDER BY 1 NULLS LAST
    """).fetchall():
        print(f"  {str(v):<6} {n:>14,}")

# 5) monto_unit_oferta sanity -------------------------------------------------
print("\nmonto_unit_oferta presence by dataset:")
for ds, n_total, n_nn, mn, p50, mx in con.execute("""
  SELECT dataset, COUNT(*), COUNT(monto_unit_oferta),
         MIN(monto_unit_oferta), median(monto_unit_oferta), MAX(monto_unit_oferta)
  FROM p WHERE monto_unit_oferta IS NOT NULL
  GROUP BY 1 ORDER BY 1
""").fetchall():
    print(f"  {ds:<12} n_total_with={n_total:>12,}  min={mn}  median={p50}  max={mx}")

# 6) Same-region pre/post -----------------------------------------------------
if "same_region" in cols:
    print("\nsame_region rate by dataset × period:")
    rows = con.execute("""
      SELECT dataset,
             CASE WHEN fecha_pub < DATE '2024-12-12' THEN 'pre' ELSE 'post' END AS period,
             AVG(CAST(same_region AS DOUBLE)) AS rate,
             COUNT(*)
      FROM p
      WHERE same_region IS NOT NULL AND fecha_pub IS NOT NULL
      GROUP BY 1,2 ORDER BY 1,2
    """).fetchall()
    for ds, per, rate, n in rows:
        print(f"  {ds:<12} {per:<5} {rate:.4f}  (n={n:,})")

print("\nDone.")
