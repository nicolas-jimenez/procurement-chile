"""
clean/06_combine_sii_merged.py
─────────────────────────────────────────────────────────────────────────────
Concatenate the two separately SII-merged files into a single parquet.

Input:  data/clean/licitaciones_sii_merged.parquet
        data/clean/compra_agil_sii_merged.parquet
Output: data/clean/combined_sii_merged.parquet

This is the step that runs after 04a and 04b (which can run in parallel).
"""

import sys
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config import DATA_CLEAN  # noqa: E402

LIC_FILE = DATA_CLEAN / "licitaciones_sii_merged.parquet"
CA_FILE  = DATA_CLEAN / "compra_agil_sii_merged.parquet"
OUT_FILE = DATA_CLEAN / "combined_sii_merged.parquet"

print("=" * 70)
print("STEP 1 — Check inputs")
print("=" * 70)

for f in [LIC_FILE, CA_FILE]:
    if not f.exists():
        raise FileNotFoundError(f"Missing input: {f}")
    meta = pq.read_metadata(f)
    print(f"  {f.name}: {meta.num_rows:,} rows, {f.stat().st_size/1e9:.3f} GB")

# ══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("STEP 2 — Inspect schemas")
print("=" * 70)

lic_schema = pq.read_schema(LIC_FILE)
ca_schema  = pq.read_schema(CA_FILE)

lic_fields = {f.name: f.type for f in lic_schema}
ca_fields  = {f.name: f.type for f in ca_schema}

lic_only = set(lic_fields) - set(ca_fields)
ca_only  = set(ca_fields)  - set(lic_fields)

if lic_only:
    print(f"  Columns only in licitaciones: {sorted(lic_only)}")
if ca_only:
    print(f"  Columns only in compra_agil:  {sorted(ca_only)}")

# Build merged schema: union of all fields; prefer licitaciones type for shared cols
all_field_names = list(lic_fields.keys()) + [c for c in ca_fields if c not in lic_fields]
merged_schema_fields = []
for name in all_field_names:
    if name in lic_fields:
        merged_schema_fields.append(pa.field(name, lic_fields[name]))
    else:
        merged_schema_fields.append(pa.field(name, ca_fields[name]))
merged_schema = pa.schema(merged_schema_fields)

print(f"\n  Merged schema: {len(merged_schema)} columns")

# ══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("STEP 3 — Concatenate (row-group streaming)")
print("=" * 70)

writer = pq.ParquetWriter(OUT_FILE, merged_schema, compression="snappy")
total_rows = 0

for label, src_file in [("licitaciones", LIC_FILE), ("compra_agil", CA_FILE)]:
    src_pf = pq.ParquetFile(src_file)
    n_batches = src_pf.metadata.num_row_groups
    print(f"\n  {label}: {n_batches} row groups")
    for i, batch in enumerate(src_pf.iter_batches(batch_size=500_000)):
        table = pa.Table.from_batches([batch])
        # Align to merged_schema: add missing columns as null, cast shared cols
        arrays = []
        for field in merged_schema:
            if field.name in table.schema.names:
                col = table.column(field.name)
                try:
                    col = col.cast(field.type)
                except Exception:
                    col = pa.array([None] * len(table), type=field.type)
            else:
                col = pa.array([None] * len(table), type=field.type)
            arrays.append(col)
        aligned = pa.Table.from_arrays(arrays, schema=merged_schema)
        writer.write_table(aligned)
        total_rows += len(aligned)
        if (i + 1) % 20 == 0 or (i + 1) == n_batches:
            print(f"    row group {i+1}/{n_batches}  ({total_rows:,} rows so far)")

writer.close()

# ══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("STEP 4 — Verify output")
print("=" * 70)

out_meta = pq.read_metadata(OUT_FILE)
print(f"  Rows written : {out_meta.num_rows:,}")
print(f"  Row groups   : {out_meta.num_row_groups}")
print(f"  File size    : {OUT_FILE.stat().st_size/1e9:.3f} GB")
print(f"  Output       : {OUT_FILE}")

# Quick dataset-level count
import pandas as pd
counts = pq.read_table(OUT_FILE, columns=["dataset"]).to_pandas()["dataset"].value_counts()
print(f"\n  Row counts by dataset:")
for ds, n in counts.sort_index().items():
    print(f"    {ds}: {n:,}")

print("\nDone.")
