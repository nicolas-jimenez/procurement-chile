"""
03_clean_compra_agil.py
─────────────────────────────────────────────────────────────────────────────
Load, clean, and append all Compra Ágil (cotización) monthly files (2022–2025).

Raw layout
──────────────────────────────────────────────────────────────────────────────
  data/raw/chilecompra/compra_agil/
      COT_YYYY-MM/
          COT1_YYYY-MM.csv   ← first batch
          COT2_YYYY-MM.csv   ← second batch (same schema)

One row = one bidder response to one cotización item.
Key columns
  CodigoCotizacion  – unique cotización ID (≈ Codigo in licitaciones)
  RUTProveedor      – bidder RUT  (≈ RutProveedor in licitaciones)
  RUTUnidaddeCompra – buyer  RUT  (≈ RutUnidad in licitaciones)
  Region            – buyer region
  Tamano            – "MiPyme" | "Grande"
  ProveedorSeleccionado – "si" | "no"
  MontoTotal        – bid amount (CLP)
  MontoTotalDisponble   – estimated/available budget (CLP)
  Estado            – tender status

Cleaning steps
──────────────────────────────────────────────────────────────────────────────
  1. Encoding      : latin-1 + CP1252 fixup
  2. Source tag    : source_year, source_month
  3. Dates         : FechaPublicacionParaCotizar, FechaCierreParaCotizar → datetime
  4. Numerics      : MontoTotal, MontoTotalDisponble, CantidadSolicitada (comma-decimal)
  5. Normalise     : ProveedorSeleccionado → bool (is_selected)
                     Tamano → canonical {"MiPyme","Grande",None}
  6. Duplicates    : drop exact duplicates; flag key-dups on
                     (CodigoCotizacion × RUTProveedor × CodigoProducto)
  7. is_key_dup    : bool flag (like licitaciones pipeline)

Output
──────────────────────────────────────────────────────────────────────────────
  data/clean/compra_agil_panel.parquet
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

ROOT    = Path(__file__).resolve().parents[2]
# NOTE: The raw compra_agil files were moved to:
# /Users/nicolasjimenez/Library/CloudStorage/OneDrive-YaleUniversity/procurement-spillovers-onedrive/data/raw/chilecompra/compra_agil
# Keep this local path synced from that location.
RAW_DIR = ROOT / "data" / "raw" / "chilecompra" / "compra_agil"
OUT_DIR = ROOT / "data" / "clean"
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_PARQUET = OUT_DIR / "compra_agil_panel.parquet"

# ── CP1252 fixup (same as licitaciones pipeline) ───────────────────────────
_CP1252_TRANS: dict = {}
for _b in range(0x80, 0xA0):
    _latin1 = bytes([_b]).decode("latin-1")
    try:
        _cp1252 = bytes([_b]).decode("cp1252")
        if _latin1 != _cp1252:
            _CP1252_TRANS[_latin1] = _cp1252
    except (UnicodeDecodeError, ValueError):
        pass
_CP1252_TABLE = str.maketrans(_CP1252_TRANS)

def fix_cp1252(s):
    return s.translate(_CP1252_TABLE) if isinstance(s, str) else s

DATE_COLS    = ["FechaPublicacionParaCotizar", "FechaCierreParaCotizar"]
AMOUNT_COLS  = ["MontoTotal", "MontoTotalDisponble", "CantidadSolicitada"]
KEY_COLS     = ["CodigoCotizacion", "RUTProveedor", "CodigoProducto"]

def parse_comma_decimal(series: pd.Series) -> pd.Series:
    def _fix(val):
        if pd.isna(val): return val
        val = str(val).strip()
        if val in ("nan", "None", "NA", ""): return np.nan
        n_commas = val.count(",")
        if n_commas == 0:   return val
        elif n_commas == 1: return val.replace(",", ".")
        else:
            parts = val.rsplit(",", 1)
            return parts[0].replace(",","") + "." + parts[1]
    s = series.copy().astype(str).str.strip()
    s = s.replace({"nan": np.nan, "None": np.nan, "NA": np.nan, "": np.nan})
    s = s.map(_fix)
    return pd.to_numeric(s, errors="coerce")


def clean_file(csv_path: Path, year: int, month: int):
    df = pd.read_csv(csv_path, sep=";", encoding="latin-1",
                     low_memory=False, quotechar='"')

    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].map(fix_cp1252)

    df.insert(0, "source_year",  np.int16(year))
    df.insert(1, "source_month", np.int8(month))

    # Dates
    for col in DATE_COLS:
        if col in df.columns:
            s = df[col].astype(str).str.strip()
            s = s.replace({"nan": np.nan, "None": np.nan, "": np.nan})
            df[col] = pd.to_datetime(s, errors="coerce")

    # Numerics
    for col in AMOUNT_COLS:
        if col in df.columns and df[col].dtype == object:
            df[col] = parse_comma_decimal(df[col])

    # Normalise ProveedorSeleccionado → bool
    if "ProveedorSeleccionado" in df.columns:
        df["is_selected"] = df["ProveedorSeleccionado"].str.strip().str.lower() == "si"
        df.drop(columns=["ProveedorSeleccionado"], inplace=True)

    # Tamano → canonical
    if "Tamano" in df.columns:
        df["Tamano"] = df["Tamano"].str.strip().str.title()
        df["Tamano"] = df["Tamano"].where(df["Tamano"].isin(["Mipyme","Grande"]), other=np.nan)
        # Normalise "Mipyme" → "MiPyme"
        df["Tamano"] = df["Tamano"].replace({"Mipyme": "MiPyme"})

    # Literal "NA" strings → NaN
    for col in df.select_dtypes(include="object").columns:
        df.loc[df[col].astype(str).str.strip().str.upper() == "NA", col] = np.nan

    # Dedup exact rows
    n_before = len(df)
    df = df.drop_duplicates()
    n_full_dropped = n_before - len(df)

    # Flag key-duplicates
    key_present = [c for c in KEY_COLS if c in df.columns]
    df["is_key_dup"] = df.duplicated(subset=key_present, keep=False) if key_present else False

    return df, n_full_dropped


# ── Discover files ─────────────────────────────────────────────────────────
all_csvs = sorted(RAW_DIR.rglob("COT*.csv"))
print(f"Found {len(all_csvs)} CSV files across {len(list(RAW_DIR.iterdir()))} months\n")

# ── Incremental write ──────────────────────────────────────────────────────
writer         = None
ref_schema     = None
total_rows     = 0
total_key_dups = 0
year_counts: dict = {}

for i, csv_path in enumerate(all_csvs, 1):
    folder = csv_path.parent.name          # e.g. COT_2022-01
    parts  = folder.replace("COT_","").split("-")
    year, month = int(parts[0]), int(parts[1])
    label  = f"{year}-{month:02d}  ({csv_path.name})"

    print(f"[{i:>2}/{len(all_csvs)}] {label} ...", end=" ", flush=True)

    df, dropped = clean_file(csv_path, year, month)
    key_dups    = int(df["is_key_dup"].sum())

    # Build Arrow table
    table = pa.Table.from_pandas(df, preserve_index=False)

    if ref_schema is None:
        ref_schema = table.schema

    # Cast to reference schema (coerce mismatched types)
    try:
        table = table.cast(ref_schema)
    except pa.ArrowInvalid:
        # Schema drift: use safe_cast column-by-column
        arrays = []
        for field in ref_schema:
            if field.name in df.columns:
                try:
                    arrays.append(pa.array(df[field.name], type=field.type, from_pandas=True))
                except Exception:
                    arrays.append(pa.array([None]*len(df), type=field.type))
            else:
                arrays.append(pa.array([None]*len(df), type=field.type))
        table = pa.Table.from_arrays(arrays, schema=ref_schema)

    if writer is None:
        writer = pq.ParquetWriter(OUT_PARQUET, ref_schema, compression="snappy")
    writer.write_table(table)

    total_rows     += len(df)
    total_key_dups += key_dups
    year_counts[year] = year_counts.get(year, 0) + len(df)

    print(f"{len(df):>8,} rows  |  full_dup_dropped={dropped:,}  |  key_dup_flagged={key_dups:,}")
    del df, table

if writer:
    writer.close()
    print(f"\nParquet written → {OUT_PARQUET}")
else:
    print("No files processed."); sys.exit(1)

# ── Summary ────────────────────────────────────────────────────────────────
n_cols = len(pq.read_schema(OUT_PARQUET).names)
print("\n" + "=" * 70)
print("COMPRA ÁGIL PANEL SUMMARY")
print("=" * 70)
print(f"  Total rows          : {total_rows:,}")
print(f"  Total columns       : {n_cols}")
print(f"  Key-duplicate rows  : {total_key_dups:,} ({100*total_key_dups/total_rows:.1f}%)")
print(f"\n  Rows by year:")
for yr, cnt in sorted(year_counts.items()):
    print(f"    {yr}: {cnt:>12,}")
print(f"\n  Output: {OUT_PARQUET}")
