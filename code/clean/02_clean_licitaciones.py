"""
Clean, standardize, and append all ChileCompra monthly files (2022–2025).
Output: data/clean/chilecompra_panel.parquet  (and optionally .csv)

Cleaning steps applied to every file
──────────────────────────────────────
 1. ENCODING        — read as latin-1; fix embedded CP1252 "smart" chars
                      (\x80–\x9f range: curly quotes, em-dash, ellipsis, etc.)
                      that ChileCompra embeds in otherwise latin-1 text fields
 2. SCHEMA ALIGN    — V1 (105 cols, ≤2024-11) vs V2 (111 cols, ≥2024-12);
                      pad V1 rows with NaN for the 6 new V2 columns
 3. SENTINEL DATES  — replace '1900-01-01' strings → NaT, then parse all
                      date columns to datetime64[ns]
 4. NUMERIC STRINGS — comma-decimal + Spanish sci-notation (e.g. "3,3e+08")
                      → float64; done for all amount/quantity columns
 5. RFB_* CODES     — decode to human-readable labels in time-unit, payment,
                      and duration columns
 6. SENTINEL INTS   — -1 in UnidadTiempo / TipoPago → NaN
 7. ID COLUMNS      — CodigoProveedor, CodigoSucursalProveedor: float→Int64
 8. 'NA' STRINGS    — literal "NA" object strings → NaN
 9. DUPLICATES      — drop fully identical rows; flag key-duplicates
                      (Codigo × Correlativo × CodigoProveedor) with
                      boolean column `is_key_dup`
10. SOURCE TAG      — add `source_year` and `source_month` from folder name
11. APPEND          — stream each cleaned month to a single parquet file
                      (incremental write, no full panel held in RAM)

Usage
──────
    python code/clean/02_clean_licitaciones.py            # parquet only
    python code/clean/02_clean_licitaciones.py --csv      # also write .csv
    python code/clean/02_clean_licitaciones.py --months 2024_7 2023_1  # subset
"""

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# ── CLI ────────────────────────────────────────────────────────────────────────
parser = argparse.ArgumentParser()
parser.add_argument("--csv",    action="store_true", help="Also write a .csv output")
parser.add_argument("--months", nargs="*", default=None,
                    help="Process only these YYYY_M folders, e.g. 2024_7 2023_1")
args = parser.parse_args()

# ── Paths ──────────────────────────────────────────────────────────────────────
ROOT        = Path(__file__).resolve().parents[2]
DATA_DIR    = ROOT / "data" / "raw" / "chilecompra" / "licitaciones"
OUT_DIR     = ROOT / "data" / "clean"
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_PARQUET = OUT_DIR / "chilecompra_panel.parquet"
OUT_CSV     = OUT_DIR / "chilecompra_panel.csv"

# ── Constants ─────────────────────────────────────────────────────────────────
SENTINEL_DATE = "1900-01-01"

# 6 columns added in V2 (2024-12 onward) — always string type
V2_EXTRA_COLS = [
    "CriteriosAmbientales",
    "DescripcionCriteriosAmbientales",
    "CriteriosSociales",
    "DescripcionCriteriosSociales",
    "rbhDescripcionCriteriosSociales",
    "CriteriosEvaluacion",
]

# Date columns (object strings in raw file → datetime64 after cleaning)
DATE_COLS = [
    "FechaCreacion", "FechaCierre", "FechaInicio", "FechaFinal",
    "FechaPubRespuestas", "FechaActoAperturaTecnica", "FechaActoAperturaEconomica",
    "FechaPublicacion", "FechaAdjudicacion", "FechaEstimadaAdjudicacion",
    "FechaSoporteFisico", "FechaEstimadaFirma", "FechaVisitaTerreno",
    "FechaEntregaAntecedentes", "FechaAprobacion", "FechaEnvioOferta",
]

# Columns with Spanish-locale numbers (comma decimal / sci notation)
COMMA_NUMERIC_COLS = [
    "MontoEstimado",
    "Monto Estimado Adjudicado",
    "MontoUnitarioOferta",
    "Valor Total Ofertado",
    "MontoLineaAdjudica",
    "Cantidad",
    "Cantidad Ofertada",
    "CantidadAdjudicada",
]

# RFB decode maps
RFB_TIME_MAP = {
    "RFB_TIME_PERIOD_HOURS":  "Horas",
    "RFB_TIME_PERIOD_DAYS":   "Días",
    "RFB_TIME_PERIOD_WEEKS":  "Semanas",
    "RFB_TIME_PERIOD_MONTHS": "Meses",
}
RFB_PAYMENT_MAP = {
    "RFB_CONTRACT_PAYMENT_METHOD_30_DAYS": "30 días",
    "RFB_CONTRACT_PAYMENT_METHOD_45_DAYS": "45 días",
    "RFB_CONTRACT_PAYMENT_METHOD_OTHERS":  "Otro",
}
RFB_DURATION_MAP = {
    "RFB_CONTRACT_TIME_PERIOD_INMEDIATE_EXECUTION":  "Ejecución inmediata",
    "RFB_CONTRACT_TIME_PERIOD_ALONG_TIME_EXECUTION": "Ejecución en el tiempo",
}
TIPO_PAGO_MAP = {1: "Contra entrega", 2: "Por estado de avance",
                 3: "Otro", 4: "No aplica"}

# Natural key for duplicate detection
KEY_COLS  = ["Codigo", "Correlativo", "CodigoProveedor"]

# ── CP1252 fixup ───────────────────────────────────────────────────────────────
# The files are Latin-1 (ISO-8859-1) but ChileCompra occasionally embeds bytes
# from the CP1252 "Windows extras" range (\x80–\x9f).  In Latin-1 those bytes
# are control characters with no glyph; CP1252 maps them to printable glyphs
# (curly quotes, em-dash, bullet, ellipsis, …).  We read as latin-1 and then
# remap those control bytes to their CP1252 unicode equivalents so the strings
# come out clean.  The five bytes undefined in CP1252 (0x81,0x8d,0x8f,0x90,0x9d)
# are left as-is (they appear extremely rarely and have no meaningful mapping).
_CP1252_TRANS: dict = {}
for _b in range(0x80, 0xA0):
    _latin1 = bytes([_b]).decode("latin-1")
    try:
        _cp1252 = bytes([_b]).decode("cp1252")
        if _latin1 != _cp1252:               # only remap where cp1252 differs
            _CP1252_TRANS[_latin1] = _cp1252
    except (UnicodeDecodeError, ValueError):
        pass                                 # undefined in cp1252 — skip
_CP1252_TABLE = str.maketrans(_CP1252_TRANS)


def fix_cp1252(s: str) -> str:
    """Remap embedded CP1252 control bytes to their proper Unicode glyphs."""
    return s.translate(_CP1252_TABLE) if isinstance(s, str) else s

# Always-null column to drop
DROP_COLS = ["PeriodoTiempoRenovacion"]

# Explicit pandas dtypes to enforce on every file before writing to parquet.
# This guarantees a consistent Arrow schema across all row-groups regardless
# of whether a column happens to be all-NaN in a given month.
DTYPE_MAP: dict = {
    # --- source tags ---
    "source_year":  "int16",
    "source_month": "int8",
    # --- tender header int ids ---
    "Codigo":            "int64",
    "CodigoEstado":      "int64",
    "CodigoOrganismo":   "int64",
    "CodigoUnidad":      "int64",
    "CodigoTipo":        "int64",
    "TipoConvocatoria":  "int64",
    "Etapas":            "int64",
    "EstadoEtapas":      "int64",
    "TomaRazon":         "int64",
    "EstadoPublicidadOfertas": "int64",
    "EstadoCS":          "int64",
    "Obras":             "int64",
    "CantidadReclamos":  "int64",
    "Informada":         "int64",
    "VisibilidadMonto":  "int64",
    "SubContratacion":   "int64",
    "ExtensionPlazo":    "int64",
    "EsBaseTipo":        "int64",
    "UnidadTiempoContratoLicitacion": "int64",
    "ValorTiempoRenovacion":          "int64",
    "EsRenovable":       "int64",
    "NumeroOferentes":   "int64",
    "Correlativo":       "int64",
    "CodigoEstadoLicitacion": "int64",
    "Codigoitem":        "int64",
    "CodigoProductoONU": "int64",
    "FechaTiempoEvaluacion": "int64",
    "FechasUsuario":     "int64",
    "UnidadTiempoDuracionContrato": "int64",
    "TiempoDuracionContrato":       "int64",
    # --- nullable int ids (have NaN rows) ---
    "CodigoProveedor":         "Int64",
    "CodigoSucursalProveedor": "Int64",
    # --- float columns ---
    "Contrato":                 "float64",
    "TipoAprobacion":           "float64",
    "Estimacion":               "float64",
    "Tiempo":                   "float64",
    "MontoEstimado":            "float64",
    "Monto Estimado Adjudicado":"float64",
    "MontoUnitarioOferta":      "float64",
    "Valor Total Ofertado":     "float64",
    "MontoLineaAdjudica":       "float64",
    "Cantidad":                 "float64",
    "Cantidad Ofertada":        "float64",
    "CantidadAdjudicada":       "float64",
    # --- bool ---
    "is_key_dup": "bool",
    # Everything else is object (string) — handled below
}


# ── Helper functions ───────────────────────────────────────────────────────────

def parse_comma_decimal(series: pd.Series) -> pd.Series:
    """
    Convert Spanish-locale numeric strings to float.
    Handles: "3,3e+08" → 3.3e8, "648372,0551" → 648372.0551
    """
    def _fix(val):
        if pd.isna(val):
            return val
        val = str(val).strip()
        if val in ("nan", "None", "NA", ""):
            return np.nan
        n_commas = val.count(",")
        if n_commas == 0:
            return val
        elif n_commas == 1:
            return val.replace(",", ".")
        else:
            parts = val.rsplit(",", 1)
            return parts[0].replace(",", "") + "." + parts[1]

    s = series.copy().astype(str).str.strip()
    s = s.replace({"nan": np.nan, "None": np.nan, "NA": np.nan, "": np.nan})
    s = s.map(_fix)
    return pd.to_numeric(s, errors="coerce")


def clean_sentinel_dates(df: pd.DataFrame) -> pd.DataFrame:
    for col in DATE_COLS:
        if col not in df.columns:
            continue
        s = df[col].astype(str).str.strip()
        s = s.replace({SENTINEL_DATE: pd.NaT, "nan": pd.NaT, "None": pd.NaT, "": pd.NaT})
        df[col] = pd.to_datetime(s, errors="coerce")
    return df


def decode_rfb(df: pd.DataFrame) -> pd.DataFrame:
    for col in ["UnidadTiempo", "UnidadTiempoEvaluacion"]:
        if col in df.columns:
            df[col] = df[col].map(lambda x: RFB_TIME_MAP.get(str(x), x) if pd.notna(x) else x)
            df[col] = df[col].replace({"-1": np.nan, -1: np.nan})
    if "Modalidad" in df.columns:
        df["Modalidad"] = df["Modalidad"].map(
            lambda x: RFB_PAYMENT_MAP.get(str(x), x) if pd.notna(x) else x)
        df["Modalidad"] = df["Modalidad"].replace({"0": np.nan, 0: np.nan})
    if "TipoDuracionContrato" in df.columns:
        df["TipoDuracionContrato"] = df["TipoDuracionContrato"].map(
            lambda x: RFB_DURATION_MAP.get(str(x), x) if pd.notna(x) else x)
    if "TipoPago" in df.columns:
        df["TipoPago"] = df["TipoPago"].map(
            lambda x: TIPO_PAGO_MAP.get(int(x), x)
            if pd.notna(x) and str(x) not in ("-1", "") else np.nan)
    return df


def enforce_dtypes(df: pd.DataFrame, canonical_cols: list) -> pd.DataFrame:
    """
    Reorder to canonical column list and enforce the DTYPE_MAP.
    Columns not in DTYPE_MAP are kept as object (string).
    """
    # Ensure every canonical column exists
    for col in canonical_cols:
        if col not in df.columns:
            df[col] = np.nan

    df = df[canonical_cols].copy()

    for col, dtype in DTYPE_MAP.items():
        if col not in df.columns:
            continue
        try:
            if dtype == "bool":
                df[col] = df[col].astype(bool)
            elif dtype == "Int64":
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
            elif dtype.startswith("int"):
                df[col] = pd.to_numeric(df[col], errors="coerce").astype(dtype)
            elif dtype == "float64":
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
        except Exception:
            pass  # leave as-is if cast fails; will be caught by pyarrow

    # Force V2 extra cols to object so pyarrow always infers string (not null/double)
    for col in V2_EXTRA_COLS:
        if col in df.columns and col not in DTYPE_MAP:
            df[col] = df[col].astype(object)

    return df


def clean_file(csv_path: Path, year: int, month: int):
    """Load and fully clean one monthly CSV. Returns (df, n_full_dropped)."""

    df = pd.read_csv(csv_path, sep=";", encoding="latin-1",
                     low_memory=False, quotechar='"')

    # Fix CP1252 "smart" chars embedded in otherwise latin-1 text columns
    # (e.g. \x96 → '–', \x93/\x94 → '"'/'"', \x91/\x92 → '''/''')
    for _col in df.select_dtypes(include="object").columns:
        df[_col] = df[_col].map(fix_cp1252)

    df.insert(0, "source_year",  year)
    df.insert(1, "source_month", month)

    # Pad V1 files with the 6 V2-only columns (as object so type is string)
    for col in V2_EXTRA_COLS:
        if col not in df.columns:
            df[col] = np.nan

    # Drop always-null columns
    for col in DROP_COLS:
        if col in df.columns:
            df.drop(columns=col, inplace=True)

    df = clean_sentinel_dates(df)

    for col in COMMA_NUMERIC_COLS:
        if col in df.columns and df[col].dtype == object:
            df[col] = parse_comma_decimal(df[col])

    df = decode_rfb(df)

    # CodigoProveedor / CodigoSucursalProveedor: float → Int64
    for col in ["CodigoProveedor", "CodigoSucursalProveedor"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # 'NA' literal strings → NaN
    for col in df.select_dtypes(include="object").columns:
        df.loc[df[col].astype(str).str.strip().str.upper() == "NA", col] = np.nan

    # Drop fully-duplicate rows
    n_before = len(df)
    df = df.drop_duplicates()
    n_full_dropped = n_before - len(df)

    # Flag key-duplicates
    key_present = [c for c in KEY_COLS if c in df.columns]
    df["is_key_dup"] = df.duplicated(subset=key_present, keep=False) if key_present else False

    return df, n_full_dropped


# ── Explicit Arrow schema (avoids null-type inference for all-NaN columns) ────
# pyarrow.Table.from_pandas infers `null` type for all-NaN object columns.
# We hard-code the target schema so every row-group has the same field types
# regardless of whether a column happened to be empty in that month.
def _pa_schema_from_canonical(cols: list) -> pa.Schema:
    """Return an explicit pa.Schema for the canonical column list."""
    # Mapping from column name → Arrow type
    int64  = pa.int64()
    int16  = pa.int16()
    int8   = pa.int8()
    f64    = pa.float64()
    ts     = pa.timestamp("ns")
    string = pa.string()
    bool_  = pa.bool_()

    TYPE_OVERRIDE = {
        "source_year":  int16,
        "source_month": int8,
        "Codigo":       int64, "CodigoEstado": int64, "CodigoOrganismo": int64,
        "CodigoUnidad": int64, "CodigoTipo": int64, "TipoConvocatoria": int64,
        "Etapas": int64, "EstadoEtapas": int64, "TomaRazon": int64,
        "EstadoPublicidadOfertas": int64, "EstadoCS": int64, "Obras": int64,
        "CantidadReclamos": int64, "Informada": int64, "VisibilidadMonto": int64,
        "SubContratacion": int64, "ExtensionPlazo": int64, "EsBaseTipo": int64,
        "UnidadTiempoContratoLicitacion": int64, "ValorTiempoRenovacion": int64,
        "EsRenovable": int64, "NumeroOferentes": int64, "Correlativo": int64,
        "CodigoEstadoLicitacion": int64, "Codigoitem": int64,
        "CodigoProductoONU": int64, "FechaTiempoEvaluacion": int64,
        "FechasUsuario": int64, "UnidadTiempoDuracionContrato": int64,
        "TiempoDuracionContrato": int64,
        # nullable ints stored as int64 in Arrow (pandas uses Int64)
        "CodigoProveedor": int64, "CodigoSucursalProveedor": int64,
        # floats
        "Contrato": f64, "TipoAprobacion": f64, "Estimacion": f64,
        "Tiempo": f64, "MontoEstimado": f64, "Monto Estimado Adjudicado": f64,
        "MontoUnitarioOferta": f64, "Valor Total Ofertado": f64,
        "MontoLineaAdjudica": f64, "Cantidad": f64,
        "Cantidad Ofertada": f64, "CantidadAdjudicada": f64,
        # dates
        **{c: ts for c in DATE_COLS},
        # bool
        "is_key_dup": bool_,
    }
    fields = []
    for col in cols:
        arrow_type = TYPE_OVERRIDE.get(col, string)  # default: string
        fields.append(pa.field(col, arrow_type))
    return pa.schema(fields)


# ── Build canonical column order from first V2 file header ────────────────────
_all_sorted = sorted(
    DATA_DIR.rglob("lic_*.csv"),
    key=lambda p: (int(p.parent.name.split("_")[0]),
                   int(p.parent.name.split("_")[1])),
)
_first_v2 = next(
    (p for p in _all_sorted
     if int(p.parent.name.split("_")[0]) * 100
        + int(p.parent.name.split("_")[1]) >= 202412),
    None,
)
if _first_v2:
    _v2_header = pd.read_csv(_first_v2, sep=";", encoding="latin-1", nrows=0)
    CANONICAL_COLS = (
        ["source_year", "source_month"]
        + [c for c in _v2_header.columns if c not in DROP_COLS]
        + ["is_key_dup"]
    )
else:
    CANONICAL_COLS = None   # will be derived from the first file processed


# ── File list ─────────────────────────────────────────────────────────────────
all_csvs = _all_sorted
if args.months:
    all_csvs = [p for p in all_csvs if p.parent.name in args.months]

total = len(all_csvs)
print(f"Files to process: {total}\n")

# ── Build explicit Arrow schema from canonical column list ────────────────────
if CANONICAL_COLS is not None:
    ref_schema = _pa_schema_from_canonical(CANONICAL_COLS)
else:
    ref_schema = None   # will be set after first file

# ── Incremental parquet write ─────────────────────────────────────────────────
writer         = None
total_rows     = 0
total_key_dups = 0
year_counts: dict = {}
date_min = date_max = None

for i, csv_path in enumerate(all_csvs, 1):
    folder = csv_path.parent.name
    year   = int(folder.split("_")[0])
    month  = int(folder.split("_")[1])
    label  = f"{year}-{month:02d}"

    print(f"[{i:>2}/{total}] {label} ...", end=" ", flush=True)

    df, dropped = clean_file(csv_path, year, month)
    key_dups    = int(df["is_key_dup"].sum())

    # Set canonical column list from first file if not yet known
    if CANONICAL_COLS is None:
        CANONICAL_COLS = list(df.columns)

    # Enforce canonical column order and explicit dtypes
    df = enforce_dtypes(df, CANONICAL_COLS)

    # Build Arrow table with explicit schema (avoids null-type inference)
    if ref_schema is None:
        ref_schema = _pa_schema_from_canonical(list(df.columns))
    if writer is None:
        writer = pq.ParquetWriter(OUT_PARQUET, ref_schema, compression="snappy")

    # Cast each column to its declared Arrow type
    arrays = []
    for field in ref_schema:
        series = df[field.name] if field.name in df.columns else pd.Series([None] * len(df))
        # Convert to a safe Python-native array: replace NaN/None with None
        if field.type == pa.string():
            # For string fields, NaN (float) must become None before passing to pa.array
            arr = pa.array(
                [None if (v is None or (isinstance(v, float) and np.isnan(v))) else str(v)
                 for v in series],
                type=pa.string()
            )
        else:
            arr = pa.array(series, type=field.type, from_pandas=True)
        arrays.append(arr)
    table = pa.Table.from_arrays(arrays, schema=ref_schema)
    writer.write_table(table)

    # Accumulate stats
    total_rows     += len(df)
    total_key_dups += key_dups
    year_counts[year] = year_counts.get(year, 0) + len(df)

    pub = df["FechaPublicacion"].dropna() if "FechaPublicacion" in df.columns else pd.Series([], dtype="datetime64[ns]")
    if len(pub):
        mn, mx = pub.min(), pub.max()
        date_min = mn if date_min is None else min(date_min, mn)
        date_max = mx if date_max is None else max(date_max, mx)

    print(f"{len(df):>7,} rows  |  full_dup_dropped={dropped:,}"
          f"  |  key_dup_flagged={key_dups:,}")

    del df, table

if writer:
    writer.close()
    print(f"\nParquet written → {OUT_PARQUET}")
else:
    print("No files processed.")
    sys.exit(1)

# ── Optional CSV ──────────────────────────────────────────────────────────────
if args.csv:
    print(f"Writing CSV → {OUT_CSV} …", end=" ", flush=True)
    pq.read_table(OUT_PARQUET).to_pandas().to_csv(OUT_CSV, index=False)
    print("done")

# ── Final summary ─────────────────────────────────────────────────────────────
n_cols = len(pq.read_schema(OUT_PARQUET).names)
print("\n" + "=" * 70)
print("PANEL SUMMARY")
print("=" * 70)
print(f"  Total rows          : {total_rows:,}")
print(f"  Total columns       : {n_cols}")
print(f"  Key-duplicate rows  : {total_key_dups:,} "
      f"({total_key_dups / total_rows * 100:.1f}%)")
if date_min and date_max:
    print(f"  Date range          : {date_min.date()} → {date_max.date()}")
print(f"\n  Rows by year:")
for yr, cnt in sorted(year_counts.items()):
    print(f"    {yr}: {cnt:>10,}")
print(f"\n  Output: {OUT_PARQUET}")
