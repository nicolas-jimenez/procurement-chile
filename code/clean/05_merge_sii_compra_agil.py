"""
clean/05_merge_sii_compra_agil.py
─────────────────────────────────────────────────────────────────────────────
SII merge for compra ágil — EXPANDED harmonized schema.

Input:  data/clean/compra_agil_panel.parquet
Output: data/clean/compra_agil_sii_merged.parquet

Carries through product-, bid- and tender-level fields that were previously
dropped, so the downstream combined panel is self-contained for both
mechanisms.  Licit-only fields are left NA on CA rows.

Derived fields:
  * n_lines_cot     = # distinct CodigoProducto per CodigoCotizacion
  * is_single_line  = n_lines_cot == 1
  * monto_unit_oferta = MontoTotal / CantidadSolicitada  when is_single_line
                        else NA  (because MontoTotal is cotización-level)
  * monto_total_oferta = MontoTotal (raw bidder total for the cotización)
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from _region_matching import REGION_KEY_TO_CC_LABEL, region_key_series, same_region_from_series

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config import DATA_CLEAN, DATA_RAW_SII  # noqa: E402

IN_FILE     = DATA_CLEAN / "compra_agil_panel.parquet"
OUT_FILE    = DATA_CLEAN / "compra_agil_sii_merged.parquet"
SII_PARQUET = DATA_RAW_SII / "rutsCharacteristics.parquet"

CA_COLS = [
    # identifiers + housekeeping
    "CodigoCotizacion", "source_year", "source_month", "is_key_dup",
    # buyer
    "RUTUnidaddeCompra", "Region", "DireccionEntrega",
    # tender meta
    "NombreCotizacion", "DescripcionCotizacion", "Estado",
    "FechaPublicacionParaCotizar", "FechaCierreParaCotizar", "PlazoEntrega",
    "MontoTotalDisponble", "moneda", "NombreCriterio",
    "CodigoOC", "EstadoOC", "MotivoCancelacion",
    # product/line
    "CodigoProducto", "NombreProductoGenerico", "CantidadSolicitada",
    "ProductoCotizado",
    # bidder + bid
    "RUTProveedor", "Tamano", "MontoTotal",
    "is_selected", "ProveedorSeleccionado",
]

SII_KEEP = [
    "year", "rut", "dv", "region", "provincia", "comuna",
    "razonsocial", "tramoventas", "ntrabajadores",
    "rubro", "subrubro", "actividadeconomica", "tipodecontribuyente",
    "tramocapitalpropiopositivo", "tramocapitalpropionegativo",
]


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────
def parse_rut(s: pd.Series) -> tuple[pd.Series, pd.Series]:
    s = s.astype(str).str.strip()
    split = s.str.split("-", n=1, expand=True)
    rut_str = split[0].str.strip().str.replace(".", "", regex=False)
    rut_str = rut_str.replace({"nan": np.nan, "None": np.nan, "": np.nan})
    rut_num = pd.to_numeric(rut_str, errors="coerce").astype("Int64")
    dv = split[1].str.strip() if 1 in split.columns else pd.Series(np.nan, index=s.index)
    dv = dv.replace({"nan": np.nan, "None": np.nan, "": np.nan})
    return rut_num, dv


def _na_series(idx):
    return pd.Series(np.nan, index=idx, dtype="object")


def _col(df: pd.DataFrame, name: str) -> pd.Series:
    if name in df.columns:
        return df[name]
    return _na_series(df.index)


def _str(df: pd.DataFrame, name: str) -> pd.Series:
    s = _col(df, name)
    return s.astype(str).where(s.notna(), other=np.nan)


def _num(df: pd.DataFrame, name: str) -> pd.Series:
    return pd.to_numeric(_col(df, name), errors="coerce")


def _int64(df: pd.DataFrame, name: str) -> pd.Series:
    return pd.to_numeric(_col(df, name), errors="coerce").astype("Int64")


def _dt(df: pd.DataFrame, name: str) -> pd.Series:
    return pd.to_datetime(_col(df, name), errors="coerce")


# ─────────────────────────────────────────────────────────────────────────────
# Standard slice
# ─────────────────────────────────────────────────────────────────────────────
def build_standard_slice(df: pd.DataFrame) -> pd.DataFrame:
    idx = df.index

    # derived flags
    if "is_selected" in df.columns:
        is_selected = df["is_selected"].fillna(False).astype(bool)
    elif "ProveedorSeleccionado" in df.columns:
        is_selected = df["ProveedorSeleccionado"].astype(str).str.strip().str.lower().eq("si")
    else:
        is_selected = pd.Series(False, index=idx)
    is_key_dup = _col(df, "is_key_dup").fillna(False).astype(bool) \
        if "is_key_dup" in df.columns else pd.Series(False, index=idx)

    # n_lines per cotización (distinct product codes within cotización)
    if "CodigoCotizacion" in df.columns and "CodigoProducto" in df.columns:
        n_lines = (
            df.groupby("CodigoCotizacion")["CodigoProducto"]
              .transform("nunique")
              .astype("Int64")
        )
    else:
        n_lines = pd.Series(pd.NA, index=idx, dtype="Int64")
    is_single_line = (n_lines == 1).astype("boolean")

    # unit bid (CA): MontoTotal / CantidadSolicitada *only* on single-line cotizaciones
    monto_total = _num(df, "MontoTotal")
    cantidad_sol = _num(df, "CantidadSolicitada")
    unit_price = monto_total.where(
        (n_lines == 1) & (cantidad_sol > 0),
        other=np.nan
    ) / cantidad_sol.where(cantidad_sol > 0, other=np.nan)

    # n_oferentes in CA: distinct RUTProveedor per cotización (bidders who submitted)
    if "CodigoCotizacion" in df.columns and "RUTProveedor" in df.columns:
        n_oferentes = (
            df.groupby("CodigoCotizacion")["RUTProveedor"]
              .transform("nunique")
              .astype("Int64")
        )
    else:
        n_oferentes = pd.Series(pd.NA, index=idx, dtype="Int64")

    out = pd.DataFrame({
        # ── identifiers ─────────────────────────────────────────────────
        "dataset":          "compra_agil",
        "tender_id":        _str(df, "CodigoCotizacion"),
        "tender_ext_id":    _str(df, "CodigoCotizacion"),   # same as tender_id for CA
        "line_id":          _int64(df, "CodigoProducto"),   # line identified by product code
        "rut_bidder_raw":   _str(df, "RUTProveedor"),
        "rut_unidad":       _str(df, "RUTUnidaddeCompra"),

        # ── buyer geo/sector ────────────────────────────────────────────
        "region_buyer":     _str(df, "Region"),
        "comuna_buyer":     _na_series(idx),
        "sector":           _na_series(idx),  # filled downstream via crosswalk

        # ── time ────────────────────────────────────────────────────────
        "fecha_pub":        _dt(df, "FechaPublicacionParaCotizar"),
        "fecha_cierre":     _dt(df, "FechaCierreParaCotizar"),
        "fecha_adj":        _na_series(idx),          # licit-only
        "fecha_envio_oferta": _na_series(idx),        # licit-only
        "source_year":      _int64(df, "source_year"),
        "source_month":     _int64(df, "source_month"),

        # ── tender meta ─────────────────────────────────────────────────
        "tender_name":      _str(df, "NombreCotizacion"),
        "tender_desc":      _str(df, "DescripcionCotizacion"),
        "estado_tender":    _str(df, "Estado"),
        "obras":            _na_series(idx),          # licit-only
        "cantidad_reclamos": _na_series(idx),
        "tipo":             _na_series(idx),
        "tipo_adquisicion": _na_series(idx),
        "codigo_tipo":      _na_series(idx),
        "tipo_convocatoria": _na_series(idx),
        "modalidad":        _na_series(idx),
        "tipo_pago":        _na_series(idx),
        "sub_contratacion": _na_series(idx),
        "tiempo":           _na_series(idx),
        "unidad_tiempo":    _na_series(idx),
        "fecha_tiempo_eval": _na_series(idx),
        "unidad_tiempo_eval": _na_series(idx),

        # ── money / contract (tender-level) ─────────────────────────────
        "monto_estimado":   _num(df, "MontoTotalDisponble"),
        "estimacion":       _na_series(idx),
        "fuente_financiamiento": _na_series(idx),
        "visibilidad_monto": _na_series(idx),
        "codigo_moneda":    _na_series(idx),
        "moneda_adq":       _na_series(idx),
        "just_monto_estimado": _na_series(idx),
        "obs_contrato":     _na_series(idx),
        "extension_plazo":  _na_series(idx),
        "unidad_tiempo_contrato": _na_series(idx),
        "valor_tiempo_renov": _na_series(idx),
        "es_renovable":     _na_series(idx),
        "contrato":         _na_series(idx),

        # ── criteria (rubric) ───────────────────────────────────────────
        "criterios_ambientales": _na_series(idx),
        "criterios_sociales":    _na_series(idx),
        "criterios_evaluacion":  _na_series(idx),
        "n_criterios_eval":      _na_series(idx),
        "nombre_criterio":       _str(df, "NombreCriterio"),

        # ── product / line ──────────────────────────────────────────────
        "codigo_producto":  _int64(df, "CodigoProducto"),
        "nombre_producto":  _str(df, "NombreProductoGenerico"),
        "unidad_medida":    _na_series(idx),
        "cantidad_solicitada": cantidad_sol,
        "n_lines_cot":      n_lines,
        "is_single_line":   is_single_line,
        "producto_cotizado": _str(df, "ProductoCotizado"),

        # ── bidder / bid-level ──────────────────────────────────────────
        "n_oferentes":      n_oferentes,
        "estado_oferta":    _na_series(idx),
        "cantidad_ofertada": _na_series(idx),
        "moneda_oferta":    _na_series(idx),
        "monto_unit_oferta": unit_price,
        "monto_total_oferta": monto_total,
        "monto_oferta":     monto_total,                  # harmonized alias
        "oferta_seleccionada": _na_series(idx),
        "monto_est_adjudicado": _na_series(idx),
        "cantidad_adjudicada": _na_series(idx),
        "monto_adjudicado": monto_total.where(is_selected, other=np.nan),

        # ── flags ───────────────────────────────────────────────────────
        "is_selected":      is_selected.astype(bool),
        "is_key_dup":       is_key_dup,
        "tamano":           _str(df, "Tamano"),
        "estado":           _str(df, "Estado"),

        # ── CA-only extras ──────────────────────────────────────────────
        "direccion_entrega": _str(df, "DireccionEntrega"),
        "plazo_entrega":    _int64(df, "PlazoEntrega"),
        "moneda_ca":        _str(df, "moneda"),
        "codigo_oc":        _str(df, "CodigoOC"),
        "estado_oc":        _num(df, "EstadoOC"),
        "motivo_cancelacion": _str(df, "MotivoCancelacion"),
    })
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline
# ─────────────────────────────────────────────────────────────────────────────
import sys
sys.stdout.reconfigure(line_buffering=True)

print("=" * 70, flush=True)
print("STEP 1 — Discovering year-months in compra ágil panel", flush=True)
print("=" * 70, flush=True)

if not IN_FILE.exists():
    raise FileNotFoundError(f"Missing input: {IN_FILE}")

schema = pq.read_schema(IN_FILE)
avail_cols = [c for c in CA_COLS if c in schema.names]
missing_cols = [c for c in CA_COLS if c not in schema.names]
if missing_cols:
    print(f"  [WARN] Columns not in compra ágil panel: {missing_cols}", flush=True)

ym_raw = pq.read_table(IN_FILE, columns=["source_year", "source_month"]).to_pandas()
ym_pairs = sorted({(int(y), int(m)) for y, m in zip(ym_raw["source_year"], ym_raw["source_month"])
                    if pd.notna(y) and pd.notna(m)})
del ym_raw
cc_years = sorted({y for y, _ in ym_pairs})
print(f"  years: {cc_years}", flush=True)
print(f"  year-months: {len(ym_pairs)}", flush=True)
print(f"  loading {len(avail_cols)} columns", flush=True)

print("\n" + "=" * 70)
print("STEP 2 — Building SII per-year snapshots via DuckDB (on-disk)")
print("=" * 70, flush=True)

import duckdb, tempfile

sii_schema = pq.read_schema(SII_PARQUET)
sii_keep_present = [c for c in SII_KEEP if c in sii_schema.names]
_indicator = next((c for c in ["razonsocial", "tramoventas", "rubro"] if c in sii_keep_present), None)

SNAP_DIR = Path(tempfile.mkdtemp(prefix="sii_snaps_ca_"))
print(f"  snapshot dir: {SNAP_DIR}", flush=True)

con = duckdb.connect()
con.execute("PRAGMA memory_limit='2GB'")
con.execute("PRAGMA threads=2")
con.execute(f"PRAGMA temp_directory='{SNAP_DIR}/tmp'")

other_cols = [c for c in sii_keep_present if c not in ("year", "rut", "dv")]
other_cols_sel = ", ".join(other_cols)
other_cols_sel_qual = ", ".join(f"t.{c}" for c in other_cols)

snap_paths: dict[int, Path] = {}
for y in cc_years:
    out_path = SNAP_DIR / f"sii_snap_{y}.parquet"
    con.execute(f"""
      COPY (
        SELECT
          CAST(t.rut AS BIGINT) AS rut,
          UPPER(TRIM(t.dv))     AS dv,
          {other_cols_sel_qual}
        FROM (
          SELECT rut, dv, {other_cols_sel},
                 ROW_NUMBER() OVER (PARTITION BY rut, dv ORDER BY year DESC) AS _rn
          FROM read_parquet('{SII_PARQUET}')
          WHERE year > 2015 AND year <= {y}
        ) t
        WHERE t._rn = 1
      ) TO '{out_path}' (FORMAT 'parquet', COMPRESSION 'zstd')
    """)
    nrows = con.execute(f"SELECT COUNT(*) FROM read_parquet('{out_path}')").fetchone()[0]
    snap_paths[y] = out_path
    print(f"  snapshot year<={y}: {nrows:,} rows → {out_path.name}", flush=True)

con.close()

def load_sii_snap(y: int) -> pd.DataFrame:
    snap = pq.read_table(snap_paths[y]).to_pandas()
    for c in snap.columns:
        if snap[c].dtype == object:
            s = snap[c].astype("string").str.strip()
            s = s.mask(s.isin(["nan", "None", ""]))
            snap[c] = s.astype("category")
    snap["rut"] = pd.to_numeric(snap["rut"], errors="coerce").astype("Int64")
    snap = snap.rename(columns={"rut": "_snap_rut"})
    return snap

_snap_cache: dict = {}
def get_sii_snap(y: int) -> pd.DataFrame:
    if y in _snap_cache:
        return _snap_cache[y]
    _snap_cache.clear()
    _snap_cache[y] = load_sii_snap(y)
    return _snap_cache[y]

print("\n" + "=" * 70, flush=True)
print("STEP 3 — Month-by-month SII merge", flush=True)
print("=" * 70, flush=True)

writer = None
ref_schema = None
total_rows = 0
stats = {y: {"n": 0, "matched": 0} for y in cc_years}

import gc
CHUNK_ROWS = 150_000

def process_chunk(raw_chunk_pd, y, m):
    global writer, ref_schema, total_rows
    cc_y = build_standard_slice(raw_chunk_pd)
    cc_y["rut_bidder"], cc_y["dv_bidder"] = parse_rut(cc_y["rut_bidder_raw"])
    cc_y["dv_bidder"] = cc_y["dv_bidder"].str.upper()

    sii_snap = get_sii_snap(y)

    merged_y = cc_y.merge(
        sii_snap,
        left_on=["rut_bidder", "dv_bidder"],
        right_on=["_snap_rut", "dv"],
        how="left",
        suffixes=("", "_sii"),
    )
    merged_y.drop(columns=["_snap_rut"], inplace=True, errors="ignore")

    merged_y["_sii_region_key"] = region_key_series(merged_y.get("region", pd.Series(np.nan, index=merged_y.index)))
    merged_y["_buyer_region_key"] = region_key_series(merged_y.get("region_buyer", pd.Series(np.nan, index=merged_y.index)))
    merged_y["sii_region_cc_style"] = merged_y["_sii_region_key"].map(REGION_KEY_TO_CC_LABEL)
    merged_y["same_region"] = same_region_from_series(
        merged_y.get("region", pd.Series(np.nan, index=merged_y.index)),
        merged_y.get("region_buyer", pd.Series(np.nan, index=merged_y.index)),
    )
    merged_y.drop(columns=["_sii_region_key", "_buyer_region_key"], inplace=True, errors="ignore")

    hit = int(merged_y[_indicator].notna().sum()) if _indicator else 0
    stats[y]["n"] += len(cc_y)
    stats[y]["matched"] += hit
    total_rows += len(merged_y)

    table = pa.Table.from_pandas(merged_y, preserve_index=False)
    if ref_schema is None:
        ref_schema = table.schema
        writer = pq.ParquetWriter(OUT_FILE, ref_schema, compression="snappy")
    else:
        try:
            table = table.cast(ref_schema)
        except Exception:
            arrays = []
            for field in ref_schema:
                if field.name in merged_y.columns:
                    try:
                        arr = pa.array(merged_y[field.name], type=field.type, from_pandas=True)
                    except Exception:
                        arr = pa.array([None] * len(merged_y), type=field.type)
                else:
                    arr = pa.array([None] * len(merged_y), type=field.type)
                arrays.append(arr)
            table = pa.Table.from_arrays(arrays, schema=ref_schema)
    writer.write_table(table)
    n_rows_chunk = len(cc_y)
    del cc_y, sii_snap, merged_y, table
    gc.collect()
    return n_rows_chunk, hit

for (y, m) in ym_pairs:
    raw_t = pq.read_table(
        IN_FILE,
        columns=avail_cols,
        filters=[("source_year", "=", int(y)), ("source_month", "=", int(m))],
    )
    n_rows_month = raw_t.num_rows
    n_chunks = max(1, (n_rows_month + CHUNK_ROWS - 1) // CHUNK_ROWS)
    month_hit = 0
    for ci in range(n_chunks):
        start = ci * CHUNK_ROWS
        length = min(CHUNK_ROWS, n_rows_month - start)
        sub = raw_t.slice(start, length).to_pandas()
        _, h = process_chunk(sub, y, m)
        month_hit += h
        del sub
        gc.collect()
    del raw_t
    gc.collect()
    pct = 100 * month_hit / max(n_rows_month, 1)
    print(f"  {y}-{m:02d}: {n_rows_month:>9,} rows,  matched {month_hit:>9,}  ({pct:.1f}%)  [{n_chunks} chunk(s)]", flush=True)

if writer:
    writer.close()

print(f"\n  Written: {total_rows:,} rows → {OUT_FILE}", flush=True)
print(f"  Size: {OUT_FILE.stat().st_size/1e9:.2f} GB", flush=True)
print("\n  Match rate by year (compra ágil):", flush=True)
for y, s in sorted(stats.items()):
    pct = 100 * s["matched"] / max(s["n"], 1)
    print(f"    {y}: {s['matched']:,}/{s['n']:,}  ({pct:.1f}%)", flush=True)
print("\nDone.", flush=True)
