"""
clean/04_merge_sii_licitaciones.py
─────────────────────────────────────────────────────────────────────────────
SII merge for licitaciones — EXPANDED harmonized schema.

Input:  data/clean/chilecompra_panel.parquet
Output: data/clean/licitaciones_sii_merged.parquet

Carries through product-, bid- and tender-level fields that were previously
dropped, so the downstream combined panel is self-contained for both
mechanisms.  CA-only fields are left NA on licit rows and vice versa; the
union-schema logic in 06_combine_sii_merged.py handles this automatically.

Flow:
  1) Load licitaciones rows by year
  2) Standardise to common schema fields (expanded)
  3) Parse bidder RUT (rut_bidder, dv_bidder)
  4) Merge with SII snapshot (last available <= source_year)
  5) Write incrementally to parquet
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

IN_FILE     = DATA_CLEAN / "chilecompra_panel.parquet"
OUT_FILE    = DATA_CLEAN / "licitaciones_sii_merged.parquet"
SII_PARQUET = DATA_RAW_SII / "rutsCharacteristics.parquet"

# ─────────────────────────────────────────────────────────────────────────────
# Raw input columns to read from chilecompra_panel
# ─────────────────────────────────────────────────────────────────────────────
LIC_COLS = [
    # identifiers + housekeeping
    "Codigo", "CodigoExterno", "source_year", "source_month", "is_key_dup",
    # buyer
    "RutUnidad", "RegionUnidad", "ComunaUnidad", "sector",
    # tender meta
    "Nombre", "Descripcion", "Estado", "Obras", "CantidadReclamos",
    "Tipo", "Tipo de Adquisicion", "CodigoTipo", "TipoConvocatoria",
    "Modalidad", "TipoPago", "SubContratacion",
    "Tiempo", "UnidadTiempo", "FechaTiempoEvaluacion", "UnidadTiempoEvaluacion",
    "FechaPublicacion", "FechaCierre", "FechaAdjudicacion",
    "MontoEstimado", "Estimacion", "FuenteFinanciamiento", "VisibilidadMonto",
    "CodigoMoneda", "Moneda Adquisicion",
    "JustificacionMontoEstimado", "ObservacionContrato", "ExtensionPlazo",
    "UnidadTiempoContratoLicitacion", "ValorTiempoRenovacion", "EsRenovable",
    "Contrato",
    # criteria (rubric)
    "CriteriosAmbientales", "CriteriosSociales", "CriteriosEvaluacion",
    # product/line
    "Codigoitem", "CodigoProductoONU", "Nombre producto genrico",
    "UnidadMedida", "Cantidad",
    # bidder + bid
    "RutProveedor", "NumeroOferentes",
    "Estado Oferta", "Cantidad Ofertada", "Moneda de la Oferta",
    "MontoUnitarioOferta", "Valor Total Ofertado", "FechaEnvioOferta",
    "Oferta seleccionada", "Monto Estimado Adjudicado",
    "CantidadAdjudicada", "MontoLineaAdjudica",
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


def _na_series(idx, dtype=object):
    return pd.Series(np.nan, index=idx, dtype=dtype if dtype is not object else "object")


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
    is_selected = _col(df, "Oferta seleccionada").astype(str).str.strip().eq("Seleccionada") \
        if "Oferta seleccionada" in df.columns else pd.Series(False, index=idx)
    is_key_dup = _col(df, "is_key_dup").fillna(False).astype(bool) \
        if "is_key_dup" in df.columns else pd.Series(False, index=idx)

    # criterios evaluacion: raw text + number of criteria (semicolon split)
    crit_eval_raw = _str(df, "CriteriosEvaluacion")
    n_crit = crit_eval_raw.fillna("").str.count(";") + crit_eval_raw.notna().astype(int)
    n_crit = n_crit.where(crit_eval_raw.notna(), other=np.nan)

    # licit bid totals: prefer Valor Total Ofertado, fallback MontoUnitarioOferta * Cantidad Ofertada
    vto = _num(df, "Valor Total Ofertado")
    muo = _num(df, "MontoUnitarioOferta")
    cant_of = _num(df, "Cantidad Ofertada")
    monto_total_oferta = vto.where(vto.notna(), muo * cant_of)

    out = pd.DataFrame({
        # ── identifiers ─────────────────────────────────────────────────
        "dataset":          "licitaciones",
        "tender_id":        _str(df, "Codigo"),
        "tender_ext_id":    _str(df, "CodigoExterno"),
        "line_id":          _int64(df, "Codigoitem"),
        "rut_bidder_raw":   _str(df, "RutProveedor"),
        "rut_unidad":       _str(df, "RutUnidad"),

        # ── buyer geo/sector ────────────────────────────────────────────
        "region_buyer":     _str(df, "RegionUnidad"),
        "comuna_buyer":     _str(df, "ComunaUnidad"),
        "sector":           _str(df, "sector"),

        # ── time (tender-level) ─────────────────────────────────────────
        "fecha_pub":        _dt(df, "FechaPublicacion"),
        "fecha_cierre":     _dt(df, "FechaCierre"),
        "fecha_adj":        _dt(df, "FechaAdjudicacion"),
        "fecha_envio_oferta": _dt(df, "FechaEnvioOferta"),
        "source_year":      _int64(df, "source_year"),
        "source_month":     _int64(df, "source_month"),

        # ── tender meta ─────────────────────────────────────────────────
        "tender_name":      _str(df, "Nombre"),
        "tender_desc":      _str(df, "Descripcion"),
        "estado_tender":    _str(df, "Estado"),
        "obras":            _int64(df, "Obras"),
        "cantidad_reclamos": _int64(df, "CantidadReclamos"),
        "tipo":             _str(df, "Tipo"),
        "tipo_adquisicion": _str(df, "Tipo de Adquisicion"),
        "codigo_tipo":      _int64(df, "CodigoTipo"),
        "tipo_convocatoria": _int64(df, "TipoConvocatoria"),
        "modalidad":        _str(df, "Modalidad"),
        "tipo_pago":        _str(df, "TipoPago"),
        "sub_contratacion": _int64(df, "SubContratacion"),
        "tiempo":           _num(df, "Tiempo"),
        "unidad_tiempo":    _str(df, "UnidadTiempo"),
        "fecha_tiempo_eval": _int64(df, "FechaTiempoEvaluacion"),
        "unidad_tiempo_eval": _str(df, "UnidadTiempoEvaluacion"),

        # ── monetary / contract terms (tender-level) ────────────────────
        "monto_estimado":   _num(df, "MontoEstimado"),
        "estimacion":       _num(df, "Estimacion"),
        "fuente_financiamiento": _str(df, "FuenteFinanciamiento"),
        "visibilidad_monto": _int64(df, "VisibilidadMonto"),
        "codigo_moneda":    _str(df, "CodigoMoneda"),
        "moneda_adq":       _str(df, "Moneda Adquisicion"),
        "just_monto_estimado": _str(df, "JustificacionMontoEstimado"),
        "obs_contrato":     _str(df, "ObservacionContrato"),
        "extension_plazo":  _int64(df, "ExtensionPlazo"),
        "unidad_tiempo_contrato": _int64(df, "UnidadTiempoContratoLicitacion"),
        "valor_tiempo_renov": _int64(df, "ValorTiempoRenovacion"),
        "es_renovable":     _int64(df, "EsRenovable"),
        "contrato":         _num(df, "Contrato"),

        # ── criteria (rubric) ───────────────────────────────────────────
        "criterios_ambientales": _str(df, "CriteriosAmbientales"),
        "criterios_sociales":    _str(df, "CriteriosSociales"),
        "criterios_evaluacion":  crit_eval_raw,
        "n_criterios_eval":      n_crit.astype("Int64"),
        "nombre_criterio":       _na_series(idx),  # CA-only field

        # ── product / line ──────────────────────────────────────────────
        "codigo_producto":  _int64(df, "CodigoProductoONU"),
        "nombre_producto":  _str(df, "Nombre producto genrico"),
        "unidad_medida":    _str(df, "UnidadMedida"),
        "cantidad_solicitada": _num(df, "Cantidad"),

        # ── bidder / bid-level ──────────────────────────────────────────
        "n_oferentes":      _num(df, "NumeroOferentes"),
        "estado_oferta":    _str(df, "Estado Oferta"),
        "cantidad_ofertada": _num(df, "Cantidad Ofertada"),
        "moneda_oferta":    _str(df, "Moneda de la Oferta"),
        "monto_unit_oferta": muo,
        "monto_total_oferta": monto_total_oferta,
        "monto_oferta":     monto_total_oferta,           # harmonized alias
        "oferta_seleccionada": _str(df, "Oferta seleccionada"),
        "monto_est_adjudicado": _num(df, "Monto Estimado Adjudicado"),
        "cantidad_adjudicada": _num(df, "CantidadAdjudicada"),
        "monto_adjudicado": _num(df, "MontoLineaAdjudica"),

        # ── flags & CA-only placeholders ────────────────────────────────
        "is_selected":      is_selected.astype(bool),
        "is_key_dup":       is_key_dup,
        "tamano":           _na_series(idx),          # CA-only
        "estado":           _na_series(idx),          # CA-only (Estado cotización)
        "direccion_entrega": _na_series(idx),         # CA-only
        "plazo_entrega":    _na_series(idx),          # CA-only
        "moneda_ca":        _na_series(idx),          # CA-only (the 'moneda' field)
    })
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline
# ─────────────────────────────────────────────────────────────────────────────
import sys
sys.stdout.reconfigure(line_buffering=True)

print("=" * 70, flush=True)
print("STEP 1 — Discovering year-months in licitaciones panel", flush=True)
print("=" * 70, flush=True)

if not IN_FILE.exists():
    raise FileNotFoundError(f"Missing input: {IN_FILE}")

schema = pq.read_schema(IN_FILE)
avail_cols = [c for c in LIC_COLS if c in schema.names]
missing_cols = [c for c in LIC_COLS if c not in schema.names]
if missing_cols:
    print(f"  [WARN] Columns not in licitaciones panel: {missing_cols}", flush=True)

# Month-level batching to keep memory low
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

import duckdb, tempfile, os

sii_schema = pq.read_schema(SII_PARQUET)
sii_keep_present = [c for c in SII_KEEP if c in sii_schema.names]
_indicator = next((c for c in ["razonsocial", "tramoventas", "rubro"] if c in sii_keep_present), None)

SNAP_DIR = Path(tempfile.mkdtemp(prefix="sii_snaps_"))
print(f"  snapshot dir: {SNAP_DIR}", flush=True)

con = duckdb.connect()
con.execute("PRAGMA memory_limit='2GB'")
con.execute("PRAGMA threads=2")
con.execute(f"PRAGMA temp_directory='{SNAP_DIR}/tmp'")

other_cols = [c for c in sii_keep_present if c not in ("year", "rut", "dv")]
other_cols_sel = ", ".join(other_cols)
other_cols_sel_qual = ", ".join(f"t.{c}" for c in other_cols)

# Build one snapshot per relevant year (latest row per (rut,dv) with year<=y).
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
    """Read the pre-built snapshot for year y and cast strings to category."""
    snap = pq.read_table(snap_paths[y]).to_pandas()
    for c in snap.columns:
        if snap[c].dtype == object:
            s = snap[c].astype("string").str.strip()
            s = s.mask(s.isin(["nan", "None", ""]))
            snap[c] = s.astype("category")
    snap["rut"] = pd.to_numeric(snap["rut"], errors="coerce").astype("Int64")
    snap = snap.rename(columns={"rut": "_snap_rut"})
    return snap

print("\n" + "=" * 70, flush=True)
print("STEP 3 — Month-by-month SII merge", flush=True)
print("=" * 70, flush=True)

writer = None
ref_schema = None
total_rows = 0
stats = {y: {"n": 0, "matched": 0} for y in cc_years}

# lazy single-item cache
_snap_cache: dict = {}
def get_sii_snap(y: int) -> pd.DataFrame:
    if y in _snap_cache:
        return _snap_cache[y]
    _snap_cache.clear()
    _snap_cache[y] = load_sii_snap(y)
    return _snap_cache[y]

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
print("\n  Match rate by year (licitaciones):", flush=True)
for y, s in sorted(stats.items()):
    pct = 100 * s["matched"] / max(s["n"], 1)
    print(f"    {y}: {s['matched']:,}/{s['n']:,}  ({pct:.1f}%)", flush=True)
print("\nDone.", flush=True)
