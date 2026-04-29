#!/usr/bin/env python3
"""
parse_details_to_parquet.py  <YEAR>  [--evict]
──────────────────────────────────────────────────────────────────────────────
Consolidate all detail JSON files for a given year into a single parquet file.

What it does:
  1. Finds all detail JSONs under detail_json/YEAR/MM/*.json
  2. Parses each into a flat row (same fields as the R manifest, plus items)
  3. Writes  data/clean/ordenes_compra_YEAR.parquet
  4. With --evict: marks each JSON as Dropbox "online only" to free local disk
     (files remain in Dropbox cloud; only the local copy is evicted)

Outputs:
  data/clean/ordenes_compra_YEAR.parquet
  data/raw/chilecompra/ordenes_compra/manifests/parse_YEAR_summary.csv

Usage:
  python3 sandbox/code/download/parse_details_to_parquet.py 2022
  python3 sandbox/code/download/parse_details_to_parquet.py 2022 --evict
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from multiprocessing import Pool, cpu_count
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# ── Paths ──────────────────────────────────────────────────────────────────────
_DB_ROOT_DEFAULT = "/Users/martinferraritagle/Dropbox/procurement-chile-db"
DB_ROOT    = Path(os.environ.get("PROCUREMENT_CHILE_DB", _DB_ROOT_DEFAULT))
RAW_ROOT   = DB_ROOT / "data" / "raw" / "chilecompra" / "ordenes_compra"
CLEAN_ROOT = DB_ROOT / "data" / "clean"
MANIFESTS  = RAW_ROOT / "manifests"

CHUNK_SIZE  = 50_000   # rows per parquet write chunk
# Respect SLURM allocation; fall back to all-but-one local core
N_WORKERS   = max(1, int(os.environ.get("SLURM_CPUS_PER_TASK", cpu_count())) - 1)


# ── JSON parser ────────────────────────────────────────────────────────────────
def _safe(val, cast=None):
    if val is None:
        return None
    if cast is not None:
        try:
            return cast(val)
        except (TypeError, ValueError):
            return None
    return val


def parse_one(json_path: Path) -> dict:
    """Parse a single detail JSON into a flat dict. Never raises."""
    base = {"_path": str(json_path), "status": "error"}
    try:
        text = json_path.read_text(encoding="utf-8")
        data = json.loads(text)
    except Exception as exc:
        return {**base, "_error": f"read/json: {exc}"}

    listado = data.get("Listado") or []
    # Normalise: API sometimes returns a dict instead of a list
    if isinstance(listado, dict):
        listado = [listado]

    if not listado:
        return {
            **base,
            "codigo":  json_path.stem,
            "status":  "empty",
            "_error":  "empty Listado",
        }

    rec      = listado[0]
    fechas   = rec.get("Fechas")   or {}
    comprador = rec.get("Comprador") or {}
    proveedor = rec.get("Proveedor") or {}
    items    = rec.get("Items")    or {}
    item_list = items.get("Listado") or []
    if isinstance(item_list, dict):
        item_list = [item_list]

    # Extract year/month from path: detail_json/YYYY/MM/codigo.json
    parts = json_path.parts
    try:
        source_year  = int(parts[-3])
        source_month = int(parts[-2])
    except (IndexError, ValueError):
        source_year  = None
        source_month = None

    return {
        "status":                     "ok",
        "codigo":                     _safe(rec.get("Codigo")),
        "nombre":                     _safe(rec.get("Nombre")),
        "codigo_estado":              _safe(rec.get("CodigoEstado"),  int),
        "estado":                     _safe(rec.get("Estado")),
        "codigo_licitacion":          _safe(rec.get("CodigoLicitacion")),
        "codigo_tipo":                _safe(rec.get("CodigoTipo")),
        "tipo":                       _safe(rec.get("Tipo")),
        "tipo_moneda":                _safe(rec.get("TipoMoneda")),
        "codigo_estado_proveedor":    _safe(rec.get("CodigoEstadoProveedor"), int),
        "estado_proveedor":           _safe(rec.get("EstadoProveedor")),
        # dates
        "fecha_creacion_oc":          _safe(fechas.get("FechaCreacion")),
        "fecha_envio_oc":             _safe(fechas.get("FechaEnvio")),
        "fecha_aceptacion_oc":        _safe(fechas.get("FechaAceptacion")),
        "fecha_cancelacion_oc":       _safe(fechas.get("FechaCancelacion")),
        "fecha_ultima_modificacion_oc": _safe(fechas.get("FechaUltimaModificacion")),
        # monetary
        "total_neto":                 _safe(rec.get("TotalNeto"),    float),
        "porcentaje_iva":             _safe(rec.get("PorcentajeIva"), float),
        "impuestos":                  _safe(rec.get("Impuestos"),    float),
        "total":                      _safe(rec.get("Total"),        float),
        "descuentos":                 _safe(rec.get("Descuentos"),   float),
        "cargos":                     _safe(rec.get("Cargos"),       float),
        # logistics
        "financiamiento":             _safe(rec.get("Financiamiento")),
        "pais":                       _safe(rec.get("Pais")),
        "tipo_despacho":              _safe(rec.get("TipoDespacho")),
        "forma_pago":                 _safe(rec.get("FormaPago")),
        # buyer
        "comprador_codigo_organismo": _safe(comprador.get("CodigoOrganismo")),
        "comprador_nombre_organismo": _safe(comprador.get("NombreOrganismo")),
        "comprador_rut_unidad":       _safe(comprador.get("RutUnidad")),
        "comprador_codigo_unidad":    _safe(comprador.get("CodigoUnidad")),
        "comprador_nombre_unidad":    _safe(comprador.get("NombreUnidad")),
        "comprador_comuna":           _safe(comprador.get("ComunaUnidad")),
        "comprador_region":           _safe(comprador.get("RegionUnidad")),
        # supplier
        "proveedor_codigo":           _safe(proveedor.get("Codigo")),
        "proveedor_nombre":           _safe(proveedor.get("Nombre")),
        "proveedor_rut":              _safe(proveedor.get("RutSucursal")),
        "proveedor_comuna":           _safe(proveedor.get("Comuna")),
        "proveedor_region":           _safe(proveedor.get("Region")),
        # items
        "n_items_reported":           _safe(items.get("Cantidad"),   int),
        "n_items_parsed":             len(item_list),
        # provenance
        "source_year":                source_year,
        "source_month":               source_month,
        "_path":                      str(json_path),
    }


# ── Dropbox eviction ───────────────────────────────────────────────────────────
def evict_file(path: Path) -> bool:
    """
    Mark a Dropbox file as 'online only' (Mac-only via xattr).
    On Linux/Bouchet this is a no-op that returns False.
    """
    try:
        subprocess.run(
            ["xattr", "-w", "com.dropbox.attrs", '{"p":{"5065":"02"}}', str(path)],
            check=True, capture_output=True,
        )
        return True
    except (FileNotFoundError, subprocess.CalledProcessError):
        return False


# ── Main ───────────────────────────────────────────────────────────────────────
def main() -> None:
    args = sys.argv[1:]
    if not args or args[0] in ("-h", "--help"):
        print(__doc__)
        sys.exit(0)

    year_str = args[0]
    if not year_str.isdigit() or len(year_str) != 4:
        print(f"ERROR: YEAR must be a 4-digit number, got: {year_str}")
        sys.exit(1)

    do_evict = "--evict" in args

    year_dir   = RAW_ROOT / "detail_json" / year_str
    out_parquet = CLEAN_ROOT / f"ordenes_compra_{year_str}.parquet"
    MANIFESTS.mkdir(parents=True, exist_ok=True)

    if not year_dir.exists():
        print(f"ERROR: No detail_json directory found for {year_str}: {year_dir}")
        sys.exit(1)

    # ── Collect paths ──────────────────────────────────────────────────────────
    print(f"Scanning {year_dir} ...")
    json_paths = sorted(year_dir.rglob("*.json"))
    n_total = len(json_paths)
    if n_total == 0:
        print(f"No JSON files found under {year_dir}. Nothing to do.")
        sys.exit(0)

    print(f"Found {n_total:,} JSON files for {year_str}.")
    print(f"Output: {out_parquet}")
    print(f"Workers: {N_WORKERS}   Chunk size: {CHUNK_SIZE:,}")
    if do_evict:
        print("Eviction: ENABLED (will set Dropbox online-only after writing parquet)")
    print()

    # ── Parse in chunks ────────────────────────────────────────────────────────
    writer: Optional[pq.ParquetWriter] = None
    n_ok = n_empty = n_error = 0
    evict_ok = evict_fail = 0

    for chunk_start in range(0, n_total, CHUNK_SIZE):
        chunk = json_paths[chunk_start: chunk_start + CHUNK_SIZE]
        pct   = 100 * chunk_start / n_total

        print(f"  Parsing [{chunk_start:,}–{chunk_start + len(chunk):,} / {n_total:,}]  ({pct:.0f}%) ...",
              end="\r", flush=True)

        with Pool(N_WORKERS) as pool:
            rows = pool.map(parse_one, chunk)

        # Tally
        for r in rows:
            s = r.get("status")
            if s == "ok":
                n_ok += 1
            elif s == "empty":
                n_empty += 1
            else:
                n_error += 1

        # Write chunk to parquet
        df = pd.DataFrame(rows)
        # Drop internal path column from output (keep for eviction below)
        paths_in_chunk = [Path(r["_path"]) for r in rows]
        df = df.drop(columns=["_path"], errors="ignore")

        table = pa.Table.from_pandas(df, preserve_index=False)

        if writer is None:
            writer = pq.ParquetWriter(out_parquet, table.schema, compression="snappy")
        writer.write_table(table)

        # Evict JSONs for this chunk
        if do_evict:
            for p in paths_in_chunk:
                if evict_file(p):
                    evict_ok += 1
                else:
                    evict_fail += 1

    if writer:
        writer.close()

    # ── Summary ────────────────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"  Year          : {year_str}")
    print(f"  Total files   : {n_total:,}")
    print(f"  Parsed OK     : {n_ok:,}")
    print(f"  Empty         : {n_empty:,}")
    print(f"  Errors        : {n_error:,}")
    size_mb = out_parquet.stat().st_size / 1e6 if out_parquet.exists() else 0
    print(f"  Parquet size  : {size_mb:.1f} MB  →  {out_parquet}")
    if do_evict:
        print(f"  Evicted       : {evict_ok:,} files  ({evict_fail} failed)")
    print(f"{'='*60}\n")

    # Write parse summary CSV
    summary = pd.DataFrame([{
        "year":         year_str,
        "n_json_files": n_total,
        "n_ok":         n_ok,
        "n_empty":      n_empty,
        "n_error":      n_error,
        "parquet_path": str(out_parquet),
        "parquet_mb":   round(size_mb, 1),
        "evict_ok":     evict_ok if do_evict else None,
        "evict_fail":   evict_fail if do_evict else None,
    }])
    summary_path = MANIFESTS / f"parse_{year_str}_summary.csv"
    summary.to_csv(summary_path, index=False)
    print(f"Summary written to {summary_path}")


if __name__ == "__main__":
    main()
