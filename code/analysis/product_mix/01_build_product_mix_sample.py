"""
01_build_product_mix_sample.py
Build a harmonized buyer-side product-line sample around the policy-relevant
1-200 UTM tender universe already used in the DiD pipeline.

Inputs
  output/did/samples/did_bid_sample.parquet
  data/clean/chilecompra_panel.parquet
  data/clean/compra_agil_panel.parquet

Outputs
  output/product_mix/samples/product_mix_lines.parquet
  output/product_mix/samples/product_mix_tenders.parquet
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
sys.path.insert(0, str(ROOT / "code" / "analysis" / "did"))

from did_utils import CA_PANEL, LIC_PANEL, OUT_SAMPLES  # noqa: E402

OUT_DIR = ROOT / "output" / "product_mix"
OUT_SAMPLES_DIR = OUT_DIR / "samples"
for _d in [OUT_DIR, OUT_SAMPLES_DIR]:
    _d.mkdir(parents=True, exist_ok=True)

OUT_LINES = OUT_SAMPLES_DIR / "product_mix_lines.parquet"
OUT_TENDERS = OUT_SAMPLES_DIR / "product_mix_tenders.parquet"


def main() -> None:
    did_sample = OUT_SAMPLES / "did_bid_sample.parquet"
    con = duckdb.connect()
    con.execute("PRAGMA threads=8")
    con.execute("PRAGMA enable_progress_bar=false")

    print("Building tender metadata ...")
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE tender_meta AS
        SELECT DISTINCT
            dataset,
            tender_id,
            rut_unidad,
            sector,
            CAST(fecha_pub AS DATE) AS fecha_pub,
            strftime(CAST(fecha_pub AS DATE), '%Y-%m') AS year_month,
            CAST(source_year AS INTEGER) AS source_year,
            CAST(source_month AS INTEGER) AS source_month,
            CAST(monto_estimado AS DOUBLE) AS monto_estimado,
            CAST(monto_utm AS DOUBLE) AS monto_utm,
            band,
            CAST(treated AS INTEGER) AS treated,
            CAST(post AS INTEGER) AS post,
            CAST(did AS INTEGER) AS did
        FROM read_parquet('{did_sample}')
        """
    )

    print("Collapsing licitaciones to buyer-requested lines ...")
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE lic_lines AS
        SELECT
            'licitaciones'::VARCHAR AS dataset,
            CAST(Codigo AS VARCHAR) AS tender_id,
            CAST(Correlativo AS VARCHAR) AS line_id,
            CASE
                WHEN CodigoProductoONU IS NULL THEN NULL
                ELSE lpad(CAST(CodigoProductoONU AS VARCHAR), 8, '0')
            END AS product_code8,
            CASE
                WHEN CodigoProductoONU IS NULL THEN NULL
                ELSE substring(lpad(CAST(CodigoProductoONU AS VARCHAR), 8, '0'), 1, 6)
            END AS product_class6,
            CASE
                WHEN CodigoProductoONU IS NULL THEN NULL
                ELSE substring(lpad(CAST(CodigoProductoONU AS VARCHAR), 8, '0'), 1, 4)
            END AS product_family4,
            CASE
                WHEN CodigoProductoONU IS NULL THEN NULL
                ELSE substring(lpad(CAST(CodigoProductoONU AS VARCHAR), 8, '0'), 1, 2)
            END AS product_segment2,
            "Nombre producto genrico" AS product_name,
            "Nombre linea Adquisicion" AS line_description,
            CAST(Cantidad AS DOUBLE) AS quantity_requested,
            UnidadMedida AS quantity_unit,
            Rubro1,
            Rubro2,
            Rubro3
        FROM read_parquet('{LIC_PANEL}')
        QUALIFY row_number() OVER (PARTITION BY Codigo, Correlativo ORDER BY Codigo) = 1
        """
    )

    print("Collapsing Compra Ágil to buyer-requested lines ...")
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE ca_lines AS
        WITH base AS (
            SELECT DISTINCT
                CodigoCotizacion,
                CodigoProducto,
                NombreProductoGenerico,
                ProductoCotizado,
                CantidadSolicitada
            FROM read_parquet('{CA_PANEL}')
        )
        SELECT
            'compra_agil'::VARCHAR AS dataset,
            CodigoCotizacion AS tender_id,
            CAST(
                row_number() OVER (
                    PARTITION BY CodigoCotizacion
                    ORDER BY CodigoProducto, ProductoCotizado, CantidadSolicitada
                ) AS VARCHAR
            ) AS line_id,
            CASE
                WHEN CodigoProducto IS NULL THEN NULL
                ELSE lpad(CAST(CodigoProducto AS VARCHAR), 8, '0')
            END AS product_code8,
            CASE
                WHEN CodigoProducto IS NULL THEN NULL
                ELSE substring(lpad(CAST(CodigoProducto AS VARCHAR), 8, '0'), 1, 6)
            END AS product_class6,
            CASE
                WHEN CodigoProducto IS NULL THEN NULL
                ELSE substring(lpad(CAST(CodigoProducto AS VARCHAR), 8, '0'), 1, 4)
            END AS product_family4,
            CASE
                WHEN CodigoProducto IS NULL THEN NULL
                ELSE substring(lpad(CAST(CodigoProducto AS VARCHAR), 8, '0'), 1, 2)
            END AS product_segment2,
            NombreProductoGenerico AS product_name,
            ProductoCotizado AS line_description,
            CAST(CantidadSolicitada AS DOUBLE) AS quantity_requested,
            CAST(NULL AS VARCHAR) AS quantity_unit,
            CAST(NULL AS VARCHAR) AS Rubro1,
            CAST(NULL AS VARCHAR) AS Rubro2,
            CAST(NULL AS VARCHAR) AS Rubro3
        FROM base
        """
    )

    print("Joining buyer-side lines to tender metadata ...")
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE product_mix_lines AS
        SELECT
            t.dataset,
            t.tender_id,
            l.line_id,
            t.rut_unidad,
            t.sector,
            t.fecha_pub,
            t.year_month,
            t.source_year,
            t.source_month,
            t.monto_estimado,
            t.monto_utm,
            t.band,
            t.treated,
            t.post,
            t.did,
            l.product_code8,
            l.product_class6,
            l.product_family4,
            l.product_segment2,
            l.product_name,
            l.line_description,
            l.quantity_requested,
            l.quantity_unit,
            l.Rubro1,
            l.Rubro2,
            l.Rubro3
        FROM (
            SELECT * FROM lic_lines
            UNION ALL
            SELECT * FROM ca_lines
        ) l
        JOIN tender_meta t
          ON l.dataset = t.dataset
         AND l.tender_id = t.tender_id
        """
    )

    print("Building tender-level bundle metrics ...")
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE product_mix_tenders AS
        SELECT
            dataset,
            tender_id,
            rut_unidad,
            sector,
            fecha_pub,
            year_month,
            source_year,
            source_month,
            monto_estimado,
            monto_utm,
            band,
            treated,
            post,
            did,
            COUNT(*) AS n_lines,
            COUNT(DISTINCT product_code8) AS n_product8,
            COUNT(DISTINCT product_family4) AS n_family4,
            COUNT(DISTINCT product_segment2) AS n_segment2,
            CAST(COUNT(*) = 1 AS INTEGER) AS single_line,
            AVG(quantity_requested) AS mean_quantity_requested,
            SUM(CASE WHEN quantity_requested IS NOT NULL THEN quantity_requested ELSE 0 END) AS sum_quantity_requested
        FROM product_mix_lines
        GROUP BY
            dataset, tender_id, rut_unidad, sector, fecha_pub, year_month,
            source_year, source_month, monto_estimado, monto_utm,
            band, treated, post, did
        """
    )

    print("Writing outputs ...")
    con.execute(
        f"""
        COPY product_mix_lines
        TO '{OUT_LINES}'
        (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )
    con.execute(
        f"""
        COPY product_mix_tenders
        TO '{OUT_TENDERS}'
        (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )

    for label, table in [("lines", "product_mix_lines"), ("tenders", "product_mix_tenders")]:
        stats = con.execute(
            f"""
            SELECT
                COUNT(*) AS n_rows,
                COUNT(DISTINCT tender_id) AS n_tenders,
                COUNT(DISTINCT rut_unidad) AS n_buyers
            FROM {table}
            """
        ).fetchone()
        print(f"{label:>7}: rows={stats[0]:,} | tenders={stats[1]:,} | buyers={stats[2]:,}")

    print(f"\nSaved: {OUT_LINES}")
    print(f"Saved: {OUT_TENDERS}")


if __name__ == "__main__":
    main()
