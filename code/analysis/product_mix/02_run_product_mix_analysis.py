"""
02_run_product_mix_analysis.py
Run buyer-side product-mix, mechanism-shift, and bundling analyses on the
harmonized product-mix sample.

Inputs
  output/product_mix/samples/product_mix_lines.parquet
  output/product_mix/samples/product_mix_tenders.parquet

Outputs
  output/product_mix/tables/*.csv
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
sys.path.insert(0, str(ROOT / "code" / "analysis" / "did"))

from did_utils import run_twfe_did  # noqa: E402

OUT_DIR = ROOT / "output" / "product_mix"
SAMPLES = OUT_DIR / "samples"
TABLES = OUT_DIR / "tables"
for _d in [TABLES]:
    _d.mkdir(parents=True, exist_ok=True)

LINES = SAMPLES / "product_mix_lines.parquet"
TENDERS = SAMPLES / "product_mix_tenders.parquet"


def _shift_table(
    con: duckdb.DuckDBPyConnection,
    *,
    level_col: str,
    name_expr: str,
    min_pre_total: int,
    out_name: str,
    top_n: int = 25,
) -> pd.DataFrame:
    query = f"""
    WITH base AS (
        SELECT
            {level_col} AS level_code,
            {name_expr} AS level_name,
            dataset,
            post
        FROM read_parquet('{LINES}')
        WHERE band = 'treated'
          AND {level_col} IS NOT NULL
    ),
    agg AS (
        SELECT
            level_code,
            any_value(level_name) AS level_name,
            SUM(CASE WHEN post = 0 AND dataset = 'licitaciones' THEN 1 ELSE 0 END) AS pre_lic,
            SUM(CASE WHEN post = 0 AND dataset = 'compra_agil'  THEN 1 ELSE 0 END) AS pre_ca,
            SUM(CASE WHEN post = 1 AND dataset = 'licitaciones' THEN 1 ELSE 0 END) AS post_lic,
            SUM(CASE WHEN post = 1 AND dataset = 'compra_agil'  THEN 1 ELSE 0 END) AS post_ca
        FROM base
        GROUP BY 1
    ),
    calc AS (
        SELECT
            *,
            pre_lic + pre_ca AS pre_total,
            post_lic + post_ca AS post_total,
            pre_ca::DOUBLE / NULLIF(pre_lic + pre_ca, 0) AS pre_ca_share,
            post_ca::DOUBLE / NULLIF(post_lic + post_ca, 0) AS post_ca_share,
            (post_ca::DOUBLE / NULLIF(post_lic + post_ca, 0))
              - (pre_ca::DOUBLE / NULLIF(pre_lic + pre_ca, 0)) AS delta_ca_share
        FROM agg
    )
    SELECT
        level_code,
        level_name,
        pre_total,
        post_total,
        round(pre_ca_share, 4) AS pre_ca_share,
        round(post_ca_share, 4) AS post_ca_share,
        round(delta_ca_share, 4) AS delta_ca_share
    FROM calc
    WHERE pre_total >= {min_pre_total}
    ORDER BY delta_ca_share DESC, post_total DESC
    LIMIT {top_n}
    """
    out = con.execute(query).df()
    out.to_csv(TABLES / out_name, index=False)
    return out


def _sector_shift_table(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    query = f"""
    WITH base AS (
        SELECT DISTINCT dataset, tender_id, sector, post
        FROM read_parquet('{TENDERS}')
        WHERE band = 'treated'
    ),
    agg AS (
        SELECT
            sector,
            post,
            SUM(CASE WHEN dataset = 'compra_agil' THEN 1 ELSE 0 END) AS ca_tenders,
            SUM(CASE WHEN dataset = 'licitaciones' THEN 1 ELSE 0 END) AS lic_tenders
        FROM base
        GROUP BY 1, 2
    )
    SELECT
        sector,
        post,
        ca_tenders + lic_tenders AS n_tenders,
        round(ca_tenders::DOUBLE / NULLIF(ca_tenders + lic_tenders, 0), 4) AS ca_share
    FROM agg
    ORDER BY sector, post
    """
    out = con.execute(query).df()
    out.to_csv(TABLES / "sector_shift_treated_band.csv", index=False)
    return out


def _bundling_cell_means(
    con: duckdb.DuckDBPyConnection,
    *,
    bands: list[str],
    out_name: str,
) -> pd.DataFrame:
    band_list = ", ".join(f"'{b}'" for b in bands)
    query = f"""
    SELECT
        band,
        treated,
        post,
        AVG(n_lines) AS mean_n_lines,
        AVG(n_product8) AS mean_n_product8,
        AVG(n_family4) AS mean_n_family4,
        AVG(n_segment2) AS mean_n_segment2,
        AVG(single_line) AS mean_single_line,
        AVG(log(1 + greatest(mean_quantity_requested, 0))) AS mean_log1p_mean_qty
    FROM read_parquet('{TENDERS}')
    WHERE band IN ({band_list})
    GROUP BY 1, 2, 3
    ORDER BY treated, post, band
    """
    out = con.execute(query).df()
    out.to_csv(TABLES / out_name, index=False)
    return out


def _bundling_did(
    *,
    bands: list[str],
    out_name: str,
) -> pd.DataFrame:
    tend = pd.read_parquet(TENDERS)
    tend = tend[tend["band"].isin(bands)].copy()
    tend["year_month"] = tend["year_month"].astype("string")
    tend["log1p_mean_quantity_requested"] = np.log1p(
        pd.to_numeric(tend["mean_quantity_requested"], errors="coerce").clip(lower=0)
    )

    rows = []
    for outcome in [
        "n_lines",
        "n_product8",
        "n_family4",
        "n_segment2",
        "single_line",
        "log1p_mean_quantity_requested",
    ]:
        res = run_twfe_did(
            tend,
            outcome_col=outcome,
            entity_col="rut_unidad",
            time_col="year_month",
            did_col="did",
            treat_col="treated",
            cluster_col="rut_unidad",
            label=outcome,
        )
        rows.append(
            {
                "outcome": outcome,
                "coef_did": res.get("coef_did"),
                "se_did": res.get("se_did"),
                "pval_did": res.get("pval_did"),
                "n_obs": res.get("n_obs"),
                "n_entities": res.get("n_entities"),
            }
        )

    out = pd.DataFrame(rows)
    out.to_csv(TABLES / out_name, index=False)
    return out


def _write_bundling_did_comparison(results: dict[str, pd.DataFrame]) -> pd.DataFrame:
    frames = []
    for spec, df in results.items():
        d = df.copy()
        d.insert(0, "control_spec", spec)
        frames.append(d)
    out = pd.concat(frames, ignore_index=True)
    out.to_csv(TABLES / "bundling_did_comparison.csv", index=False)
    return out


def _top_product_quantity_examples(
    con: duckdb.DuckDBPyConnection,
    top_codes: list[str],
) -> pd.DataFrame:
    if not top_codes:
        out = pd.DataFrame()
        out.to_csv(TABLES / "top_product_quantity_examples.csv", index=False)
        return out

    code_list = ", ".join(f"'{c}'" for c in top_codes)
    query = f"""
    SELECT
        product_code8,
        max(product_name) AS product_name,
        dataset,
        post,
        COUNT(*) AS n_lines,
        round(AVG(quantity_requested), 2) AS mean_qty,
        round(median(quantity_requested), 2) AS median_qty
    FROM read_parquet('{LINES}')
    WHERE band = 'treated'
      AND product_code8 IN ({code_list})
    GROUP BY 1, 3, 4
    ORDER BY product_code8, dataset, post
    """
    out = con.execute(query).df()
    out.to_csv(TABLES / "top_product_quantity_examples.csv", index=False)
    return out


def main() -> None:
    con = duckdb.connect()
    con.execute("PRAGMA threads=8")
    con.execute("PRAGMA enable_progress_bar=false")

    print("Sector shift table ...")
    sector_shift = _sector_shift_table(con)

    print("Segment shift table ...")
    segment_shift = _shift_table(
        con,
        level_col="product_segment2",
        name_expr="product_name",
        min_pre_total=1_000,
        out_name="segment_shift_treated_band.csv",
        top_n=25,
    )

    print("Family shift table ...")
    family_shift = _shift_table(
        con,
        level_col="product_family4",
        name_expr="product_name",
        min_pre_total=500,
        out_name="family_shift_treated_band.csv",
        top_n=25,
    )

    print("Product-code shift table ...")
    product_shift = _shift_table(
        con,
        level_col="product_code8",
        name_expr="product_name",
        min_pre_total=250,
        out_name="product_code_shift_treated_band.csv",
        top_n=25,
    )

    print("Bundling cell means (all controls) ...")
    _bundling_cell_means(
        con,
        bands=["control_low", "treated", "control_high"],
        out_name="bundling_cell_means_all_controls.csv",
    )

    print("Bundling cell means (treated vs 1-30 UTM) ...")
    bundling_means_low = _bundling_cell_means(
        con,
        bands=["treated", "control_low"],
        out_name="bundling_cell_means_low_control.csv",
    )

    print("Bundling cell means (treated vs 100-200 UTM) ...")
    bundling_means = _bundling_cell_means(
        con,
        bands=["treated", "control_high"],
        out_name="bundling_cell_means_high_control.csv",
    )

    print("Tender-level bundling DiD (all controls) ...")
    bundling_did_all = _bundling_did(
        bands=["control_low", "treated", "control_high"],
        out_name="bundling_did_tender_all_controls.csv",
    )

    print("Tender-level bundling DiD (treated vs 1-30 UTM) ...")
    bundling_did_low = _bundling_did(
        bands=["treated", "control_low"],
        out_name="bundling_did_tender_low_control.csv",
    )

    print("Tender-level bundling DiD (treated vs 100-200 UTM) ...")
    bundling_did = _bundling_did(
        bands=["treated", "control_high"],
        out_name="bundling_did_tender_high_control.csv",
    )

    bundling_comp = _write_bundling_did_comparison(
        {
            "all_controls": bundling_did_all,
            "low_control": bundling_did_low,
            "high_control": bundling_did,
        }
    )

    top_codes = product_shift["level_code"].head(10).astype(str).tolist()
    print("Top-product quantity examples ...")
    qty_examples = _top_product_quantity_examples(con, top_codes)

    print("\nKey outputs")
    print(sector_shift.head(12).to_string(index=False))
    print("\nTop family movers")
    print(family_shift.head(10).to_string(index=False))
    print("\nTender-level bundling DiD")
    print(bundling_did.to_string(index=False))
    print("\nTender-level DiD by control choice")
    print(bundling_comp.to_string(index=False))
    if not qty_examples.empty:
        print("\nTop-product quantity examples")
        print(qty_examples.head(20).to_string(index=False))


if __name__ == "__main__":
    main()
