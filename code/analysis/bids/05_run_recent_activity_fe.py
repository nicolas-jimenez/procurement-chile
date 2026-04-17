"""
05_run_recent_activity_fe.py
Estimate bid-level markup regressions with bidder and year-month fixed effects,
using recent bidder activity as a workload proxy.

Supported activity measures
  bids  : recent bidder-tender participations in did_bid_sample.parquet,
          timed using fecha_pub for consistency across datasets
  wins  : recent awarded tenders

Supported scopes
  all         : activity in the full procurement universe
  same_sector : activity only within the current buyer sector

Outputs
  output/bids/tables/recent_<activity>_fe_results.csv
  output/bids/tables/recent_<activity>_sample_stats.csv
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
from scipy.special import ndtr

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
sys.path.insert(0, str(ROOT / "code" / "analysis" / "did"))

from did_utils import CA_PANEL, COMBINED, LIC_PANEL, OUT_SAMPLES, _cluster_se, _twoway_demean  # noqa: E402

SECTORS = ["Municipalidades", "Obras Públicas"]
OUT_BIDS = ROOT / "output" / "bids"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run recent-activity FE regressions.")
    parser.add_argument(
        "--activity",
        choices=["bids", "wins"],
        default="bids",
        help="Recent activity measure to use in the workload proxy.",
    )
    parser.add_argument(
        "--start-date",
        default="2022-04-01",
        help="Outcome-sample lower bound on bid publication date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--scope",
        choices=["all", "same_sector", "both"],
        default="both",
        help="Workload scope to estimate.",
    )
    return parser.parse_args()


def _bidder_expr() -> str:
    return r"""
CASE
  WHEN rut_bidder IS NOT NULL
       AND regexp_extract(upper(trim(coalesce(dv_bidder, ''))), '([0-9K])', 1) <> ''
    THEN CAST(CAST(rut_bidder AS BIGINT) AS VARCHAR) || '-' ||
         regexp_extract(upper(trim(coalesce(dv_bidder, ''))), '([0-9K])', 1)
  WHEN regexp_extract(
         upper(regexp_replace(coalesce(rut_bidder_raw, ''), '[^0-9K]', '', 'g')),
         '^(\d+)', 1
       ) <> ''
       AND regexp_extract(
         upper(regexp_replace(coalesce(rut_bidder_raw, ''), '[^0-9K]', '', 'g')),
         '([0-9K])$', 1
       ) <> ''
    THEN regexp_extract(
           upper(regexp_replace(coalesce(rut_bidder_raw, ''), '[^0-9K]', '', 'g')),
           '^(\d+)', 1
         )
         || '-' ||
         regexp_extract(
           upper(regexp_replace(coalesce(rut_bidder_raw, ''), '[^0-9K]', '', 'g')),
           '([0-9K])$', 1
         )
  ELSE NULL
END
"""


def _build_timing_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE ca_tender_close AS
        SELECT
            CAST(CodigoCotizacion AS VARCHAR) AS tender_id,
            CAST(MAX(FechaCierreParaCotizar) AS DATE) AS tender_close_date
        FROM read_parquet('{CA_PANEL}')
        WHERE CodigoCotizacion IS NOT NULL
        GROUP BY 1
        """
    )
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE lic_award_dates AS
        SELECT
            CAST(Codigo AS VARCHAR) AS tender_id,
            CAST(MAX(FechaAdjudicacion) AS DATE) AS award_date
        FROM read_parquet('{LIC_PANEL}')
        WHERE Codigo IS NOT NULL
        GROUP BY 1
        """
    )
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE lic_bid_dates AS
        SELECT
            CAST(Codigo AS VARCHAR) AS tender_id,
            trim(CAST(RutProveedor AS VARCHAR)) AS rut_bidder_raw,
            CAST(MAX(FechaEnvioOferta) AS DATE) AS bid_date_lic
        FROM read_parquet('{LIC_PANEL}')
        WHERE Codigo IS NOT NULL
          AND RutProveedor IS NOT NULL
        GROUP BY 1, 2
        """
    )


def _build_activity_universe(
    con: duckdb.DuckDBPyConnection,
    *,
    activity: str,
) -> None:
    bidder_expr = _bidder_expr()
    if activity == "bids":
        did_sample = OUT_SAMPLES / "did_bid_sample.parquet"
        con.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE activity_events AS
            SELECT
                sector,
                bidder_id,
                CAST(fecha_pub AS DATE) AS activity_date
            FROM read_parquet('{did_sample}')
            WHERE bidder_id IS NOT NULL
              AND fecha_pub IS NOT NULL
            """
        )
    else:
        con.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE activity_events AS
            WITH raw_wins AS (
                SELECT DISTINCT
                    c.dataset,
                    CAST(c.tender_id AS VARCHAR) AS tender_id,
                    c.sector,
                    {bidder_expr} AS bidder_id,
                    CAST(c.fecha_pub AS DATE) AS fecha_pub
                FROM read_parquet('{COMBINED}') c
                WHERE c.is_selected = TRUE
                  AND c.fecha_pub IS NOT NULL
            )
            SELECT
                w.sector,
                w.bidder_id,
                COALESCE(lad.award_date, w.fecha_pub) AS activity_date
            FROM raw_wins w
            LEFT JOIN lic_award_dates lad
              ON w.dataset = 'licitaciones'
             AND w.tender_id = lad.tender_id
            WHERE w.bidder_id IS NOT NULL
            """
        )

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE activity_daily AS
        SELECT
            sector,
            bidder_id,
            activity_date,
            COUNT(*)::DOUBLE AS n_events
        FROM activity_events
        WHERE activity_date IS NOT NULL
        GROUP BY 1, 2, 3
        """
    )


def _build_outcome_sample(
    con: duckdb.DuckDBPyConnection,
    *,
    start_date: str,
) -> None:
    did_sample = OUT_SAMPLES / "did_bid_sample.parquet"
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE reg_base AS
        SELECT
            sector,
            dataset,
            bidder_id,
            CAST(fecha_pub AS DATE) AS bid_date,
            strftime(CAST(fecha_pub AS DATE), '%Y-%m') AS year_month_str,
            CAST(log_sub_price_ratio AS DOUBLE) AS y,
            CAST(monto_estimado AS DOUBLE) AS monto_estimado,
            CAST(same_region AS DOUBLE) AS same_region
        FROM read_parquet('{did_sample}')
        WHERE sector IN ('Municipalidades', 'Obras Públicas')
          AND bidder_id IS NOT NULL
          AND fecha_pub IS NOT NULL
          AND log_sub_price_ratio IS NOT NULL
          AND monto_estimado > 0
          AND CAST(fecha_pub AS DATE) >= DATE '{start_date}'
        """
    )


def _load_sector_sample(
    con: duckdb.DuckDBPyConnection,
    *,
    sector: str,
    scope: str,
) -> pd.DataFrame:
    if scope == "all":
        event_dates = """
    event_dates AS (
      SELECT DISTINCT bidder_id, bid_date AS event_date
      FROM reg_base
      WHERE sector = ?
      UNION
      SELECT DISTINCT a.bidder_id, a.activity_date AS event_date
      FROM activity_daily a
      JOIN sector_bidders sb USING (bidder_id)
    ),
    date_panel AS (
      SELECT
          e.bidder_id,
          e.event_date AS bid_date,
          COALESCE(a.n_events, 0.0) AS n_events
      FROM event_dates e
      LEFT JOIN activity_daily a
        ON e.bidder_id = a.bidder_id
       AND e.event_date = a.activity_date
    ),
        """
        workload_cols = ""
        scope_on = "r.bidder_id = w.bidder_id AND r.bid_date = w.bid_date"
        params = [sector, sector, sector]
    else:
        event_dates = """
    event_dates AS (
      SELECT DISTINCT bidder_id, sector, bid_date AS event_date
      FROM reg_base
      WHERE sector = ?
      UNION
      SELECT DISTINCT bidder_id, sector, activity_date AS event_date
      FROM activity_daily
      WHERE sector = ?
    ),
    date_panel AS (
      SELECT
          e.bidder_id,
          e.sector,
          e.event_date AS bid_date,
          COALESCE(a.n_events, 0.0) AS n_events
      FROM event_dates e
      LEFT JOIN activity_daily a
        ON e.bidder_id = a.bidder_id
       AND e.sector = a.sector
       AND e.event_date = a.activity_date
    ),
        """
        workload_cols = ", sector"
        scope_on = "r.bidder_id = w.bidder_id AND r.sector = w.sector AND r.bid_date = w.bid_date"
        params = [sector, sector, sector, sector]

    query = f"""
    WITH sector_bidders AS (
      SELECT DISTINCT bidder_id
      FROM reg_base
      WHERE sector = ?
    ),
    {event_dates}
    workload AS (
      SELECT
          bidder_id{workload_cols},
          bid_date,
          COALESCE(
            SUM(n_events) OVER (
              PARTITION BY bidder_id{workload_cols}
              ORDER BY bid_date
              RANGE BETWEEN INTERVAL 1 MONTH PRECEDING AND INTERVAL 1 DAY PRECEDING
            ),
            0.0
          ) AS recent_1m,
          COALESCE(
            SUM(n_events) OVER (
              PARTITION BY bidder_id{workload_cols}
              ORDER BY bid_date
              RANGE BETWEEN INTERVAL 3 MONTH PRECEDING AND INTERVAL 1 DAY PRECEDING
            ),
            0.0
          ) AS recent_3m
      FROM date_panel
    ),
    sec AS (
      SELECT
          r.y,
          r.monto_estimado,
          r.same_region,
          CASE WHEN r.dataset = 'compra_agil' THEN 1.0 ELSE 0.0 END AS dataset_ca,
          COALESCE(w.recent_1m, 0.0) AS recent_1m,
          COALESCE(w.recent_3m, 0.0) AS recent_3m,
          r.bidder_id,
          r.year_month_str
      FROM reg_base r
      LEFT JOIN workload w
        ON {scope_on}
      WHERE r.sector = ?
    ),
    entity_map AS (
      SELECT bidder_id, row_number() OVER (ORDER BY bidder_id) AS entity_id
      FROM (SELECT DISTINCT bidder_id FROM sec)
    ),
    time_map AS (
      SELECT year_month_str, row_number() OVER (ORDER BY year_month_str) AS time_id
      FROM (SELECT DISTINCT year_month_str FROM sec)
    )
    SELECT
        s.y,
        s.monto_estimado,
        s.same_region,
        s.dataset_ca,
        s.recent_1m,
        s.recent_3m,
        e.entity_id,
        t.time_id
    FROM sec s
    JOIN entity_map e USING (bidder_id)
    JOIN time_map t USING (year_month_str)
    """
    return con.execute(query, params).df()


def _run_twfe(df: pd.DataFrame, recent_col: str) -> dict[str, float | int | str]:
    sub = (
        df[["y", recent_col, "log_monto_est", "same_region", "dataset_ca", "entity_id", "time_id"]]
        .replace([np.inf, -np.inf], np.nan)
        .dropna()
        .copy()
    )
    entity_sizes = sub.groupby("entity_id", sort=False)["y"].transform("count")
    sub = sub[entity_sizes > 1].copy()

    demean_cols = ["y", recent_col, "log_monto_est", "same_region", "dataset_ca"]
    sub_dm = _twoway_demean(sub, "entity_id", "time_id", demean_cols, n_iter=8)

    X = sub_dm[[recent_col, "log_monto_est", "same_region", "dataset_ca"]].to_numpy(dtype=float)
    y = sub_dm["y"].to_numpy(dtype=float)
    clu = sub["entity_id"].to_numpy()

    valid = np.isfinite(y) & np.isfinite(X).all(axis=1)
    X = X[valid]
    y = y[valid]
    clu = clu[valid]

    coefs, _, _, _ = np.linalg.lstsq(X, y, rcond=None)
    resid = y - X @ coefs
    se = _cluster_se(X, resid, clu)

    coef = float(coefs[0])
    se0 = float(se[0])
    tstat = coef / se0 if se0 > 0 else np.nan
    pval = float(2 * (1 - ndtr(abs(tstat)))) if np.isfinite(tstat) else np.nan

    return {
        "regressor": recent_col,
        "n_obs": int(len(y)),
        "n_bidders": int(pd.Series(clu).nunique()),
        "coef": coef,
        "se": se0,
        "t": tstat,
        "p": pval,
        "ci_low": coef - 1.96 * se0,
        "ci_high": coef + 1.96 * se0,
        "pct_effect_per_unit": 100.0 * coef,
        "controls": "log_monto_est + same_region + dataset_ca",
        "fixed_effects": "bidder + year_month",
        "log_monto_est_coef": float(coefs[1]),
        "log_monto_est_se": float(se[1]),
        "same_region_coef": float(coefs[2]),
        "same_region_se": float(se[2]),
        "dataset_ca_coef": float(coefs[3]),
        "dataset_ca_se": float(se[3]),
    }


def main() -> None:
    args = _parse_args()
    out_tbl = OUT_BIDS / "tables"
    out_tbl.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.execute("PRAGMA threads=8")
    con.execute("PRAGMA enable_progress_bar=false")

    if args.activity == "wins":
        print("Building timing tables ...")
        _build_timing_tables(con)
    print(f"Building {args.activity} activity universe ...")
    _build_activity_universe(con, activity=args.activity)
    print("Building outcome sample ...")
    _build_outcome_sample(con, start_date=args.start_date)

    stats_rows: list[dict[str, float | int | str]] = []
    result_rows: list[dict[str, float | int | str]] = []

    scopes = ["all", "same_sector"] if args.scope == "both" else [args.scope]
    for scope in scopes:
        print(f"Scope: {scope}")
        for sector in SECTORS:
            print(f"  Loading {sector} ...")
            df = _load_sector_sample(con, sector=sector, scope=scope)
            df["log_monto_est"] = np.log(df["monto_estimado"])
            df["any_recent_1m"] = (df["recent_1m"] > 0).astype(float)
            df["any_recent_3m"] = (df["recent_3m"] > 0).astype(float)
            df["log1p_recent_1m"] = np.log1p(df["recent_1m"])
            df["log1p_recent_3m"] = np.log1p(df["recent_3m"])

            stats_rows.append(
                {
                    "activity": args.activity,
                    "scope": scope,
                    "sector": sector,
                    "n_rows_raw": int(len(df)),
                    "n_bidders": int(df["entity_id"].nunique()),
                    "n_months": int(df["time_id"].nunique()),
                    "share_ca": float(df["dataset_ca"].mean()),
                    "share_missing_same_region": float(df["same_region"].isna().mean()),
                    "mean_recent_1m": float(df["recent_1m"].mean()),
                    "p90_recent_1m": float(df["recent_1m"].quantile(0.9)),
                    "share_any_recent_1m": float((df["recent_1m"] > 0).mean()),
                    "mean_recent_3m": float(df["recent_3m"].mean()),
                    "p90_recent_3m": float(df["recent_3m"].quantile(0.9)),
                    "share_any_recent_3m": float((df["recent_3m"] > 0).mean()),
                    "mean_y": float(df["y"].mean()),
                }
            )

            for transform, recent_col in [
                ("count", "recent_1m"),
                ("count", "recent_3m"),
                ("any", "any_recent_1m"),
                ("any", "any_recent_3m"),
                ("log1p", "log1p_recent_1m"),
                ("log1p", "log1p_recent_3m"),
            ]:
                row = _run_twfe(df, recent_col)
                row["activity"] = args.activity
                row["scope"] = scope
                row["sector"] = sector
                row["transform"] = transform
                row["window"] = "1m" if recent_col.endswith("1m") else "3m"
                result_rows.append(row)

    res_df = pd.DataFrame(result_rows)
    stats_df = pd.DataFrame(stats_rows)

    res_path = out_tbl / f"recent_{args.activity}_fe_results.csv"
    stats_path = out_tbl / f"recent_{args.activity}_sample_stats.csv"
    res_df.to_csv(res_path, index=False)
    stats_df.to_csv(stats_path, index=False)

    print("\nSample stats:")
    print(stats_df.to_string(index=False))
    print("\nRegression results:")
    print(
        res_df[
            [
                "activity",
                "scope",
                "sector",
                "transform",
                "window",
                "regressor",
                "n_obs",
                "n_bidders",
                "coef",
                "se",
                "p",
                "pct_effect_per_unit",
            ]
        ].to_string(index=False)
    )
    print(f"\nSaved: {res_path}")
    print(f"Saved: {stats_path}")


if __name__ == "__main__":
    main()
