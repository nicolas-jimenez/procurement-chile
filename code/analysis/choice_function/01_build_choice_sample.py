"""
01_build_choice_sample.py
──────────────────────────────────────────────────────────────────────────────
Build the bidder x tender choice sample used to estimate buyer-level choice
functions (same-region preference vs. price). Also emits a comprehensive
diagnostics report.

Outputs (under {OUTPUT_ROOT}/choice_function/):
  samples/
    choice_sample_full.parquet
    choice_sample_licitaciones_pre.parquet
    choice_sample_licitaciones_post.parquet
    choice_sample_compra_agil_pre.parquet
    choice_sample_compra_agil_post.parquet
  diagnostics/
    sample_funnel.csv
    variable_coverage.csv
    buyer_sample_depth.csv
    buyer_depth_summary.csv
    within_tender_variation.csv
    outcome_crosstabs.csv
    price_distributions.csv
    temporal_coverage.csv
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

# ── Paths ────────────────────────────────────────────────────────────────────
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1]))
from config import DATA_CLEAN, OUTPUT_ROOT  # noqa: E402

sys.path.insert(0, str(HERE.parents[0].parent / "analysis" / "did"))
from did_utils import COMBINED, REFORM_DATE, load_utm_table  # noqa: E402

OUT_DIR     = OUTPUT_ROOT / "choice_function"
OUT_SAMPLES = OUT_DIR / "samples"
OUT_DIAG    = OUT_DIR / "diagnostics"
for _d in [OUT_DIR, OUT_SAMPLES, OUT_DIAG]:
    _d.mkdir(parents=True, exist_ok=True)

REFORM = pd.Timestamp(REFORM_DATE)

# Estados that indicate an awarded tender (licitaciones uses "Adjudicada";
# compra agil uses "OC Emitida" — an order was actually placed).
AWARDED_STATES = ("Adjudicada", "OC Emitida")


# ── Step 1: pull + aggregate to bidder×tender via DuckDB ──────────────────────
def load_and_aggregate() -> tuple[pd.DataFrame, dict]:
    """
    Stream the combined parquet through DuckDB, filter bad rows, and collapse
    line-items to one row per (tender_id, rut_bidder_raw). Returns the aggregated
    frame plus a funnel of row counts for diagnostics.
    """
    con = duckdb.connect()
    combined = str(COMBINED)

    funnel: dict[str, int] = {}

    # Raw count
    funnel["raw_rows"] = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{combined}')"
    ).fetchone()[0]

    # 1. drop key duplicates
    q_nodup = f"""
        SELECT * FROM read_parquet('{combined}')
        WHERE is_key_dup = FALSE
    """
    funnel["after_dedup"] = con.execute(f"SELECT COUNT(*) FROM ({q_nodup})").fetchone()[0]

    # 2. fecha_pub not null
    q_fp = f"{q_nodup} AND fecha_pub IS NOT NULL"
    funnel["after_fecha_pub"] = con.execute(f"SELECT COUNT(*) FROM ({q_fp})").fetchone()[0]

    # 3. monto_oferta > 0
    q_mo = f"{q_fp} AND monto_oferta IS NOT NULL AND monto_oferta > 0"
    funnel["after_monto_positive"] = con.execute(f"SELECT COUNT(*) FROM ({q_mo})").fetchone()[0]

    # 4. is_selected not null
    q_is = f"{q_mo} AND is_selected IS NOT NULL"
    funnel["after_is_selected"] = con.execute(f"SELECT COUNT(*) FROM ({q_is})").fetchone()[0]

    # 5. keep awarded states only (Adjudicada for licitaciones, OC Emitida for compra_agil)
    states_list = ",".join(f"'{s}'" for s in AWARDED_STATES)
    q_aw = f"{q_is} AND estado_tender IN ({states_list})"
    funnel["after_awarded_state"] = con.execute(f"SELECT COUNT(*) FROM ({q_aw})").fetchone()[0]

    # 6. Collapse to one row per (dataset, tender_id, rut_bidder_raw)
    # Sum monto_oferta across line items; take max(is_selected); keep bidder/buyer
    # attributes via ANY_VALUE (they are time-invariant within a tender-bidder pair).
    print("Aggregating to bidder×tender level via DuckDB ...")
    q_agg = f"""
        WITH base AS ({q_aw})
        SELECT
            dataset,
            tender_id,
            rut_bidder_raw,
            ANY_VALUE(rut_bidder)        AS rut_bidder,
            ANY_VALUE(dv_bidder)         AS dv_bidder,
            ANY_VALUE(rut_unidad)        AS rut_unidad,
            ANY_VALUE(region_buyer)      AS region_buyer,
            ANY_VALUE(comuna_buyer)      AS comuna_buyer,
            ANY_VALUE(sector)            AS sector,
            ANY_VALUE(region)            AS region_bidder,
            ANY_VALUE(comuna)            AS comuna_bidder,
            ANY_VALUE(tramoventas)       AS tramoventas,
            ANY_VALUE(ntrabajadores)     AS ntrabajadores,
            ANY_VALUE(rubro)             AS rubro,
            ANY_VALUE(tipodecontribuyente) AS tipodecontribuyente,
            ANY_VALUE(same_region)       AS same_region,
            ANY_VALUE(tipo)              AS tipo,
            ANY_VALUE(codigo_tipo)       AS codigo_tipo,
            ANY_VALUE(tipo_convocatoria) AS tipo_convocatoria,
            ANY_VALUE(estado_tender)     AS estado_tender,
            MAX(monto_estimado)          AS monto_estimado,
            SUM(monto_oferta)            AS monto_oferta,
            MAX(CAST(is_selected AS INTEGER)) AS is_selected,
            ANY_VALUE(n_oferentes)       AS n_oferentes_raw,
            MIN(fecha_pub)               AS fecha_pub,
            ANY_VALUE(source_year)       AS source_year,
            ANY_VALUE(source_month)      AS source_month,
            COUNT(*)                     AS n_lines
        FROM base
        GROUP BY dataset, tender_id, rut_bidder_raw
    """
    df = con.execute(q_agg).df()
    funnel["after_aggregate_bidder_tender"] = len(df)
    con.close()

    return df, funnel


# ── Step 2: per-tender filters (awarded, 2+ bidders, 2+ unique bids) ──────────
def filter_choice_sets(df: pd.DataFrame, funnel: dict) -> pd.DataFrame:
    # Count bidders per tender
    bid_counts = df.groupby("tender_id")["rut_bidder_raw"].transform("nunique")
    df["n_bidders"] = bid_counts.astype("int32")

    # Require at least one winner per tender
    winner_counts = df.groupby("tender_id")["is_selected"].transform("sum")
    df = df[winner_counts >= 1].copy()
    funnel["after_has_winner"] = len(df)

    # Require 2+ bidders
    df = df[df["n_bidders"] >= 2].copy()
    funnel["after_2plus_bidders"] = len(df)

    # Require same_region not null (SII match exists)
    df = df[df["same_region"].notna()].copy()
    funnel["after_same_region_match"] = len(df)

    # Recompute n_bidders after filters
    df["n_bidders"] = df.groupby("tender_id")["rut_bidder_raw"].transform("nunique").astype("int32")
    # Keep 2+ after SII-match drop
    df = df[df["n_bidders"] >= 2].copy()
    funnel["after_2plus_bidders_final"] = len(df)

    return df


# ── Step 3: derived variables ────────────────────────────────────────────────
def add_derived_vars(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["fecha_pub"]    = pd.to_datetime(df["fecha_pub"])
    df["post"]         = (df["fecha_pub"] >= REFORM).astype("int8")
    df["year_month"]   = df["fecha_pub"].dt.to_period("M").astype(str)
    df["log_bid"]      = np.log(df["monto_oferta"])
    df["log_est"]      = np.where(df["monto_estimado"] > 0,
                                   np.log(df["monto_estimado"]), np.nan)
    df["log_bid_ratio"] = np.where(df["monto_estimado"] > 0,
                                    np.log(df["monto_oferta"] / df["monto_estimado"]), np.nan)
    df["bid_discount"] = np.where(df["monto_estimado"] > 0,
                                   (df["monto_estimado"] - df["monto_oferta"]) / df["monto_estimado"],
                                   np.nan)
    df["same_region"]  = df["same_region"].astype("int8")
    df["is_selected"]  = df["is_selected"].astype("int8")

    # Within-tender bid rank and lowest flag
    df["bid_rank"]     = df.groupby("tender_id")["monto_oferta"].rank(method="min").astype("int32")
    min_per_tender     = df.groupby("tender_id")["monto_oferta"].transform("min")
    df["is_lowest_bid"] = (df["monto_oferta"] == min_per_tender).astype("int8")

    # SME from tramoventas (1–9 → 1, 10–13 → 0, else NA)
    tv = pd.to_numeric(df["tramoventas"], errors="coerce")
    sme = np.where(tv.between(1, 9),  1,
          np.where(tv.between(10, 13), 0, np.nan))
    df["sme"] = sme.astype("float32")

    # UTM conversion
    try:
        utm = load_utm_table()
        df = df.merge(utm, on=["source_year", "source_month"], how="left")
        df["monto_utm"] = df["monto_estimado"] / df["utm_clp"]
    except Exception as exc:
        print(f"  UTM merge failed: {exc}")
        df["monto_utm"] = np.nan

    # Tender-level locality aggregates
    g = df.groupby("tender_id")
    df["n_local_bidders"]    = g["same_region"].transform("sum").astype("int32")
    df["share_local_bidders"] = df["n_local_bidders"] / df["n_bidders"]
    df["has_local_bidder"]    = (df["n_local_bidders"] >= 1).astype("int8")

    # local_is_cheapest: lowest bid in the tender comes from a local firm
    lowest_local = df[df["is_lowest_bid"] == 1].groupby("tender_id")["same_region"].max()
    df["local_is_cheapest"] = df["tender_id"].map(lowest_local).fillna(0).astype("int8")

    # log employee count
    nt = pd.to_numeric(df["ntrabajadores"], errors="coerce")
    df["log_ntrabajadores"] = np.log1p(nt)

    return df


# ── Diagnostics ──────────────────────────────────────────────────────────────
def diag_funnel(funnel: dict, df_final: pd.DataFrame) -> None:
    rows = []
    prev = None
    steps = [
        ("raw_rows",                       "Raw combined file"),
        ("after_dedup",                    "After dropping is_key_dup"),
        ("after_fecha_pub",                "After requiring fecha_pub not null"),
        ("after_monto_positive",           "After requiring monto_oferta > 0"),
        ("after_is_selected",              "After requiring is_selected not null"),
        ("after_awarded_state",            "After keeping awarded states (Adjudicada/OC Emitida)"),
        ("after_aggregate_bidder_tender",  "After aggregating to bidder×tender"),
        ("after_has_winner",               "After keeping tenders with >=1 winner"),
        ("after_2plus_bidders",            "After requiring 2+ bidders"),
        ("after_same_region_match",        "After requiring same_region not null"),
        ("after_2plus_bidders_final",      "After re-enforcing 2+ bidders post SII filter"),
    ]
    for k, label in steps:
        n = funnel.get(k)
        drop_pct = None if prev is None or prev == 0 else 100 * (prev - n) / prev
        rows.append({"step": label, "n_rows": n, "pct_drop_from_prev": drop_pct})
        prev = n
    rows.append({
        "step": "Final choice sample (written to disk)",
        "n_rows": len(df_final),
        "pct_drop_from_prev": None,
    })
    out = pd.DataFrame(rows)
    out.to_csv(OUT_DIAG / "sample_funnel.csv", index=False)
    print("\n=== SAMPLE FUNNEL ===")
    print(out.to_string(index=False))


def diag_coverage(df: pd.DataFrame) -> None:
    cols = [
        "dataset", "tender_id", "rut_bidder_raw", "rut_unidad",
        "region_buyer", "region_bidder", "sector",
        "same_region", "is_selected", "is_lowest_bid",
        "monto_oferta", "monto_estimado", "log_bid_ratio",
        "tramoventas", "sme", "ntrabajadores",
        "tipo", "fecha_pub", "monto_utm",
    ]
    rows = []
    n_total = len(df)
    for c in cols:
        if c not in df.columns:
            continue
        nn = int(df[c].notna().sum())
        rows.append({
            "variable": c,
            "n_non_missing": nn,
            "n_total": n_total,
            "share_non_missing": nn / n_total if n_total else np.nan,
        })
    pd.DataFrame(rows).to_csv(OUT_DIAG / "variable_coverage.csv", index=False)

    # same_region share by dataset × post
    g = df.groupby(["dataset", "post"]).agg(
        n_rows=("same_region", "size"),
        share_same_region=("same_region", "mean"),
    ).reset_index()
    g.to_csv(OUT_DIAG / "same_region_by_dataset_post.csv", index=False)


def diag_buyer_depth(df: pd.DataFrame) -> None:
    # Basic depth metrics
    buyer = df.groupby("rut_unidad").agg(
        n_awarded_tenders=("tender_id", "nunique"),
        n_bids=("tender_id", "size"),
        avg_n_bidders=("n_bidders", "mean"),
    ).reset_index()
    # Share of winners that are local (only look at winners per buyer)
    winners = df[df["is_selected"] == 1]
    local_share = winners.groupby("rut_unidad")["same_region"].mean().rename("share_local_winner").reset_index()
    buyer = buyer.merge(local_share, on="rut_unidad", how="left")
    # Split pre/post counts
    tend_buyer = df.drop_duplicates(subset=["tender_id"])[["tender_id", "rut_unidad", "post"]]
    pre_post = tend_buyer.groupby(["rut_unidad", "post"]).size().unstack(fill_value=0)
    pre_post.columns = [f"n_tenders_post{int(c)}" for c in pre_post.columns]
    pre_post = pre_post.reset_index()
    buyer = buyer.merge(pre_post, on="rut_unidad", how="left")
    buyer.to_csv(OUT_DIAG / "buyer_sample_depth.csv", index=False)

    # Summary distribution
    thresholds = [1, 5, 10, 20, 50, 100, 200]
    rows = [{"min_awarded_tenders": t,
             "n_buyers": int((buyer["n_awarded_tenders"] >= t).sum())}
            for t in thresholds]
    pd.DataFrame(rows).to_csv(OUT_DIAG / "buyer_depth_summary.csv", index=False)


def diag_within_tender_variation(df: pd.DataFrame) -> None:
    rows = []
    for (ds, post), sub in df.groupby(["dataset", "post"]):
        g = sub.groupby("tender_id")
        sr_var = g["same_region"].transform("nunique") > 1
        sme_var = g["sme"].transform(lambda s: s.dropna().nunique()) > 1
        winner_lowest = (sub["is_selected"] == 1) & (sub["is_lowest_bid"] == 1)
        winner_not_lowest = (sub["is_selected"] == 1) & (sub["is_lowest_bid"] == 0)
        n_tenders = sub["tender_id"].nunique()
        rows.append({
            "dataset": ds,
            "post": int(post),
            "n_tenders": int(n_tenders),
            "share_tenders_sr_varies": float(sr_var.groupby(sub["tender_id"]).any().mean()),
            "share_tenders_sme_varies": float(sme_var.groupby(sub["tender_id"]).any().mean()),
            "share_winners_lowest":     float(winner_lowest.sum() / max((sub["is_selected"] == 1).sum(), 1)),
            "share_winners_not_lowest": float(winner_not_lowest.sum() / max((sub["is_selected"] == 1).sum(), 1)),
        })
    pd.DataFrame(rows).to_csv(OUT_DIAG / "within_tender_variation.csv", index=False)


def diag_outcomes(df: pd.DataFrame) -> None:
    rows = []
    for (ds, post), sub in df.groupby(["dataset", "post"]):
        sel = sub["is_selected"]
        low = sub["is_lowest_bid"]
        sr = sub["same_region"]
        rows.append({
            "dataset": ds, "post": int(post),
            "n_bids": len(sub),
            "share_selected": float(sel.mean()),
            "share_lowest_bid": float(low.mean()),
            "share_local_bidders": float(sr.mean()),
            "share_winners_lowest": float(((sel == 1) & (low == 1)).sum() / max((sel == 1).sum(), 1)),
            "share_winners_local": float(((sel == 1) & (sr == 1)).sum() / max((sel == 1).sum(), 1)),
            "share_winners_local_and_lowest": float(((sel == 1) & (sr == 1) & (low == 1)).sum() / max((sel == 1).sum(), 1)),
            "tenders_multiple_winners": int((sub.groupby("tender_id")["is_selected"].sum() > 1).sum()),
        })
    pd.DataFrame(rows).to_csv(OUT_DIAG / "outcome_crosstabs.csv", index=False)


def diag_price(df: pd.DataFrame) -> None:
    rows = []
    for (ds, post), sub in df.groupby(["dataset", "post"]):
        q = [0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99]
        row = {"dataset": ds, "post": int(post), "n": len(sub)}
        for name, series in [
            ("monto_oferta",    sub["monto_oferta"]),
            ("monto_estimado",  sub["monto_estimado"]),
            ("log_bid_ratio",   sub["log_bid_ratio"]),
        ]:
            valid = series.dropna()
            for p in q:
                row[f"{name}_p{int(p*100)}"] = float(valid.quantile(p)) if len(valid) else np.nan
        lbr = sub["log_bid_ratio"].dropna()
        row["share_lbr_gt_2"]  = float((lbr > 2).mean())  if len(lbr) else np.nan
        row["share_lbr_lt_m2"] = float((lbr < -2).mean()) if len(lbr) else np.nan
        row["share_bid_eq_estimate"] = float((sub["monto_oferta"] == sub["monto_estimado"]).mean())
        rows.append(row)
    pd.DataFrame(rows).to_csv(OUT_DIAG / "price_distributions.csv", index=False)


def diag_temporal(df: pd.DataFrame) -> None:
    g = df.groupby(["dataset", "year_month"]).agg(
        n_bids=("tender_id", "size"),
        n_tenders=("tender_id", "nunique"),
    ).reset_index().sort_values(["dataset", "year_month"])
    g.to_csv(OUT_DIAG / "temporal_coverage.csv", index=False)

    # Monto_utm distribution pre vs post for compra_agil
    ca = df[df["dataset"] == "compra_agil"]
    rows = []
    for p in [0, 1]:
        sub = ca[ca["post"] == p]["monto_utm"].dropna()
        if len(sub):
            rows.append({
                "dataset": "compra_agil", "post": p, "n": len(sub),
                "p5":  float(sub.quantile(0.05)),  "p25": float(sub.quantile(0.25)),
                "p50": float(sub.quantile(0.50)),  "p75": float(sub.quantile(0.75)),
                "p95": float(sub.quantile(0.95)),  "mean": float(sub.mean()),
            })
    if rows:
        pd.DataFrame(rows).to_csv(OUT_DIAG / "compra_agil_monto_utm_pre_post.csv", index=False)


# ── Main ──────────────────────────────────────────────────────────────────────
def main() -> None:
    print("=" * 70)
    print("Building choice sample from", COMBINED)
    print("=" * 70)
    df, funnel = load_and_aggregate()
    print(f"After aggregate: {len(df):,} rows")

    df = filter_choice_sets(df, funnel)
    print(f"After choice-set filters: {len(df):,} rows")

    df = add_derived_vars(df)
    print(f"Final with derived vars: {len(df):,} rows, {df['tender_id'].nunique():,} tenders, "
          f"{df['rut_unidad'].nunique():,} buyers, {df['rut_bidder_raw'].nunique():,} bidders.")

    # Save full + splits
    print("\nWriting sample files ...")
    df.to_parquet(OUT_SAMPLES / "choice_sample_full.parquet", index=False)
    for ds in ["licitaciones", "compra_agil"]:
        for p in [0, 1]:
            sub = df[(df["dataset"] == ds) & (df["post"] == p)].copy()
            suffix = "pre" if p == 0 else "post"
            out = OUT_SAMPLES / f"choice_sample_{ds}_{suffix}.parquet"
            sub.to_parquet(out, index=False)
            print(f"  {out.name}: {len(sub):,} bids, {sub['tender_id'].nunique():,} tenders, "
                  f"{sub['rut_unidad'].nunique():,} buyers")

    # Diagnostics
    print("\nGenerating diagnostics ...")
    diag_funnel(funnel, df)
    diag_coverage(df)
    diag_buyer_depth(df)
    diag_within_tender_variation(df)
    diag_outcomes(df)
    diag_price(df)
    diag_temporal(df)
    print(f"\nAll diagnostics written to {OUT_DIAG}")


if __name__ == "__main__":
    main()
