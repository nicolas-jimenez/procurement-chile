"""
03_entry_bidding.py
─────────────────────────────────────────────────────────────────────────────
Regression analysis: effect of simultaneous bidding on
  (1) entry into new markets (from bid_level_simult.parquet)
  (2) bid levels / log sub-price ratio (from output/bids/bid_analysis_sample.parquet,
      which has actual bid amounts)

Inputs
  output/simultaneousbids/bid_level_simult.parquet   (entry analysis)
  output/simultaneousbids/firm_month_panel.parquet   (sim bid counts)
  output/bids/bid_analysis_sample.parquet            (bid ratio analysis)

Outputs  output/simultaneousbids/tables/
  t8_entry_regressions.csv
  t9_bid_level_regressions.csv
  t10_munic_vs_obras.csv
  t11_pre_post_regressions.csv
  t12_size_heterogeneity.csv
"""

from __future__ import annotations
import gc
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import statsmodels.formula.api as smf

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config import OUTPUT_ROOT  # noqa: E402

OUT  = OUTPUT_ROOT / "simultaneousbids"
TBLS = OUT / "tables"
TBLS.mkdir(parents=True, exist_ok=True)

REFORM_MONTH = "2024-12"

# ── OLS helper ────────────────────────────────────────────────────────────────
def run_ols(formula: str, data: pd.DataFrame, label: str = "") -> dict:
    clean_cols = []
    parts = formula.replace("~", "+").replace(":", "+").replace("*", "+")
    import re
    for tok in re.split(r"[+\s]+", parts):
        tok = tok.strip()
        if tok and not tok.startswith("np.") and tok.isidentifier():
            clean_cols.append(tok)

    d = data.dropna(subset=[c for c in clean_cols if c in data.columns])
    if len(d) < 50:
        return {"sample": label, "formula": formula, "n_obs": len(d), "error": "too few obs"}
    try:
        res = smf.ols(formula, data=d).fit(cov_type="HC1")
        row = {
            "sample": label, "formula": formula,
            "n_obs": int(res.nobs), "r2": round(res.rsquared, 4),
        }
        for k in res.params.index:
            row[k]            = round(float(res.params[k]), 6)
            row[k + "_se"]    = round(float(res.bse[k]), 6)
            row[k + "_p"]     = round(float(res.pvalues[k]), 4)
            row[k + "_stars"] = (
                "***" if res.pvalues[k] < 0.01 else
                "**"  if res.pvalues[k] < 0.05 else
                "*"   if res.pvalues[k] < 0.10 else ""
            )
        return row
    except Exception as e:
        return {"sample": label, "formula": formula, "error": str(e)}


# ── Load firm-month panel ─────────────────────────────────────────────────────
print("Loading firm-month panel…")
fm = pd.read_parquet(OUT / "firm_month_panel.parquet")
fm["ym_str"] = fm["ym"].astype(str)
fm["log_n_sim_lag1"]    = np.log1p(fm["n_sim_lag1"])
fm["log_n_sim_nl_lag1"] = np.log1p(fm["n_sim_nl_lag1"])
print(f"  {len(fm):,} firm-month rows")


# ─────────────────────────────────────────────────────────────────────────────
# PART 1: ENTRY REGRESSIONS
# Using bid_level_simult.parquet (has first_bid_in_region + n_sim_lag1)
# ─────────────────────────────────────────────────────────────────────────────
print("\nLoading entry analysis data…")
ENTRY_COLS = ["tender_id","rut_bidder","ym","sector","post","same_region",
              "size_group","region_buyer_n","monto_estimado",
              "n_sim_lag1","n_sim_nl_lag1","first_bid_in_region"]
ent = pd.read_parquet(OUT / "bid_level_simult.parquet", columns=ENTRY_COLS)
ent["log_monto_est"]     = np.log(ent["monto_estimado"].replace(0, np.nan))
ent["log_n_sim_lag1"]    = np.log1p(ent["n_sim_lag1"])
ent["log_n_sim_nl_lag1"] = np.log1p(ent["n_sim_nl_lag1"])
ent["pre_post"] = np.where(ent["post"]==1, "Post-reform", "Pre-reform")
print(f"  {len(ent):,} rows, {ent['rut_bidder'].nunique():,} firms")

print("[T8] Entry regressions…")
e_rows = []

# (1) Baseline — all sectors
e_rows.append(run_ols(
    "first_bid_in_region ~ log_n_sim_lag1 + log_n_sim_nl_lag1 + log_monto_est",
    ent, "All | Baseline OLS"
))
# (2) + post interaction
e_rows.append(run_ols(
    "first_bid_in_region ~ log_n_sim_lag1 + log_n_sim_nl_lag1 + post + "
    "log_n_sim_lag1:post + log_n_sim_nl_lag1:post + log_monto_est",
    ent, "All | + Post interaction"
))
# (3) Municipalidades
mu = ent[ent["sector"] == "Municipalidades"]
e_rows.append(run_ols(
    "first_bid_in_region ~ log_n_sim_lag1 + log_n_sim_nl_lag1 + log_monto_est",
    mu, "Municipalidades | Baseline"
))
# (4) Municipalidades + post
e_rows.append(run_ols(
    "first_bid_in_region ~ log_n_sim_lag1 + log_n_sim_nl_lag1 + post + "
    "log_n_sim_lag1:post + log_n_sim_nl_lag1:post + log_monto_est",
    mu, "Municipalidades | + Post"
))
# (5) Obras Públicas
ob = ent[ent["sector"] == "Obras Públicas"]
e_rows.append(run_ols(
    "first_bid_in_region ~ log_n_sim_lag1 + log_n_sim_nl_lag1 + log_monto_est",
    ob, "Obras Públicas | Baseline"
))
# (6) Obras + post
e_rows.append(run_ols(
    "first_bid_in_region ~ log_n_sim_lag1 + log_n_sim_nl_lag1 + post + "
    "log_n_sim_lag1:post + log_n_sim_nl_lag1:post + log_monto_est",
    ob, "Obras Públicas | + Post"
))

t8 = pd.DataFrame(e_rows)
t8.to_csv(TBLS / "t8_entry_regressions.csv", index=False)
_key_cols = ["sample","n_obs","r2",
             "log_n_sim_lag1","log_n_sim_lag1_se","log_n_sim_lag1_stars",
             "log_n_sim_nl_lag1","log_n_sim_nl_lag1_se","log_n_sim_nl_lag1_stars"]
print(t8[[c for c in _key_cols if c in t8.columns]].to_string())

del ent, mu, ob; gc.collect()


# ─────────────────────────────────────────────────────────────────────────────
# PART 2: BID-LEVEL REGRESSIONS
# Using bid_analysis_sample.parquet (licitaciones, Municipalidades + Obras)
# Merge with firm_month_panel for simultaneous bid counts
# ─────────────────────────────────────────────────────────────────────────────
print("\nLoading bid-analysis sample (licitaciones subset)…")

# Parse bidder_id → numeric rut for matching with firm_month
def parse_rut(s: str) -> int | float:
    """'76.956.121-8' → 76956121 (the numeric part without check digit)."""
    try:
        return int(str(s).replace(".", "").split("-")[0])
    except Exception:
        return np.nan

# Load only licitaciones × [Municipalidades, Obras Públicas]
BID_COLS = ["bidder_id","year_month","fecha_pub","sector","dataset",
            "log_sub_price_ratio","same_region","sme_sii","is_selected",
            "monto_estimado","monto_utm","post","is_new_entrant"]
chunks = []
pf_bid = pq.ParquetFile(OUTPUT_ROOT / "bids" / "bid_analysis_sample.parquet")
for batch in pf_bid.iter_batches(batch_size=300_000, columns=BID_COLS):
    d = batch.to_pandas()
    d = d[(d["dataset"] == "licitaciones") &
          (d["sector"].isin(["Municipalidades", "Obras Públicas"]))]
    if len(d) > 0:
        chunks.append(d)

bid = pd.concat(chunks, ignore_index=True)
del chunks; gc.collect()
print(f"  Loaded {len(bid):,} rows  ({bid['sector'].value_counts().to_dict()})")

# Parse firm ID and year-month to match with firm_month_panel
bid["rut_bidder"] = bid["bidder_id"].apply(parse_rut).astype("Int64")
bid["ym_str"]     = bid["year_month"].astype(str)  # Period → "YYYY-MM"

# Merge simultaneous bid counts
print("  Merging simultaneous bid counts…")
fm_merge = fm[["rut_bidder","ym_str","n_sim","n_sim_nonlocal","n_sim_local",
               "n_sim_lag1","n_sim_nl_lag1","size_group","avg_dist_km",
               "log_n_sim_lag1","log_n_sim_nl_lag1"]].copy()
fm_merge["rut_bidder"] = fm_merge["rut_bidder"].astype("Int64")
bid = bid.merge(fm_merge, on=["rut_bidder","ym_str"], how="left")

# Key variables
bid["log_n_sim"]     = np.log1p(bid["n_sim"])
bid["log_n_sim_nl"]  = np.log1p(bid["n_sim_nonlocal"])
bid["log_monto_est"] = np.log(bid["monto_estimado"].replace(0, np.nan))

# Trim extreme bid ratios
bid = bid[bid["log_sub_price_ratio"].between(-3, 3) | bid["log_sub_price_ratio"].isna()]
bid["pre_post"] = np.where(bid["post"]==1, "Post-reform", "Pre-reform")

n_matched = bid["n_sim"].notna().sum()
print(f"  Matched n_sim: {n_matched:,} / {len(bid):,} rows ({100*n_matched/len(bid):.1f}%)")
print(f"  log_sub_price_ratio non-null: {bid['log_sub_price_ratio'].notna().sum():,}")

# ─────────────────────────────────────────────────────────────────────────────
# T9: Bid-level regressions
# ─────────────────────────────────────────────────────────────────────────────
print("\n[T9] Bid-level regressions…")
b_rows = []

# (1) Baseline all
b_rows.append(run_ols(
    "log_sub_price_ratio ~ log_n_sim + log_n_sim_nl + log_monto_est",
    bid, "All | Baseline"
))
# (2) + post interaction
b_rows.append(run_ols(
    "log_sub_price_ratio ~ log_n_sim + log_n_sim_nl + log_monto_est + post + "
    "log_n_sim:post + log_n_sim_nl:post",
    bid, "All | + Post interaction"
))
# (3) Municipalidades
mu_bid = bid[bid["sector"]=="Municipalidades"]
b_rows.append(run_ols(
    "log_sub_price_ratio ~ log_n_sim + log_n_sim_nl + log_monto_est",
    mu_bid, "Municipalidades | Baseline"
))
# (4) Municipalidades + post
b_rows.append(run_ols(
    "log_sub_price_ratio ~ log_n_sim + log_n_sim_nl + log_monto_est + post + "
    "log_n_sim:post + log_n_sim_nl:post",
    mu_bid, "Municipalidades | + Post"
))
# (5) Obras Públicas
ob_bid = bid[bid["sector"]=="Obras Públicas"]
b_rows.append(run_ols(
    "log_sub_price_ratio ~ log_n_sim + log_n_sim_nl + log_monto_est",
    ob_bid, "Obras Públicas | Baseline"
))
# (6) Obras + post
b_rows.append(run_ols(
    "log_sub_price_ratio ~ log_n_sim + log_n_sim_nl + log_monto_est + post + "
    "log_n_sim:post + log_n_sim_nl:post",
    ob_bid, "Obras Públicas | + Post"
))

t9 = pd.DataFrame(b_rows)
t9.to_csv(TBLS / "t9_bid_level_regressions.csv", index=False)
_bkey = ["sample","n_obs","r2",
         "log_n_sim","log_n_sim_se","log_n_sim_stars",
         "log_n_sim_nl","log_n_sim_nl_se","log_n_sim_nl_stars"]
print(t9[[c for c in _bkey if c in t9.columns]].to_string())


# ─────────────────────────────────────────────────────────────────────────────
# T10: Sector comparison (full table)
# ─────────────────────────────────────────────────────────────────────────────
print("\n[T10] Sector × period comparison (compact)…")
t10_rows = []
for sec in ["Municipalidades", "Obras Públicas"]:
    for pp_label, pp_val in [("Pre-reform", 0), ("Post-reform", 1), ("All", None)]:
        sub_b = bid[bid["sector"]==sec] if pp_val is None else bid[(bid["sector"]==sec) & (bid["post"]==pp_val)]
        t10_rows.append(run_ols(
            "log_sub_price_ratio ~ log_n_sim + log_n_sim_nl + log_monto_est",
            sub_b, f"Bid ratio | {sec} | {pp_label}"
        ))

t10 = pd.DataFrame(t10_rows)
t10.to_csv(TBLS / "t10_munic_vs_obras.csv", index=False)
print(t10[[c for c in _bkey if c in t10.columns]].to_string())


# ─────────────────────────────────────────────────────────────────────────────
# T11: Pre/post breakdown for both outcomes
# ─────────────────────────────────────────────────────────────────────────────
print("\n[T11] Pre / post reform…")
t11_rows = []
for pp_label, pp_val in [("Pre-reform", 0), ("Post-reform", 1)]:
    t11_rows.append(run_ols(
        "log_sub_price_ratio ~ log_n_sim + log_n_sim_nl + log_monto_est",
        bid[bid["post"]==pp_val], f"Bid ratio | All | {pp_label}"
    ))
    t11_rows.append(run_ols(
        "log_sub_price_ratio ~ log_n_sim + log_n_sim_nl + log_monto_est",
        bid[(bid["post"]==pp_val) & (bid["sector"]=="Municipalidades")],
        f"Bid ratio | Municipalidades | {pp_label}"
    ))
    t11_rows.append(run_ols(
        "log_sub_price_ratio ~ log_n_sim + log_n_sim_nl + log_monto_est",
        bid[(bid["post"]==pp_val) & (bid["sector"]=="Obras Públicas")],
        f"Bid ratio | Obras Públicas | {pp_label}"
    ))

t11 = pd.DataFrame(t11_rows)
t11.to_csv(TBLS / "t11_pre_post_regressions.csv", index=False)
print(t11[[c for c in _bkey if c in t11.columns]].to_string())


# ─────────────────────────────────────────────────────────────────────────────
# T12: By firm size
# ─────────────────────────────────────────────────────────────────────────────
print("\n[T12] By firm size…")
t12_rows = []
for sz in ["micro","small","medium","large"]:
    sub = bid[bid["size_group"] == sz]
    if len(sub.dropna(subset=["log_sub_price_ratio","log_n_sim"])) > 200:
        t12_rows.append(run_ols(
            "log_sub_price_ratio ~ log_n_sim + log_n_sim_nl + log_monto_est",
            sub, f"Bid ratio | {sz}"
        ))
        # by sector
        for sec in ["Municipalidades","Obras Públicas"]:
            sub_s = sub[sub["sector"]==sec]
            if len(sub_s.dropna(subset=["log_sub_price_ratio","log_n_sim"])) > 100:
                t12_rows.append(run_ols(
                    "log_sub_price_ratio ~ log_n_sim + log_n_sim_nl + log_monto_est",
                    sub_s, f"Bid ratio | {sz} | {sec}"
                ))

t12 = pd.DataFrame(t12_rows)
t12.to_csv(TBLS / "t12_size_heterogeneity.csv", index=False)
print(t12[[c for c in _bkey if c in t12.columns]].to_string())

print("\nAll regressions complete.")
