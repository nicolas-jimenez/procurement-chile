"""
04_extensions.py  — memory-efficient sequential processing
─────────────────────────────────────────────────────────────────────────────
Run all analyses piece by piece with explicit memory releases.
Call with an argument: python3 04_extensions.py [t13|t14|t15|t16|t17|t18|all]
"""

from __future__ import annotations
import gc, sys, warnings
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import pyfixest as pfx
import statsmodels.formula.api as smf

warnings.filterwarnings("ignore")

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config import DATA_RAW_OTHER, OUTPUT_ROOT  # noqa: E402

OUT  = OUTPUT_ROOT / "simultaneousbids"
TBLS = OUT / "tables"
TBLS.mkdir(parents=True, exist_ok=True)

REFORM_MONTH = "2024-12"

utm_cw = pd.read_csv(DATA_RAW_OTHER / "utm_clp_2022_2025.csv")
utm_cw["ym"] = utm_cw["year"].astype(str) + "-" + utm_cw["month_num"].astype(str).str.zfill(2)

# ── Helpers ───────────────────────────────────────────────────────────────────
def parse_rut(s):
    try:
        return int(str(s).replace(".", "").split("-")[0])
    except Exception:
        return np.nan

def run_ols(formula, data, label=""):
    try:
        res = smf.ols(formula, data=data).fit(cov_type="HC1")
        row = {"sample": label, "estimator": "OLS",
               "n_obs": int(res.nobs), "r2": round(res.rsquared, 4)}
        for k in res.params.index:
            row[k] = round(float(res.params[k]), 6)
            row[k+"_se"] = round(float(res.bse[k]), 6)
            row[k+"_p"]  = round(float(res.pvalues[k]), 4)
            row[k+"_stars"] = (
                "***" if res.pvalues[k]<0.01 else "**" if res.pvalues[k]<0.05 else
                "*"   if res.pvalues[k]<0.10 else "")
        return row
    except Exception as e:
        return {"sample": label, "estimator": "OLS", "error": str(e)}

def run_feols(formula, data, label=""):
    try:
        res = pfx.feols(formula, data=data, vcov="HC1")
        coef = res.coef(); se = res.se(); pval = res.pvalue()
        row = {"sample": label, "estimator": "Firm FE",
               "n_obs": int(res._N),
               "r2_within": round(float(res._r2_within), 4)}
        for k in coef.index:
            row[k] = round(float(coef[k]), 6)
            row[k+"_se"] = round(float(se[k]), 6)
            row[k+"_p"]  = round(float(pval[k]), 4)
            row[k+"_stars"] = (
                "***" if pval[k]<0.01 else "**" if pval[k]<0.05 else
                "*"   if pval[k]<0.10 else "")
        return row
    except Exception as e:
        return {"sample": label, "estimator": "Firm FE", "error": str(e)}

KEY_E = ["sample","estimator","n_obs","r2","r2_within",
         "log_n_sim_lag1","log_n_sim_lag1_se","log_n_sim_lag1_stars",
         "log_n_sim_nl_lag1","log_n_sim_nl_lag1_se","log_n_sim_nl_lag1_stars"]
KEY_B = ["sample","estimator","n_obs","r2","r2_within",
         "log_n_sim","log_n_sim_se","log_n_sim_stars",
         "log_n_sim_nl","log_n_sim_nl_se","log_n_sim_nl_stars"]

TASK = sys.argv[1] if len(sys.argv)>1 else "all"

# ═══════════════════════════════════════════════════════════════════════════════
def run_t13():
    """Local vs. non-local × pre/post × firm size."""
    print("="*60 + "\n[T13] Local vs. non-local × pre/post × firm size\n" + "="*60)
    fm = pd.read_parquet(OUT / "firm_month_panel.parquet")
    fm["pre_post"] = np.where(fm["ym"] > REFORM_MONTH, "Post-reform", "Pre-reform")

    rows = []
    for pp in ["Pre-reform","Post-reform","All"]:
        fp = fm if pp=="All" else fm[fm["pre_post"]==pp]
        for sz in ["micro","small","medium","large","All"]:
            sub = fp if sz=="All" else fp[fp["size_group"]==sz]
            if len(sub)==0:
                continue
            rows.append({
                "period":              pp,
                "size_group":          sz,
                "n_firm_months":       len(sub),
                "n_firms":             sub["rut_bidder"].nunique(),
                "mean_n_sim":          round(sub["n_sim"].mean(), 2),
                "median_n_sim":        round(sub["n_sim"].median(), 1),
                "mean_n_local":        round(sub["n_sim_local"].mean(), 2),
                "median_n_local":      round(sub["n_sim_local"].median(), 1),
                "mean_n_nonlocal":     round(sub["n_sim_nonlocal"].mean(), 2),
                "median_n_nonlocal":   round(sub["n_sim_nonlocal"].median(), 1),
                "mean_share_nonlocal": round(sub["share_nonlocal"].mean(), 3),
                "pct_any_nonlocal":    round((sub["n_sim_nonlocal"]>0).mean()*100, 1),
                "pct_purely_local":    round((sub["n_sim_nonlocal"]==0).mean()*100, 1),
                "mean_n_regions":      round(sub["n_regions_bid"].mean(), 2),
                "mean_avg_dist_km":    round(sub["avg_dist_km"].mean(), 1),
            })

    t13 = pd.DataFrame(rows)
    t13.to_csv(TBLS / "t13_local_nonlocal_prepost_size.csv", index=False)
    print(t13[["period","size_group","n_firm_months","mean_n_local",
               "mean_n_nonlocal","mean_share_nonlocal","pct_any_nonlocal","mean_n_regions"]].to_string())
    del fm; gc.collect()


# ═══════════════════════════════════════════════════════════════════════════════
def _load_entry_clean():
    ECOLS = ["tender_id","rut_bidder","ym","sector","post","monto_estimado",
             "n_sim_lag1","n_sim_nl_lag1","first_bid_in_region","size_group"]
    ent = pd.read_parquet(OUT / "bid_level_simult.parquet", columns=ECOLS)
    ent["log_monto_est"]     = np.log(ent["monto_estimado"].replace(0, np.nan))
    ent["log_n_sim_lag1"]    = np.log1p(ent["n_sim_lag1"])
    ent["log_n_sim_nl_lag1"] = np.log1p(ent["n_sim_nl_lag1"])
    ent["rut_str"]           = ent["rut_bidder"].astype(str)
    ent["pre_post"]          = np.where(ent["post"]==1, "Post-reform", "Pre-reform")
    ent = ent.dropna(subset=["log_n_sim_lag1","log_n_sim_nl_lag1",
                              "log_monto_est","first_bid_in_region"])
    print(f"  Entry clean: {len(ent):,} rows, {ent['rut_bidder'].nunique():,} firms")
    return ent

ENTRY_OLS = "first_bid_in_region ~ log_n_sim_lag1 + log_n_sim_nl_lag1 + log_monto_est"
ENTRY_FE  = "first_bid_in_region ~ log_n_sim_lag1 + log_n_sim_nl_lag1 + log_monto_est | rut_str"

def run_t14():
    print("="*60 + "\n[T14] Entry regressions with firm FEs\n" + "="*60)
    ent = _load_entry_clean()
    rows = []
    for label, sec, pp_val in [
        ("All | All",           "All", None),
        ("Municipalidades",     "Municipalidades", None),
        ("Obras Públicas",      "Obras Públicas", None),
        ("All | Pre-reform",    "All", 0),
        ("All | Post-reform",   "All", 1),
    ]:
        m = pd.Series(True, index=ent.index)
        if sec!="All": m &= ent["sector"]==sec
        if pp_val is not None: m &= ent["post"]==pp_val
        sub = ent[m]
        print(f"  OLS  {label} (n={len(sub):,})")
        rows.append(run_ols(ENTRY_OLS, sub, label+" | OLS"))
        print(f"  FE   {label}")
        rows.append(run_feols(ENTRY_FE, sub.copy(), label+" | Firm FE"))
        del sub; gc.collect()

    del ent; gc.collect()
    t14 = pd.DataFrame(rows)
    t14.to_csv(TBLS / "t14_entry_fe.csv", index=False)
    print(t14[[c for c in KEY_E if c in t14.columns]].to_string())


# ═══════════════════════════════════════════════════════════════════════════════
def _load_bid_clean():
    fm2 = pd.read_parquet(OUT / "firm_month_panel.parquet",
                          columns=["rut_bidder","ym","n_sim","n_sim_nonlocal","size_group"])
    fm2["rut_bidder"] = fm2["rut_bidder"].astype("Int64")

    BID_COLS = ["bidder_id","year_month","sector","dataset","log_sub_price_ratio",
                "post","monto_estimado","monto_utm"]
    chunks = []
    pf_bid = pq.ParquetFile(OUTPUT_ROOT / "bids" / "bid_analysis_sample.parquet")
    for batch in pf_bid.iter_batches(batch_size=300_000, columns=BID_COLS):
        d = batch.to_pandas()
        d = d[(d["dataset"]=="licitaciones") &
              (d["sector"].isin(["Municipalidades","Obras Públicas"]))]
        if len(d)>0: chunks.append(d)
    bid = pd.concat(chunks, ignore_index=True)
    del chunks; gc.collect()

    bid["rut_bidder"] = bid["bidder_id"].apply(parse_rut).astype("Int64")
    bid["ym_str"]     = bid["year_month"].astype(str)
    bid = bid.merge(fm2.rename(columns={"ym":"ym_str"}), on=["rut_bidder","ym_str"], how="left")
    del fm2; gc.collect()

    bid["log_n_sim"]     = np.log1p(bid["n_sim"])
    bid["log_n_sim_nl"]  = np.log1p(bid["n_sim_nonlocal"])
    bid["log_monto_est"] = np.log(bid["monto_estimado"].replace(0, np.nan))
    bid["rut_str"]       = bid["rut_bidder"].astype(str)
    bid["pre_post"]      = np.where(bid["post"]==1, "Post-reform", "Pre-reform")
    bid = bid[bid["log_sub_price_ratio"].between(-3,3) | bid["log_sub_price_ratio"].isna()]
    bid = bid.dropna(subset=["log_n_sim","log_n_sim_nl","log_monto_est","log_sub_price_ratio"])
    print(f"  Bid clean: {len(bid):,} rows, n_sim matched: {bid['n_sim'].notna().sum():,}")
    return bid

BID_OLS = "log_sub_price_ratio ~ log_n_sim + log_n_sim_nl + log_monto_est"
BID_FE  = "log_sub_price_ratio ~ log_n_sim + log_n_sim_nl + log_monto_est | rut_str"

def run_t15():
    print("="*60 + "\n[T15] Bid-level regressions with firm FEs\n" + "="*60)
    bid = _load_bid_clean()
    rows = []
    for label, sec, pp_val in [
        ("All | All",                    "All", None),
        ("Municipalidades",              "Municipalidades", None),
        ("Obras Públicas",               "Obras Públicas", None),
        ("All | Pre-reform",             "All", 0),
        ("All | Post-reform",            "All", 1),
        ("Municipalidades | Pre-reform", "Municipalidades", 0),
        ("Municipalidades | Post-reform","Municipalidades", 1),
        ("Obras Públicas | Pre-reform",  "Obras Públicas",  0),
        ("Obras Públicas | Post-reform", "Obras Públicas",  1),
    ]:
        m = pd.Series(True, index=bid.index)
        if sec!="All": m &= bid["sector"]==sec
        if pp_val is not None: m &= bid["post"]==pp_val
        sub = bid[m]
        if len(sub)<50: continue
        print(f"  OLS  {label} (n={len(sub):,})")
        rows.append(run_ols(BID_OLS, sub, label+" | OLS"))
        if len(sub)>=200:
            print(f"  FE   {label}")
            rows.append(run_feols(BID_FE, sub.copy(), label+" | Firm FE"))
        del sub; gc.collect()

    del bid; gc.collect()
    t15 = pd.DataFrame(rows)
    t15.to_csv(TBLS / "t15_bid_fe.csv", index=False)
    print(t15[[c for c in KEY_B if c in t15.columns]].to_string())


# ═══════════════════════════════════════════════════════════════════════════════
def run_t16_t17():
    """UTM descriptives + entry regressions by UTM × sector × period."""
    print("="*60 + "\n[T16-T17] Entry analysis by UTM\n" + "="*60)
    ent = _load_entry_clean()
    ent = ent.merge(utm_cw[["ym","utm_clp"]], on="ym", how="left")
    ent["monto_utm"]    = ent["monto_estimado"] / ent["utm_clp"].replace(0, np.nan)
    ent["below_500"]    = (ent["monto_utm"] < 500).astype("Int8")
    ent["utm_label"]    = np.where(ent["below_500"]==1, "<500 UTM", "≥500 UTM")
    ent["pre_post"]     = np.where(ent["post"]==1, "Post-reform", "Pre-reform")

    # Drop rows where monto_utm couldn't be computed
    ent = ent.dropna(subset=["monto_utm"])
    print(f"  With UTM: {len(ent):,}")
    print(f"  % <500 UTM: {(ent['below_500']==1).mean()*100:.1f}%")

    # T16 descriptives
    t16_rows = []
    for utm_lab in ["<500 UTM","≥500 UTM"]:
        mu = ent["utm_label"]==utm_lab
        for sec in ["Municipalidades","Obras Públicas","All"]:
            ms = mu if sec=="All" else mu & (ent["sector"]==sec)
            for pp in ["Pre-reform","Post-reform","All"]:
                mp = ms if pp=="All" else ms & (ent["pre_post"]==pp)
                sub = ent[mp]
                if len(sub)<10: continue
                t16_rows.append({
                    "utm_band": utm_lab, "sector": sec, "period": pp,
                    "n_bids": len(sub),
                    "n_tenders": sub["tender_id"].nunique(),
                    "n_firms": sub["rut_bidder"].nunique(),
                    "mean_monto_utm": round(sub["monto_utm"].mean(), 1),
                    "mean_n_sim_lag1": round(sub["n_sim_lag1"].mean(), 2),
                    "pct_first_region": round(sub["first_bid_in_region"].mean()*100, 2),
                })
    t16 = pd.DataFrame(t16_rows)
    t16.to_csv(TBLS / "t16_utm_descriptives.csv", index=False)
    print("[T16]\n" + t16[t16["period"]=="All"].to_string())

    # T17 entry regressions
    e17_rows = []
    for utm_lab in ["<500 UTM","≥500 UTM"]:
        mu = ent["utm_label"]==utm_lab
        for sec in ["Municipalidades","Obras Públicas","All"]:
            ms = mu if sec=="All" else mu & (ent["sector"]==sec)
            for pp_label, pp_val in [("Pre-reform",0),("Post-reform",1),("All",None)]:
                mp = ms if pp_val is None else ms & (ent["post"]==pp_val)
                sub = ent[mp]
                label = f"{utm_lab} | {sec} | {pp_label}"
                if len(sub)<100: continue
                print(f"  OLS  {label} (n={len(sub):,})")
                e17_rows.append(run_ols(ENTRY_OLS, sub, label+" | OLS"))
                if len(sub)>=2000:
                    print(f"  FE   {label}")
                    e17_rows.append(run_feols(ENTRY_FE, sub.copy(), label+" | Firm FE"))
                del sub; gc.collect()

    del ent; gc.collect()
    t17 = pd.DataFrame(e17_rows)
    t17.to_csv(TBLS / "t17_entry_utm_split.csv", index=False)
    print("[T17]\n" + t17[[c for c in KEY_E if c in t17.columns]].to_string())


# ═══════════════════════════════════════════════════════════════════════════════
def run_t18():
    """Bid regressions within the 0-200 UTM bid sample, split at 100 UTM."""
    print("="*60 + "\n[T18] Bid regressions by UTM band (0-200 UTM sample)\n" + "="*60)
    print("  NOTE: bid_analysis_sample covers 1-200 UTM only (DiD bunching sample).")
    print("        Splitting at 100 UTM: '<100 UTM' vs '100-200 UTM'.")
    print("        ≥500 UTM bid-level data not available in current samples.\n")

    bid = _load_bid_clean()
    bid["utm_label"] = pd.cut(bid["monto_utm"], bins=[-np.inf,100,np.inf],
                               labels=["<100 UTM","100-200 UTM"])

    b18_rows = []
    for utm_lab in ["<100 UTM","100-200 UTM"]:
        mu = bid["utm_label"]==utm_lab
        for sec in ["Municipalidades","Obras Públicas","All"]:
            ms = mu if sec=="All" else mu & (bid["sector"]==sec)
            for pp_label, pp_val in [("Pre-reform",0),("Post-reform",1),("All",None)]:
                mp = ms if pp_val is None else ms & (bid["post"]==pp_val)
                sub = bid[mp]
                label = f"{utm_lab} | {sec} | {pp_label}"
                if len(sub)<50: continue
                print(f"  OLS  {label} (n={len(sub):,})")
                b18_rows.append(run_ols(BID_OLS, sub, label+" | OLS"))
                if len(sub)>=500:
                    print(f"  FE   {label}")
                    b18_rows.append(run_feols(BID_FE, sub.copy(), label+" | Firm FE"))
                del sub; gc.collect()

    del bid; gc.collect()
    t18 = pd.DataFrame(b18_rows)
    t18.to_csv(TBLS / "t18_bid_utm_split.csv", index=False)
    print(t18[[c for c in KEY_B if c in t18.columns]].to_string())


# ── Dispatch ──────────────────────────────────────────────────────────────────
dispatch = {
    "t13": run_t13, "t14": run_t14, "t15": run_t15,
    "t16": run_t16_t17, "t17": run_t16_t17, "t18": run_t18,
}

if TASK == "all":
    for fn in [run_t13, run_t14, run_t15, run_t16_t17, run_t18]:
        fn(); gc.collect()
elif TASK in dispatch:
    dispatch[TASK]()
else:
    print(f"Unknown task: {TASK}. Use: t13 t14 t15 t16 t17 t18 all")

print("\n✓ Done.")
