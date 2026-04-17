"""
04_diagnose_did.py
─────────────────────────────────────────────────────────────────────────────
DiD pre-trend tests and robustness diagnostics for the Compra Ágil reform.
All tests use control = 100–200 UTM (control_high + treated).

Tests performed
───────────────
1. Wald pre-trend test
   H₀: β_{-8} = β_{-7} = … = β_{-2} = 0  (no pre-trends in event study)
   W = β̂_pre' V̂_pre⁻¹ β̂_pre ~ χ²(q),  q = pre_periods − 1 = 7
   Also reported as F = W/q ~ F(q, G−1) (cluster finite-sample correction).

2. Time placebo test
   Run DiD on pre-reform observations only (k ∈ [−8, −1]).
   Placebo "reform" at k = −4 (4 months before actual reform).
   H₀: β_placebo = 0  (no differential trend in pre-period)

3. Pre-reform balance test
   Compare entity-level pre-reform means between treated and control_high
   via OLS: ȳ_i = α + β·Treated_i + ε_i  (SE clustered by entity).
   H₀: β = 0  (no level difference before reform)

4. Event studies (control 100–200 UTM spec)
   Saves to output/did/figures/diag_event_study_{outcome}.png
   and   output/did/tables/diag_event_study_{outcome}.csv

Outputs
───────
  output/did/tables/diag_pretrend_wald.{csv,tex}
  output/did/tables/diag_placebo.{csv,tex}
  output/did/tables/diag_balance.{csv,tex}
  output/did/tables/diag_event_study_{outcome}.csv
  output/did/figures/diag_event_study_{outcome}.png
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy.stats import chi2 as chi2_dist
from scipy.stats import f as f_dist
from scipy.stats import norm

matplotlib.use("Agg")

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from did_utils import (
    OUT_FIGURES,
    OUT_SAMPLES,
    OUT_TABLES,
    REFORM_PERIOD,
    _cluster_cov,
    _cluster_se,
    _twoway_demean,
    plot_event_study,
    run_twfe_event_study,
)

# ── Config ────────────────────────────────────────────────────────────────────
ENTITY_COL  = "rut_unidad"
TIME_COL    = "year_month"
TREAT_COL   = "treated"
CLUSTER_COL = "rut_unidad"

PRE_PERIODS  = 8
POST_PERIODS = 6
PLACEBO_K    = -4   # fake reform: 4 months before actual reform
BAND_CH      = ["control_high", "treated"]

# Option B: drop k=0 (Dec 2024) as a partial-treatment transition month.
# Override with --drop-k0 on the command line.
DROP_K0 = False

# Key outcomes to diagnose (one representative per panel)
DIAG_OUTCOMES_TENDER = [
    ("n_bidders",            "N bidders"),
    ("single_bidder",        "Pr(single-bidder)"),
    ("any_sme_sii",          "Any SME bidder"),
    ("sme_share_sii",        r"\% SME bidders"),
    ("share_large_bidders",  r"\% large bidders"),
    ("winner_is_sme_sii",    "Pr(winner: SME)"),
    ("winner_is_large",      "Pr(winner: large)"),
    ("log_win_price_ratio",  "log(win bid/ref)"),
    ("log_min_price_ratio",  "log(min bid/ref)"),
]
DIAG_OUTCOMES_BID = [
    ("log_sub_price_ratio",  "log(bid/ref)"),
]

# Outcomes for event-study plots (col, DataFrame key, label, color)
ES_OUTCOMES = [
    ("n_bidders",           "tender", "N bidders",         "#1B9E77"),
    ("sme_share_sii",       "tender", r"% SME bidders",    "#D95F02"),
    ("winner_is_sme_sii",   "tender", "Pr(winner: SME)",   "#D95F02"),
    ("log_win_price_ratio", "tender", "log(win bid/ref)",  "#7570B3"),
    ("single_bidder",       "tender", "Pr(single-bidder)", "#66A61E"),
    ("log_sub_price_ratio", "bid",    "log(bid/ref)",      "#7570B3"),
]


# ── Formatting ────────────────────────────────────────────────────────────────
def _stars(pval: float) -> str:
    if pd.isna(pval):
        return ""
    if pval < 0.01:
        return "^{***}"
    if pval < 0.05:
        return "^{**}"
    if pval < 0.10:
        return "^{*}"
    return ""


def _fmt(val, decimals: int = 4) -> str:
    return f"{val:.{decimals}f}" if pd.notna(val) else "---"


def _fmt_int(val) -> str:
    return f"{int(val):,}" if pd.notna(val) else "---"


# ── 1. Wald pre-trend test ────────────────────────────────────────────────────
def run_pretrend_wald(
    df: pd.DataFrame,
    outcome_col: str,
    entity_col: str,
    time_col: str,
    treat_col: str,
    cluster_col: str,
    pre_periods: int = PRE_PERIODS,
    post_periods: int = POST_PERIODS,
    drop_k0: bool = False,
    label: str = "",
    min_obs: int = 200,
) -> dict | None:
    """
    Run event-study TWFE and test H₀: all pre-period β = 0 (Wald test).

    k_range = [-pre_periods, …, +post_periods] \ {-1}
    Pre-period dummies: k ∈ {-pre_periods, …, -2}  →  n_pre = pre_periods − 1

    Statistic: W = β̂_pre' V̂_pre⁻¹ β̂_pre ~ χ²(n_pre) under H₀.
    Also reported as F = W / n_pre ~ F(n_pre, G−1) (finite-sample approx.).
    """
    df = df.copy()
    df["_ym_int"] = df[time_col].apply(
        lambda p: p.ordinal if hasattr(p, "ordinal") else int(p)
    )
    df["k"] = df["_ym_int"] - REFORM_PERIOD.ordinal

    k_range         = list(range(-pre_periods, post_periods + 1))
    _omit_ks = {-1, 0} if drop_k0 else {-1}
    k_range_no_omit = [k for k in k_range if k not in _omit_ks]

    for k in k_range_no_omit:
        df[f"int_k{k:+d}"] = (df[treat_col] * (df["k"] == k)).astype("float64")

    int_cols = [f"int_k{k:+d}" for k in k_range_no_omit]
    needed = [outcome_col, entity_col, time_col, "k", treat_col] + int_cols
    if cluster_col not in needed:
        needed.append(cluster_col)

    sub = df[needed].replace([np.inf, -np.inf], np.nan).dropna().copy()
    sub = sub[sub["k"].between(-pre_periods, post_periods)].copy()
    if drop_k0:
        sub = sub[sub["k"] != 0].copy()
    if len(sub) < min_obs:
        return None

    entity_sizes = sub.groupby(entity_col)[outcome_col].transform("count")
    sub = sub[entity_sizes > 1].copy()
    if len(sub) < min_obs:
        return None

    dm_cols = list(dict.fromkeys([outcome_col] + int_cols))
    sub_dm  = _twoway_demean(sub, entity_col, time_col, dm_cols)

    X   = sub_dm[int_cols].to_numpy(dtype=float)
    y   = sub_dm[outcome_col].to_numpy(dtype=float)
    clu = sub[cluster_col].to_numpy()

    valid = np.isfinite(X).all(axis=1) & np.isfinite(y)
    X, y, clu = X[valid], y[valid], clu[valid]
    if len(y) < min_obs:
        return None

    coefs, _, _, _ = np.linalg.lstsq(X, y, rcond=None)
    resid = y - X @ coefs
    V     = _cluster_cov(X, resid, clu)

    # Pre-period block: first (pre_periods − 1) coefficients
    n_pre    = pre_periods - 1
    beta_pre = coefs[:n_pre]
    V_pre    = V[:n_pre, :n_pre]

    try:
        V_pre_inv = np.linalg.inv(V_pre)
    except np.linalg.LinAlgError:
        V_pre_inv = np.linalg.pinv(V_pre)

    wald       = float(beta_pre @ V_pre_inv @ beta_pre)
    f_stat     = wald / n_pre
    n_clusters = int(len(np.unique(clu)))
    chi2_pval  = float(chi2_dist.sf(wald, df=n_pre))
    f_pval     = float(f_dist.sf(f_stat, dfn=n_pre, dfd=max(n_clusters - 1, 1)))

    ses_pre = np.sqrt(np.clip(np.diag(V_pre), 0, None))

    return {
        "outcome":    outcome_col,
        "label":      label,
        "wald":       wald,
        "f_stat":     f_stat,
        "chi2_pval":  chi2_pval,
        "f_pval":     f_pval,
        "dof_pre":    n_pre,
        "n_clusters": n_clusters,
        "n_obs":      int(len(y)),
        "n_entities": int(len(np.unique(clu))),
        "pre_coefs":  beta_pre.tolist(),
        "pre_ses":    ses_pre.tolist(),
    }


# ── 2. Time placebo test ──────────────────────────────────────────────────────
def run_time_placebo(
    df: pd.DataFrame,
    outcome_col: str,
    entity_col: str,
    time_col: str,
    treat_col: str,
    cluster_col: str,
    placebo_k: int = PLACEBO_K,
    pre_periods: int = PRE_PERIODS,
    label: str = "",
    min_obs: int = 200,
) -> dict | None:
    """
    Test for pre-trends using a time placebo DiD.

    Restricts to pre-reform observations (k ∈ [-pre_periods, -1]).
    Fake "post" = k ≥ placebo_k,  fake "pre" = k < placebo_k.
    Runs TWFE DiD on this window.

    H₀: β_placebo = 0  (no differential pre-trend)
    """
    df = df.copy()
    df["_ym_int"] = df[time_col].apply(
        lambda p: p.ordinal if hasattr(p, "ordinal") else int(p)
    )
    df["k"] = df["_ym_int"] - REFORM_PERIOD.ordinal

    sub = df[df["k"].between(-pre_periods, -1)].copy()
    sub["did_placebo"] = (
        (sub[treat_col] == 1) & (sub["k"] >= placebo_k)
    ).astype("float64")

    needed = [outcome_col, entity_col, time_col, treat_col, "did_placebo"]
    if cluster_col not in needed:
        needed.append(cluster_col)

    sub = sub[needed].replace([np.inf, -np.inf], np.nan).dropna().copy()
    if len(sub) < min_obs:
        return None

    entity_sizes = sub.groupby(entity_col)[outcome_col].transform("count")
    sub = sub[entity_sizes > 1].copy()
    if len(sub) < min_obs:
        return None

    sub_dm = _twoway_demean(sub, entity_col, time_col, [outcome_col, "did_placebo"])
    X   = sub_dm[["did_placebo"]].to_numpy(dtype=float)
    y   = sub_dm[outcome_col].to_numpy(dtype=float)
    clu = sub[cluster_col].to_numpy()

    valid = np.isfinite(X).all(axis=1) & np.isfinite(y)
    X, y, clu = X[valid], y[valid], clu[valid]
    if len(y) < min_obs:
        return None

    coefs, _, _, _ = np.linalg.lstsq(X, y, rcond=None)
    resid = y - X @ coefs
    se    = _cluster_se(X, resid, clu)

    beta  = float(coefs[0])
    se_v  = float(se[0]) if se.size > 0 and se[0] > 0 else np.nan
    tstat = beta / se_v if np.isfinite(se_v) else np.nan
    pval  = float(2 * norm.sf(abs(tstat))) if np.isfinite(tstat) else np.nan

    return {
        "outcome":    outcome_col,
        "label":      label,
        "placebo_k":  placebo_k,
        "coef":       beta,
        "se":         se_v,
        "tstat":      tstat,
        "pval":       pval,
        "n_obs":      int(len(y)),
        "n_entities": int(len(np.unique(clu))),
    }


# ── 3. Pre-reform balance test ────────────────────────────────────────────────
def run_balance_test(
    df: pd.DataFrame,
    outcome_col: str,
    entity_col: str,
    time_col: str,
    treat_col: str,
    cluster_col: str,
    pre_periods: int = PRE_PERIODS,
    label: str = "",
    min_obs: int = 10,
) -> dict | None:
    """
    Compare entity-level pre-reform means between treated and control_high.

    OLS on entity-level averages: ȳ_i = α + β·Treated_i + ε_i
    SE clustered by entity.

    H₀: β = 0  (no level difference before reform)
    """
    df = df.copy()
    df["_ym_int"] = df[time_col].apply(
        lambda p: p.ordinal if hasattr(p, "ordinal") else int(p)
    )
    df["k"] = df["_ym_int"] - REFORM_PERIOD.ordinal

    pre = df[df["k"].between(-pre_periods, -1)].copy()
    if pre[outcome_col].notna().sum() < min_obs:
        return None

    entity_means = (
        pre.groupby(entity_col)
        .agg(
            y_mean    =(outcome_col, "mean"),
            treated_v =(treat_col,   "first"),
        )
        .reset_index()
        .dropna(subset=["y_mean"])
    )
    if len(entity_means) < min_obs:
        return None

    X   = np.column_stack([
        np.ones(len(entity_means)),
        entity_means["treated_v"].to_numpy(dtype=float),
    ])
    y   = entity_means["y_mean"].to_numpy(dtype=float)
    clu = entity_means[entity_col].to_numpy()

    valid = np.isfinite(X).all(axis=1) & np.isfinite(y)
    X, y, clu = X[valid], y[valid], clu[valid]
    if len(y) < min_obs:
        return None

    coefs, _, _, _ = np.linalg.lstsq(X, y, rcond=None)
    resid = y - X @ coefs
    se    = _cluster_se(X, resid, clu)

    ctrl_mean  = float(coefs[0])
    treat_diff = float(coefs[1])
    se_v  = float(se[1]) if se.size > 1 and se[1] > 0 else np.nan
    tstat = treat_diff / se_v if np.isfinite(se_v) else np.nan
    pval  = float(2 * norm.sf(abs(tstat))) if np.isfinite(tstat) else np.nan

    return {
        "outcome":    outcome_col,
        "label":      label,
        "ctrl_mean":  ctrl_mean,
        "treat_diff": treat_diff,
        "se":         se_v,
        "tstat":      tstat,
        "pval":       pval,
        "n_entities": int(len(y)),
    }


# ── LaTeX table builders ──────────────────────────────────────────────────────
def build_wald_table(results: list[dict]) -> str:
    lines = [r"{\footnotesize"]
    lines.append(r"\begin{tabular}{l r r r r r}")
    lines.append(r"\toprule")
    lines.append(
        r"Outcome & $F$-stat & $\chi^2$ & $p$-val ($\chi^2$) & $p$-val ($F$) & $N$ \\"
    )
    lines.append(r"\midrule")
    for r in results:
        sig = _stars(r["f_pval"])
        lines.append(
            rf"  {r['label']}"
            rf" & {_fmt(r['f_stat'], 2)}"
            rf" & {_fmt(r['wald'], 2)}"
            rf" & {_fmt(r['chi2_pval'], 3)}"
            rf" & ${_fmt(r['f_pval'], 3)}{sig}$"
            rf" & {_fmt_int(r['n_obs'])} \\"
        )
    lines.append(r"\bottomrule")
    lines.append(
        r"\multicolumn{6}{l}{\scriptsize"
        r" $H_0$: $\beta_{-8} = \cdots = \beta_{-2} = 0$ (7 pre-period dummies)."
        r" $F \sim F(7,\,G{-}1)$; $\chi^2 \sim \chi^2(7)$.} \\"
    )
    lines.append(
        r"\multicolumn{6}{l}{\scriptsize"
        r" TWFE, entity + year-month FE, SE clustered by procuring entity."
        r" Control: 100--200 UTM.} \\"
    )
    lines.append(r"\end{tabular}")
    lines.append(r"}")
    lines.append("")
    return "\n".join(lines)


def build_placebo_table(results: list[dict]) -> str:
    lines = [r"{\footnotesize"]
    lines.append(r"\begin{tabular}{l r r r}")
    lines.append(r"\toprule")
    lines.append(r"Outcome & $\hat\beta_{\text{placebo}}$ & SE & $p$-value \\")
    lines.append(r"\midrule")
    for r in results:
        sig  = _stars(r["pval"])
        coef = _fmt(r["coef"])
        se   = _fmt(r["se"])
        pval = _fmt(r["pval"], 3)
        lines.append(
            rf"  {r['label']} & ${coef}{sig}$ & $({se})$ & {pval} \\"
        )
    lines.append(r"\bottomrule")
    lines.append(
        r"\multicolumn{4}{l}{\scriptsize"
        r" TWFE DiD run on pre-reform data only ($k \in [-8,\,-1]$)."
        r" Placebo reform at $k = -4$.} \\"
    )
    lines.append(
        r"\multicolumn{4}{l}{\scriptsize"
        r" SE clustered by procuring entity. Control: 100--200 UTM.} \\"
    )
    lines.append(r"\end{tabular}")
    lines.append(r"}")
    lines.append("")
    return "\n".join(lines)


def build_balance_table(results: list[dict]) -> str:
    lines = [r"{\footnotesize"]
    lines.append(r"\begin{tabular}{l r r r r}")
    lines.append(r"\toprule")
    lines.append(
        r"Outcome & Control mean & Treated $-$ Control & SE & $p$-value \\"
    )
    lines.append(r"\midrule")
    for r in results:
        sig = _stars(r["pval"])
        lines.append(
            rf"  {r['label']}"
            rf" & {_fmt(r['ctrl_mean'], 3)}"
            rf" & ${_fmt(r['treat_diff'])}{sig}$"
            rf" & $({_fmt(r['se'])})$"
            rf" & {_fmt(r['pval'], 3)} \\"
        )
    lines.append(r"\bottomrule")
    lines.append(
        r"\multicolumn{5}{l}{\scriptsize"
        r" Entity-level pre-reform means ($k \in [-8,\,-1]$) regressed on Treated.} \\"
    )
    lines.append(
        r"\multicolumn{5}{l}{\scriptsize"
        r" SE clustered by procuring entity. Control: 100--200 UTM.} \\"
    )
    lines.append(r"\end{tabular}")
    lines.append(r"}")
    lines.append("")
    return "\n".join(lines)


# ── Main ──────────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(
        description="DiD pre-trend tests and diagnostics for Compra Ágil."
    )
    parser.add_argument(
        "--drop-k0",
        action="store_true",
        default=DROP_K0,
        help="Option B: exclude k=0 (Dec 2024) as a partial-treatment transition month. "
             "Output files get '_optB' suffix.",
    )
    parser.add_argument(
        "--sample",
        choices=["all", "municipalidades", "obras"],
        default="all",
        help="Sample restriction on buyer sector. 'municipalidades' keeps sector "
             "containing 'municipal'; 'obras' keeps sector containing 'obras'. "
             "Output files get '_munic'/'_obras' suffix.",
    )
    args = parser.parse_args()
    drop_k0 = args.drop_k0
    _SAMPLE_SUFFIX = {"all": "", "municipalidades": "_munic", "obras": "_obras"}
    fsuffix = _SAMPLE_SUFFIX[args.sample] + ("_optB" if drop_k0 else "")

    print("=" * 70)
    print("04_diagnose_did.py — DiD pre-trend tests and diagnostics")
    if args.sample != "all":
        print(f"  [Sample: {args.sample}]")
    if drop_k0:
        print("  [Option B] k=0 (Dec 2024) excluded as transition month")
    print("=" * 70)

    # ── Load data ───────────────────────────────────────────────────────────
    tender_path = OUT_SAMPLES / "did_tender_sample.parquet"
    bid_path    = OUT_SAMPLES / "did_bid_sample.parquet"
    if not tender_path.exists() or not bid_path.exists():
        print("  [ERROR] Sample files not found. Run 01_build_did_sample.py first.")
        return

    print("\n  Loading samples …")
    df_tender = pd.read_parquet(tender_path)
    df_bid    = pd.read_parquet(bid_path)

    for df_ in [df_tender, df_bid]:
        df_["year_month"] = df_["year_month"].apply(
            lambda x: pd.Period(x, freq="M") if not isinstance(x, pd.Period) else x
        )
        for col in df_.select_dtypes(include=["Int64", "Int8"]).columns:
            df_[col] = df_[col].astype("float64")

    # ── Sample restriction by buyer sector ─────────────────────────────────
    if args.sample != "all":
        if "sector" not in df_tender.columns or "sector" not in df_bid.columns:
            print("  [ERROR] 'sector' column not found in parquet. "
                  "Re-run 01_build_did_sample.py first.")
            return
        kw = "municipal" if args.sample == "municipalidades" else "obras"
        df_tender = df_tender[
            df_tender["sector"].astype(str).str.lower().str.contains(kw, na=False)
        ].copy()
        df_bid = df_bid[
            df_bid["sector"].astype(str).str.lower().str.contains(kw, na=False)
        ].copy()
        print(f"  [Sample: {args.sample}] "
              f"Tender: {len(df_tender):,} rows, Bid: {len(df_bid):,} rows")

    # Filter to ch spec only
    df_t = df_tender[df_tender["band"].isin(BAND_CH)].copy()
    df_b = df_bid[df_bid["band"].isin(BAND_CH)].copy()
    print(f"  Tender sample (ch): {len(df_t):,} rows | Bid sample (ch): {len(df_b):,} rows")

    # ── 1. Wald pre-trend tests ─────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("1. Wald pre-trend tests  H₀: β_pre = 0")
    print("=" * 70)
    wald_results = []
    for df_, outcomes in [(df_t, DIAG_OUTCOMES_TENDER), (df_b, DIAG_OUTCOMES_BID)]:
        for outcome_col, label in outcomes:
            if outcome_col not in df_.columns:
                continue
            r = run_pretrend_wald(
                df_, outcome_col, ENTITY_COL, TIME_COL, TREAT_COL, CLUSTER_COL,
                drop_k0=drop_k0,
                label=label,
            )
            if r is None:
                print(f"  [{label}] skipped (too few obs).")
                continue
            sig = "***" if r["f_pval"] < 0.01 else ("**" if r["f_pval"] < 0.05 else
                  ("*" if r["f_pval"] < 0.10 else ""))
            print(
                f"  {label:<35} F={r['f_stat']:6.2f}  χ²={r['wald']:6.2f}"
                f"  p(F)={r['f_pval']:.3f}{sig:3}  n={r['n_obs']:,}"
            )
            wald_results.append(r)

    if wald_results:
        wald_df = pd.DataFrame([
            {k: v for k, v in r.items() if not isinstance(v, list)}
            for r in wald_results
        ])
        wald_df.to_csv(OUT_TABLES / f"diag_pretrend_wald{fsuffix}.csv", index=False)
        (OUT_TABLES / f"diag_pretrend_wald{fsuffix}.tex").write_text(build_wald_table(wald_results))
        print(f"\n  Saved: diag_pretrend_wald{fsuffix}.{{csv,tex}}")

    # ── 2. Time placebo test ────────────────────────────────────────────────
    print("\n" + "=" * 70)
    print(f"2. Time placebo test  (fake reform at k = {PLACEBO_K})")
    print("=" * 70)
    placebo_results = []
    for df_, outcomes in [(df_t, DIAG_OUTCOMES_TENDER), (df_b, DIAG_OUTCOMES_BID)]:
        for outcome_col, label in outcomes:
            if outcome_col not in df_.columns:
                continue
            r = run_time_placebo(
                df_, outcome_col, ENTITY_COL, TIME_COL, TREAT_COL, CLUSTER_COL,
                label=label,
            )
            if r is None:
                print(f"  [{label}] skipped (too few obs).")
                continue
            sig = "***" if r["pval"] < 0.01 else ("**" if r["pval"] < 0.05 else
                  ("*" if r["pval"] < 0.10 else ""))
            print(
                f"  {label:<35} β={r['coef']:+.4f}  SE={r['se']:.4f}"
                f"  p={r['pval']:.3f}{sig:3}  n={r['n_obs']:,}"
            )
            placebo_results.append(r)

    if placebo_results:
        pd.DataFrame(placebo_results).to_csv(OUT_TABLES / f"diag_placebo{fsuffix}.csv", index=False)
        (OUT_TABLES / f"diag_placebo{fsuffix}.tex").write_text(build_placebo_table(placebo_results))
        print(f"\n  Saved: diag_placebo{fsuffix}.{{csv,tex}}")

    # ── 3. Pre-reform balance test ──────────────────────────────────────────
    print("\n" + "=" * 70)
    print("3. Pre-reform balance test  H₀: treated mean = control mean")
    print("=" * 70)
    balance_results = []
    for df_, outcomes in [(df_t, DIAG_OUTCOMES_TENDER), (df_b, DIAG_OUTCOMES_BID)]:
        for outcome_col, label in outcomes:
            if outcome_col not in df_.columns:
                continue
            r = run_balance_test(
                df_, outcome_col, ENTITY_COL, TIME_COL, TREAT_COL, CLUSTER_COL,
                label=label,
            )
            if r is None:
                print(f"  [{label}] skipped.")
                continue
            sig = "***" if r["pval"] < 0.01 else ("**" if r["pval"] < 0.05 else
                  ("*" if r["pval"] < 0.10 else ""))
            print(
                f"  {label:<35} ctrl={r['ctrl_mean']:8.4f}"
                f"  diff={r['treat_diff']:+.4f}  SE={r['se']:.4f}"
                f"  p={r['pval']:.3f}{sig:3}"
            )
            balance_results.append(r)

    if balance_results:
        pd.DataFrame(balance_results).to_csv(OUT_TABLES / f"diag_balance{fsuffix}.csv", index=False)
        (OUT_TABLES / f"diag_balance{fsuffix}.tex").write_text(build_balance_table(balance_results))
        print(f"\n  Saved: diag_balance{fsuffix}.{{csv,tex}}")

    # ── 4. Event studies (ch spec) ──────────────────────────────────────────
    print("\n" + "=" * 70)
    print("4. Event studies (control 100–200 UTM)")
    print("=" * 70)
    src_map = {"tender": df_t, "bid": df_b}
    color_map = {
        "n_bidders":           "#1B9E77",
        "sme_share_sii":       "#D95F02",
        "winner_is_sme_sii":   "#D95F02",
        "log_win_price_ratio": "#7570B3",
        "single_bidder":       "#66A61E",
        "log_sub_price_ratio": "#7570B3",
    }

    for outcome_col, src_key, label, color in ES_OUTCOMES:
        src = src_map[src_key]
        if outcome_col not in src.columns:
            continue
        es = run_twfe_event_study(
            src,
            outcome_col=outcome_col,
            entity_col=ENTITY_COL,
            time_col=TIME_COL,
            treat_col=TREAT_COL,
            cluster_col=CLUSTER_COL,
            pre_periods=PRE_PERIODS,
            post_periods=POST_PERIODS,
            drop_k0=drop_k0,
            label=label,
        )
        if es.empty:
            print(f"  [{label}] event study returned empty.")
            continue

        csv_name = f"diag_event_study_{outcome_col}{fsuffix}.csv"
        es.to_csv(OUT_TABLES / csv_name, index=False)

        k0_note = " — Dec 2024 (k=0) excluded" if drop_k0 else ""
        fig_path = OUT_FIGURES / f"diag_event_study_{outcome_col}{fsuffix}.png"
        plot_event_study(
            es,
            title=f"Event study: {label}\nTWFE, entity + year-month FE, control 100–200 UTM{k0_note}",
            out_path=fig_path,
            color=color,
            y_label=r"$\hat\beta_k$ vs. Nov 2024",
        )
        print(f"  Saved: {csv_name}  +  {fig_path.name}")

    print("\nDone.")


if __name__ == "__main__":
    main()
