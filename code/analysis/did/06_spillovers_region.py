"""
06_spillovers_region.py
─────────────────────────────────────────────────────────────────────────────
Cross-market spillover analysis for the Compra Ágil reform (Ley 21.634).

Theory: The reform excluded non-local firms from 30-100 UTM Compra Ágil
tenders. Firms based in high-export-share regions (e.g. Santiago) had capacity
freed up from peripheral markets and may have redirected to above-threshold
(100-500 UTM) licitaciones in their home regions. This should increase
competition in above-threshold tenders in high-export-share regions post-reform.

Design:
  Unit of analysis : region × year-month (region-month panel)
  Sample           : Municipalidades, ABOVE-threshold tenders (100-500 UTM)
  Outcomes         : n_bidders, n_local, n_nonlocal, n_sme, n_large,
                     share_nonlocal, single_bidder, log_win_price_ratio
  Treatment var    : export_share_r (computed from pre-reform bids)
  Specification    : event-study interacted with export_share_r

  y_rt = α_r + γ_t + Σ_{k≠-1} δ_k · (1[t=k] × ExportShare_r) + ε_rt

  Estimated separately for:
    version="continuous" : ExportShare_r standardized to mean 0, SD 1
    version="binary"     : HighExport_r = 1 if export_share_r in top tercile

Outputs:
  output/did/tables/spillover_export_share_munic.csv
  output/did/tables/spillover_event_study_{outcome}_{version}_munic.csv
  output/did/figures/spillover_event_study_{outcome}_munic.png
  output/did/figures/spillover_export_share_map_munic.png

Notes:
  - Clustering is at the region level (N=16 clusters). SEs are approximate;
    treat p-values as indicative, not precise.
  - Pre-reform window for export_share: all bids before 2024-12-12.
  - Above-threshold band: 100-500 UTM estimated cost.
  - Reform date: 2024-12-12 → event time k=0 is December 2024.
  - Omitted period: k=-1 (November 2024).
"""

from __future__ import annotations

import sys
from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd
import pyarrow.parquet as pq

matplotlib.use("Agg")

# ── Path setup ────────────────────────────────────────────────────────────────
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from did_utils import (
    COMBINED,
    OUT_TABLES,
    OUT_FIGURES,
    OUT_SAMPLES,
    REFORM_DATE,
    _twoway_demean,
    _cluster_se,
    load_utm_table,
    add_utm_value,
)

# ════════════════════════════════════════════════════════════════════════════
# ── Configuration ─────────────────────────────────────────────────────────
# ════════════════════════════════════════════════════════════════════════════
SUBSAMPLE       = "munic"        # only municipalidades
SUBSAMPLE_KW    = "municipal"    # keyword to filter sector column
ABOVE_UTM_MIN   = 100.0          # above-threshold lower bound (exclusive)
ABOVE_UTM_MAX   = 500.0          # above-threshold upper bound (inclusive)
PRE_CUTOFF      = pd.Timestamp("2024-12-12")   # reform date
OMIT_PERIOD     = pd.Period("2024-11", freq="M")   # baseline month (k=-1)
REFORM_PERIOD   = pd.Period("2024-12", freq="M")   # k=0
MIN_PRE_BIDS    = 50             # min pre-reform bids from a region to include
MIN_PRE_TENDERS = 10             # min above-threshold pre-reform tenders per region-month
HIGH_EXPORT_PCTILE = 67          # binary: top third = "high export"

# Event-study window (relative to reform month k=0)
K_MIN, K_MAX = -6, 9

# Outcomes to aggregate on the above-threshold (destination) side
OUTCOMES = [
    ("n_bidders",        "N bidders"),
    ("n_local",          "N local bidders"),
    ("n_nonlocal",       "N non-local bidders"),
    ("n_sme",            "N SME bidders"),
    ("n_large",          "N large bidders"),
    ("share_nonlocal",   "Share non-local bidders"),
    ("single_bidder",    "Pr(single bidder)"),
    ("log_win_price",    "log(win bid / ref price)"),
]

# ── Region label cleanup (reuse from 05_heterogeneity_region.py) ─────────────
_REGION_PREFIXES = (
    "Región de los ", "Región de las ", "Región del ",
    "Región de la ", "Región de ", "Región ",
)
_SHORT_NAMES = {
    "Libertador General Bernardo O\u2019Higgins": "O\u2019Higgins",
    "Libertador General Bernardo O'Higgins":      "O'Higgins",
    "Ays\u00e9n del General Carlos Ib\u00e1\u00f1ez del Campo": "Ays\u00e9n",
    "Aysen del General Carlos Ibanez del Campo":  "Ays\u00e9n",
}

def _short_region(name: str) -> str:
    s = name
    for pfx in _REGION_PREFIXES:
        if name.startswith(pfx):
            s = name[len(pfx):]
            break
    return _SHORT_NAMES.get(s, s)


# ════════════════════════════════════════════════════════════════════════════
# STEP 1: Load raw combined data and compute export_share_r
# ════════════════════════════════════════════════════════════════════════════
print("\n" + "="*70)
print("STEP 1: Load raw data and compute export_share_r")
print("="*70)

# Load full combined bid-level parquet (2022+)
print("  Loading combined_sii_merged_filtered (raw, 2022+) …")
avail = pq.read_schema(COMBINED).names
want  = [
    "tender_id", "rut_bidder", "rut_unidad", "region_buyer",
    "fecha_pub", "source_year", "source_month",
    "monto_estimado", "monto_oferta",
    "same_region", "sector", "is_key_dup",
    "tramoventas", "tamano", "is_selected",
]
cols = [c for c in want if c in avail]
raw  = pd.read_parquet(COMBINED, columns=cols)
raw  = raw[~raw["is_key_dup"].fillna(False)].copy()
raw["fecha_pub"] = pd.to_datetime(raw["fecha_pub"], errors="coerce")
raw  = raw[raw["fecha_pub"].notna()].copy()

# Restrict to 2022+
if "source_year" in raw.columns:
    raw["source_year"] = pd.to_numeric(raw["source_year"], errors="coerce")
    raw = raw[raw["source_year"] >= 2022].copy()
else:
    raw = raw[raw["fecha_pub"].dt.year >= 2022].copy()

print(f"  {len(raw):,} rows (2022+, all sectors)")

# Add UTM values
print("  Adding UTM values …")
utm = load_utm_table()
raw = add_utm_value(raw, utm)

# ── Assign home region to each firm ──────────────────────────────────────────
# Home region = mode of region_buyer where same_region == 1 (locally bid).
# If a firm never has same_region==1, fall back to overall mode of region_buyer.
print("  Assigning home region to each firm …")

pre_raw = raw[raw["fecha_pub"] < PRE_CUTOFF].copy()
pre_raw["same_region_num"] = pd.to_numeric(
    pre_raw.get("same_region", pd.Series(np.nan, index=pre_raw.index)),
    errors="coerce"
)

# Local bids: same_region == 1
local_bids = pre_raw[pre_raw["same_region_num"] == 1].copy()

def _mode(s):
    if len(s) == 0:
        return np.nan
    m = s.mode()
    return m.iloc[0] if len(m) > 0 else np.nan

# Home region from local bids
home_from_local = (
    local_bids.groupby("rut_bidder")["region_buyer"]
    .agg(_mode)
    .rename("home_region_local")
    .reset_index()
)

# Home region from all bids (fallback)
home_from_all = (
    pre_raw.groupby("rut_bidder")["region_buyer"]
    .agg(_mode)
    .rename("home_region_all")
    .reset_index()
)

firm_home = home_from_all.merge(home_from_local, on="rut_bidder", how="left")
firm_home["home_region"] = firm_home["home_region_local"].combine_first(
    firm_home["home_region_all"]
)
firm_home = firm_home[["rut_bidder", "home_region"]].dropna()
print(f"  {len(firm_home):,} firms with assigned home region")

# ── Compute export_share_r ────────────────────────────────────────────────────
# export_share_r = (pre-reform bids by home-r firms in OTHER regions) /
#                  (total pre-reform bids by home-r firms)
pre_with_home = pre_raw.merge(firm_home, on="rut_bidder", how="inner")
pre_with_home["is_export"] = (
    pre_with_home["region_buyer"] != pre_with_home["home_region"]
).astype(int)

export_stats = (
    pre_with_home.groupby("home_region")
    .agg(
        total_bids=("rut_bidder", "count"),
        export_bids=("is_export", "sum"),
    )
    .reset_index()
    .rename(columns={"home_region": "region_buyer"})
)
export_stats["export_share"] = (
    export_stats["export_bids"] / export_stats["total_bids"]
).clip(0, 1)

# Filter to regions with enough pre-reform bids
export_stats = export_stats[export_stats["total_bids"] >= MIN_PRE_BIDS].copy()

# Standardize and binary indicator
mu, sd = export_stats["export_share"].mean(), export_stats["export_share"].std()
export_stats["export_share_std"] = (export_stats["export_share"] - mu) / max(sd, 1e-8)
threshold = np.percentile(export_stats["export_share"], HIGH_EXPORT_PCTILE)
export_stats["high_export"] = (export_stats["export_share"] >= threshold).astype(int)

# Save export share table
OUT_TABLES.mkdir(parents=True, exist_ok=True)
export_stats.to_csv(
    OUT_TABLES / f"spillover_export_share_{SUBSAMPLE}.csv", index=False
)
print("\n  Export share by region:")
print(
    export_stats[["region_buyer", "total_bids", "export_bids", "export_share",
                  "high_export"]]
    .sort_values("export_share", ascending=False)
    .to_string(index=False)
)


# ════════════════════════════════════════════════════════════════════════════
# STEP 2: Build above-threshold region-month panel (100-500 UTM, municipalidades)
# ════════════════════════════════════════════════════════════════════════════
print("\n" + "="*70)
print("STEP 2: Build above-threshold region-month panel")
print("="*70)

# Filter to municipalidades sector
if "sector" in raw.columns:
    above = raw[
        raw["sector"].astype(str).str.lower().str.contains(SUBSAMPLE_KW, na=False)
    ].copy()
else:
    above = raw.copy()
print(f"  {len(above):,} rows after municipalidades filter")

# Filter to 100-500 UTM
above = above[
    (above["monto_utm"] > ABOVE_UTM_MIN) &
    (above["monto_utm"] <= ABOVE_UTM_MAX)
].copy()
print(f"  {len(above):,} rows after 100-500 UTM filter")

# Restrict to Jan 2023 onwards (same start as DiD; no upper cap so all available data is used)
above = above[
    above["fecha_pub"] >= pd.Timestamp("2023-01-01")
].copy()
print(f"  {len(above):,} rows after date filter (Jan 2023 – present)")

above["year_month"] = above["fecha_pub"].dt.to_period("M")

# ── SME flag ──────────────────────────────────────────────────────────────────
# Use tramoventas (SII): codes 1-9 = SME, 10-13 = large
if "tramoventas" in above.columns:
    def _sme_flag(v):
        try:
            code = int(round(float(str(v).strip())))
            if 1 <= code <= 9:  return 1
            if 10 <= code <= 13: return 0
        except Exception:
            pass
        return np.nan
    above["is_sme"] = above["tramoventas"].map(_sme_flag)
else:
    above["is_sme"] = np.nan

above["is_large"] = np.where(
    above["is_sme"].notna(), (above["is_sme"] == 0).astype(float), np.nan
)

# ── same_region flag ──────────────────────────────────────────────────────────
above["same_region_num"] = pd.to_numeric(
    above.get("same_region", pd.Series(np.nan, index=above.index)),
    errors="coerce"
)

# ── Winning bid flag ──────────────────────────────────────────────────────────
if "is_selected" in above.columns:
    above["is_selected_num"] = pd.to_numeric(above["is_selected"], errors="coerce")
else:
    above["is_selected_num"] = np.nan

# ── Aggregate to tender level first, then to region-month ────────────────────
print("  Aggregating to tender level …")

# Basic per-tender aggregates
tender_agg = (
    above.groupby(["tender_id", "region_buyer", "year_month",
                   "rut_unidad", "monto_estimado", "monto_utm"])
    .agg(
        n_bidders    = ("rut_bidder",       "nunique"),
        n_local      = ("same_region_num",  lambda x: (pd.to_numeric(x, errors="coerce") == 1).sum()),
        n_nonlocal   = ("same_region_num",  lambda x: (pd.to_numeric(x, errors="coerce") == 0).sum()),
        n_sme        = ("is_sme",           lambda x: (pd.to_numeric(x, errors="coerce") == 1).sum()),
        n_large      = ("is_large",         lambda x: (pd.to_numeric(x, errors="coerce") == 1).sum()),
    )
    .reset_index()
)

# Winning bid: merge separately to avoid fragile closure in lambda
if "is_selected_num" in above.columns and "monto_oferta" in above.columns:
    win_rows = above[above["is_selected_num"] == 1][["tender_id", "monto_oferta"]].copy()
    win_bid_df = (
        win_rows.groupby("tender_id")["monto_oferta"]
        .first()
        .reset_index()
        .rename(columns={"monto_oferta": "win_bid"})
    )
    tender_agg = tender_agg.merge(win_bid_df, on="tender_id", how="left")
else:
    tender_agg["win_bid"] = np.nan

tender_agg["single_bidder"]  = (tender_agg["n_bidders"] == 1).astype(float)
tender_agg["share_nonlocal"] = (
    tender_agg["n_nonlocal"] / tender_agg["n_bidders"].replace(0, np.nan)
)

# Log win price ratio
tender_agg["log_win_price"] = np.where(
    (tender_agg["win_bid"] > 0) & (tender_agg["monto_estimado"] > 0),
    np.log(tender_agg["win_bid"] / tender_agg["monto_estimado"]),
    np.nan,
)

print(f"  {len(tender_agg):,} tenders in above-threshold sample")

# ── Aggregate to region-month panel ──────────────────────────────────────────
print("  Aggregating to region-month …")
outcome_cols = [o for o, _ in OUTCOMES]

region_month = (
    tender_agg.groupby(["region_buyer", "year_month"])
    [outcome_cols]
    .mean()
    .reset_index()
)

# Count tenders per region-month (for sample size checks)
n_tenders_rm = (
    tender_agg.groupby(["region_buyer", "year_month"])
    .size()
    .reset_index(name="n_tenders")
)
region_month = region_month.merge(n_tenders_rm, on=["region_buyer", "year_month"])

print(f"  {len(region_month):,} region-month observations")
print("  Region-month counts:\n",
      region_month.groupby("region_buyer")["n_tenders"].sum()
      .sort_values(ascending=False).to_string())


# ════════════════════════════════════════════════════════════════════════════
# STEP 3: Merge export_share and build event-study panel
# ════════════════════════════════════════════════════════════════════════════
print("\n" + "="*70)
print("STEP 3: Merge export_share → build event-study panel")
print("="*70)

panel = region_month.merge(
    export_stats[["region_buyer", "export_share", "export_share_std", "high_export"]],
    on="region_buyer",
    how="inner",
)
print(f"  {panel['region_buyer'].nunique()} regions after merge with export_share")
print(f"  {len(panel):,} region-month obs in panel")

# Event time (months relative to reform = k=0 at 2024-12)
# Use ordinal subtraction (same as did_utils.py) to ensure integer result
_reform_ord = REFORM_PERIOD.ordinal
panel["k"] = panel["year_month"].apply(
    lambda p: (p.ordinal if hasattr(p, "ordinal") else int(p)) - _reform_ord
)
panel = panel[(panel["k"] >= K_MIN) & (panel["k"] <= K_MAX)].copy()
print(f"  {len(panel):,} obs after event-window filter (k in [{K_MIN}, {K_MAX}])")

# Sanity: which regions are in the panel?
print("\n  Regions in panel:")
region_summary = (
    panel.groupby("region_buyer")
    .agg(
        n_months      = ("year_month",     "nunique"),
        export_share  = ("export_share",   "first"),
        high_export   = ("high_export",    "first"),
        avg_n_bidders = ("n_bidders",      "mean"),
    )
    .sort_values("export_share", ascending=False)
    .reset_index()
)
region_summary["region_short"] = region_summary["region_buyer"].apply(_short_region)
print(region_summary[["region_short", "export_share", "high_export",
                        "n_months", "avg_n_bidders"]].to_string(index=False))


# ════════════════════════════════════════════════════════════════════════════
# STEP 4: Event-study regression
# ════════════════════════════════════════════════════════════════════════════
print("\n" + "="*70)
print("STEP 4: Event-study regressions")
print("="*70)

# All event-time periods except omitted (k=-1)
all_k = list(range(K_MIN, K_MAX + 1))
event_k = [k for k in all_k if k != -1]   # omit k=-1

def run_event_study(panel: pd.DataFrame, outcome: str, treat_var: str) -> pd.DataFrame:
    """
    Run event-study TWFE:
      y_rt = α_r + γ_t + Σ_{k≠-1} δ_k (1[t=k] × TreatVar_r) + ε_rt

    Region FE = entity, year_month = time.
    Cluster at region level.
    Returns DataFrame with columns: k, coef, se, pval, ci_low, ci_high.
    """
    df = panel[["region_buyer", "year_month", "k", outcome, treat_var]].copy()
    df = df.replace([np.inf, -np.inf], np.nan).dropna(subset=[outcome, treat_var])

    if len(df) == 0:
        return pd.DataFrame()

    # Only include k values that actually appear in the data (avoids zero-columns)
    present_k = set(df["k"].unique())
    active_k  = [k for k in event_k if k in present_k]

    # Build event-time × treat_var interaction columns
    int_cols = []
    for k in active_k:
        col = f"int_k{k:+d}"
        df[col] = (df["k"] == k).astype(float) * df[treat_var]
        int_cols.append(col)

    # Two-way demean: region FE + year_month FE
    demean_cols = [outcome] + int_cols
    dm = _twoway_demean(df, "region_buyer", "year_month", demean_cols)

    y   = dm[outcome].to_numpy(dtype=float)
    X   = dm[int_cols].to_numpy(dtype=float)
    clu = df["region_buyer"].to_numpy()

    # Drop non-finite rows
    valid = np.isfinite(y) & np.isfinite(X).all(axis=1)
    y, X, clu = y[valid], X[valid], clu[valid]

    if len(y) < 5:
        return pd.DataFrame()

    coefs, _, _, _ = np.linalg.lstsq(X, y, rcond=None)
    resid = y - X @ coefs
    se    = _cluster_se(X, resid, clu)

    from scipy.special import ndtr as _ndtr
    results = []
    for i, k in enumerate(active_k):
        c, s = float(coefs[i]), float(se[i])
        t    = c / s if s > 0 else np.nan
        p    = float(2 * (1 - _ndtr(abs(t)))) if np.isfinite(t) else np.nan
        results.append({
            "k":      k,
            "coef":   c,
            "se":     s,
            "pval":   p,
            "ci_low":  c - 1.96 * s,
            "ci_high": c + 1.96 * s,
        })

    # Insert k=-1 as zeros (omitted baseline)
    results.append({"k": -1, "coef": 0.0, "se": 0.0,
                    "pval": np.nan, "ci_low": 0.0, "ci_high": 0.0})

    # Insert NaN for k values with no data (not yet observed)
    missing_k = [k for k in event_k if k not in present_k]
    for mk in missing_k:
        results.append({"k": mk, "coef": np.nan, "se": np.nan,
                        "pval": np.nan, "ci_low": np.nan, "ci_high": np.nan})

    return pd.DataFrame(results).sort_values("k").reset_index(drop=True)


# Run for each outcome × version
VERSIONS = {
    "continuous": "export_share_std",
    "binary":     "high_export",
}

all_results: dict[tuple, pd.DataFrame] = {}

for outcome_col, outcome_label in OUTCOMES:
    if outcome_col not in panel.columns:
        print(f"  [{outcome_col}] not found in panel, skipping")
        continue
    for version, treat_var in VERSIONS.items():
        print(f"  Running: {outcome_col} × {version} …", end=" ")
        res = run_event_study(panel, outcome_col, treat_var)
        if res.empty:
            print("skipped (empty)")
            continue
        all_results[(outcome_col, version)] = res
        # Save CSV
        fname = OUT_TABLES / f"spillover_event_study_{outcome_col}_{version}_{SUBSAMPLE}.csv"
        res.to_csv(fname, index=False)
        print(f"saved ({len(res)} periods)")


# ════════════════════════════════════════════════════════════════════════════
# STEP 5: Coefficient plots (event study plots)
# ════════════════════════════════════════════════════════════════════════════
print("\n" + "="*70)
print("STEP 5: Coefficient plots")
print("="*70)

# Color palette
COL_CONT = "#1f77b4"   # blue for continuous
COL_BIN  = "#d62728"   # red for binary

def plot_event_study(
    res_cont: pd.DataFrame,
    res_bin:  pd.DataFrame,
    outcome_label: str,
    outcome_col:   str,
) -> None:
    """
    Plot event-study coefficients for both continuous and binary treatment.
    Two panels side-by-side sharing the x-axis.
    """
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.5), sharey=False)
    fig.suptitle(
        f"Cross-market spillovers: {outcome_label}\n"
        f"Above-threshold (100–500 UTM) municipalidades tenders",
        fontsize=11, y=1.01,
    )

    specs = [
        (axes[0], res_cont, COL_CONT,
         "Treatment: export share (standardized)",
         "δₖ × export share SD"),
        (axes[1], res_bin, COL_BIN,
         "Treatment: high-export region (top tercile)",
         "δₖ (high vs. low export)"),
    ]

    for ax, res, color, title, ylabel in specs:
        if res is None or res.empty:
            ax.text(0.5, 0.5, "no results", transform=ax.transAxes,
                    ha="center", va="center", fontsize=10)
            ax.set_title(title, fontsize=9)
            continue

        k_vals = res["k"].to_numpy()
        coefs  = res["coef"].to_numpy()
        ci_lo  = res["ci_low"].to_numpy()
        ci_hi  = res["ci_high"].to_numpy()
        pvals  = res["pval"].to_numpy()

        # Shade post-reform area
        ax.axvspan(-0.5, K_MAX + 0.5, color="#f0f0f0", zorder=0, label="_nolegend_")

        # Zero reference line
        ax.axhline(0, color="black", linewidth=0.8, linestyle="--", zorder=1)
        # Reform line
        ax.axvline(-0.5, color="gray", linewidth=1.0, linestyle=":", zorder=2,
                   label="Reform (Dec 2024)")

        # CI band
        ax.fill_between(k_vals, ci_lo, ci_hi,
                         alpha=0.18, color=color, zorder=3, label="_nolegend_")

        # Coefficient dots
        for ki, c, lo, hi, p in zip(k_vals, coefs, ci_lo, ci_hi, pvals):
            if ki == -1:   # omitted period
                ax.scatter(ki, 0, color="black", s=25, zorder=5, marker="D")
                continue
            sig = np.isfinite(p) and p < 0.10
            ax.errorbar(
                ki, c, yerr=[[c - lo], [hi - c]],
                fmt="o", color=color if sig else "gray",
                ecolor=color if sig else "lightgray",
                capsize=3, markersize=5, linewidth=1.2, zorder=4,
            )

        ax.set_xlabel("Months relative to reform (k=0 = Dec 2024)", fontsize=9)
        ax.set_ylabel(ylabel, fontsize=9)
        ax.set_title(title, fontsize=9)
        ax.set_xticks(all_k)
        ax.tick_params(axis="both", labelsize=8)
        ax.grid(axis="y", color="#e0e0e0", linewidth=0.5, zorder=0)
        ax.set_xlim(K_MIN - 0.7, K_MAX + 0.7)

        # Add note about few clusters
        ax.text(
            0.01, 0.02,
            "Note: 16 region clusters — SEs approximate",
            transform=ax.transAxes, fontsize=7, color="gray",
            va="bottom", style="italic",
        )

    plt.tight_layout()
    fpath = OUT_FIGURES / f"spillover_event_study_{outcome_col}_{SUBSAMPLE}.png"
    fig.savefig(fpath, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {fpath.name}")


OUT_FIGURES.mkdir(parents=True, exist_ok=True)

for outcome_col, outcome_label in OUTCOMES:
    res_cont = all_results.get((outcome_col, "continuous"), pd.DataFrame())
    res_bin  = all_results.get((outcome_col, "binary"),     pd.DataFrame())
    if res_cont.empty and res_bin.empty:
        continue
    plot_event_study(res_cont, res_bin, outcome_label, outcome_col)


# ════════════════════════════════════════════════════════════════════════════
# STEP 6: Export share bar chart (descriptive)
# ════════════════════════════════════════════════════════════════════════════
print("\n" + "="*70)
print("STEP 6: Export share bar chart")
print("="*70)

plot_df = export_stats.sort_values("export_share", ascending=True).copy()
plot_df["region_short"] = plot_df["region_buyer"].apply(_short_region)

fig, ax = plt.subplots(figsize=(7, 6))
colors = [COL_BIN if h else COL_CONT for h in plot_df["high_export"]]
bars = ax.barh(plot_df["region_short"], plot_df["export_share"],
               color=colors, edgecolor="white", linewidth=0.5)

# Threshold line
ax.axvline(threshold, color="black", linewidth=1.0, linestyle="--",
           label=f"Top-tercile threshold ({threshold:.2f})")

ax.set_xlabel("Export share (pre-reform)", fontsize=10)
ax.set_title(
    "Regional export share: fraction of pre-reform bids\nplaced by home-region firms in other regions",
    fontsize=10,
)
ax.tick_params(axis="both", labelsize=9)
ax.grid(axis="x", color="#e0e0e0", linewidth=0.5)

# Legend patches
import matplotlib.patches as mpatches
high_patch = mpatches.Patch(color=COL_BIN,  label="High export (top tercile)")
low_patch  = mpatches.Patch(color=COL_CONT, label="Low/mid export")
ax.legend(handles=[high_patch, low_patch,
                   plt.Line2D([0], [0], color="black", linestyle="--",
                               label=f"Threshold ({threshold:.2f})")],
          fontsize=8, loc="lower right")

plt.tight_layout()
fpath = OUT_FIGURES / f"spillover_export_share_{SUBSAMPLE}.png"
fig.savefig(fpath, dpi=150, bbox_inches="tight")
plt.close(fig)
print(f"  Saved: {fpath.name}")


print("\n" + "="*70)
print("DONE — all spillover outputs saved.")
print("="*70)
