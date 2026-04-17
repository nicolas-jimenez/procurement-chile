"""
07_binscatter_moderators.py
─────────────────────────────────────────────────────────────────────────────
Diagnostic binscatters for the region-level moderators used in the
heterogeneity analysis (05_heterogeneity_region.py).

Key question: Is q_pre (market thickness) strongly negatively correlated
with nonlocal_share_pre (share of non-local bidders pre-reform)?
If yes, this validates that q_pre is the dominant moderator and subsumes
the nonlocal_share_pre effect — consistent with the spatial auction model.

Outputs
  output/did/figures/binscatter_q_nonlocal_munic.png
  output/did/figures/binscatter_moderator_pairs_munic.png
  output/did/tables/moderator_correlations_munic.csv

Run from project root:
  python code/analysis/did/07_binscatter_moderators.py
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
from scipy import stats

matplotlib.use("Agg")

# ── Path setup ────────────────────────────────────────────────────────────────
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from did_utils import (
    COMBINED,
    OUT_TABLES,
    OUT_FIGURES,
    REFORM_DATE,
    load_utm_table,
    add_utm_value,
)

# ════════════════════════════════════════════════════════════════════════════
# ── Configuration ─────────────────────────────────────────────────────────
# ════════════════════════════════════════════════════════════════════════════
SUBSAMPLE       = "munic"
SUBSAMPLE_KW    = "municipal"       # keyword to filter sector column
PRE_CUTOFF      = pd.Timestamp("2024-12-01")
MIN_PRE_TENDERS = 30                # exclude regions with fewer pre-reform tenders

# UTM bands for the treated group (same as DiD: 30–100 UTM)
TREAT_UTM_MIN = 30.0
TREAT_UTM_MAX = 100.0

# Number of bins in binscatter
N_BINS = 10

# ── Region label cleanup ──────────────────────────────────────────────────────
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
# STEP 1: Load pre-reform data and compute region-level moderators
# ════════════════════════════════════════════════════════════════════════════
print("\n" + "="*70)
print("STEP 1: Load pre-reform data and compute moderators")
print("="*70)

avail = pq.read_schema(COMBINED).names
want  = [
    "tender_id", "rut_bidder", "rut_unidad", "region_buyer",
    "fecha_pub", "source_year", "source_month",
    "monto_estimado", "same_region", "sector", "is_key_dup",
]
cols = [c for c in want if c in avail]
raw  = pd.read_parquet(COMBINED, columns=cols)
raw  = raw[~raw["is_key_dup"].fillna(False)].copy()
raw["fecha_pub"] = pd.to_datetime(raw["fecha_pub"], errors="coerce")
raw  = raw[raw["fecha_pub"].notna()].copy()

# Restrict to 2022+ (for consistency with main DiD sample)
if "source_year" in raw.columns:
    raw["source_year"] = pd.to_numeric(raw["source_year"], errors="coerce")
    raw = raw[raw["source_year"] >= 2022].copy()
else:
    raw = raw[raw["fecha_pub"].dt.year >= 2022].copy()

if "source_month" not in raw.columns:
    raw["source_month"] = raw["fecha_pub"].dt.month

print(f"  {len(raw):,} rows (2022+, all sectors)")

# Add UTM values
utm = load_utm_table()
raw = add_utm_value(raw, utm)

# ── Filter to municipalidades, treated band (30–100 UTM), pre-reform ─────────
pre = raw[
    raw["sector"].astype(str).str.lower().str.contains(SUBSAMPLE_KW, na=False)
    & (raw["monto_utm"] >= TREAT_UTM_MIN)
    & (raw["monto_utm"] <= TREAT_UTM_MAX)
    & (raw["fecha_pub"] < PRE_CUTOFF)
].copy()

print(f"  {pre['tender_id'].nunique():,} pre-reform treated-band tenders (munic)")
print(f"  {len(pre):,} bids in that sample")

# ── Compute monthly tender count for each region ──────────────────────────────
pre["year_month"] = pre["fecha_pub"].dt.to_period("M")

tenders_rm = (
    pre.groupby(["region_buyer", "year_month"])
    ["tender_id"].nunique()
    .reset_index(name="n_tenders")
)

# q_pre: average monthly tenders per region
q_pre_df = (
    tenders_rm.groupby("region_buyer")["n_tenders"]
    .mean()
    .reset_index()
    .rename(columns={"n_tenders": "q_pre"})
)

# n_months: months with at least one tender (for sample-size weight)
n_months_df = (
    tenders_rm.groupby("region_buyer")["year_month"]
    .nunique()
    .reset_index()
    .rename(columns={"year_month": "n_months_active"})
)

# total tenders per region (for MIN_PRE_TENDERS filter)
total_tenders = (
    pre.groupby("region_buyer")["tender_id"]
    .nunique()
    .reset_index()
    .rename(columns={"tender_id": "total_tenders"})
)

# ── Compute nonlocal_share_pre: bid-level share ───────────────────────────────
pre["same_region_num"] = pd.to_numeric(
    pre.get("same_region", pd.Series(np.nan, index=pre.index)),
    errors="coerce",
)
nonlocal_df = (
    pre.groupby("region_buyer")
    .agg(
        n_bids         = ("rut_bidder", "count"),
        n_nonlocal     = ("same_region_num", lambda x: (x == 0).sum()),
    )
    .reset_index()
)
nonlocal_df["nonlocal_share_pre"] = (
    nonlocal_df["n_nonlocal"] / nonlocal_df["n_bids"].replace(0, np.nan)
).clip(0, 1)

# ── Compute n_pot_local: unique firms with same_region==1 ────────────────────
local_bids = pre[pre["same_region_num"] == 1].copy()
n_pot_local_df = (
    local_bids.groupby("region_buyer")["rut_bidder"]
    .nunique()
    .reset_index()
    .rename(columns={"rut_bidder": "n_pot_local"})
)

# ── Compute totval_pre: average monthly tender value (in UTM) ────────────────
val_rm = (
    pre.groupby(["region_buyer", "year_month"])
    ["monto_utm"].mean()
    .reset_index(name="avg_val_utm")
)
totval_df = (
    val_rm.groupby("region_buyer")["avg_val_utm"]
    .mean()
    .reset_index()
    .rename(columns={"avg_val_utm": "totval_pre"})
)

# ── Merge all moderators ──────────────────────────────────────────────────────
mod = (
    q_pre_df
    .merge(nonlocal_df[["region_buyer", "n_bids", "nonlocal_share_pre"]], on="region_buyer", how="outer")
    .merge(n_pot_local_df,  on="region_buyer", how="outer")
    .merge(totval_df,       on="region_buyer", how="outer")
    .merge(total_tenders,   on="region_buyer", how="outer")
    .merge(n_months_df,     on="region_buyer", how="outer")
)

# Apply MIN_PRE_TENDERS filter
mod = mod[mod["total_tenders"] >= MIN_PRE_TENDERS].copy()
mod["region_short"] = mod["region_buyer"].apply(_short_region)

# Santiago indicator
mod["is_santiago"] = mod["region_buyer"].astype(str).str.lower().str.contains(
    "metropolitana|santiago", na=False
).astype(int)

print(f"\n  {len(mod)} regions after MIN_PRE_TENDERS={MIN_PRE_TENDERS} filter")
print("\n  Region moderator summary:")
print(
    mod[["region_short", "q_pre", "nonlocal_share_pre", "n_pot_local",
         "totval_pre", "total_tenders", "is_santiago"]]
    .sort_values("q_pre", ascending=False)
    .to_string(index=False)
)

# ── Pearson / Spearman correlations between moderators ───────────────────────
MODERATOR_COLS = ["q_pre", "nonlocal_share_pre", "n_pot_local", "totval_pre"]
mod_clean = mod[MODERATOR_COLS].dropna()

corr_rows = []
for i, c1 in enumerate(MODERATOR_COLS):
    for c2 in MODERATOR_COLS[i+1:]:
        pair = mod_clean[[c1, c2]].dropna()
        if len(pair) < 4:
            continue
        r_p, p_p = stats.pearsonr(pair[c1], pair[c2])
        r_s, p_s = stats.spearmanr(pair[c1], pair[c2])
        corr_rows.append({
            "var1": c1, "var2": c2,
            "pearson_r": round(r_p, 3), "pearson_p": round(p_p, 3),
            "spearman_r": round(r_s, 3), "spearman_p": round(p_s, 3),
            "n": len(pair),
        })
        print(f"  {c1} × {c2}: Pearson r={r_p:.3f} (p={p_p:.3f}), "
              f"Spearman ρ={r_s:.3f} (p={p_s:.3f})")

corr_df = pd.DataFrame(corr_rows)
OUT_TABLES.mkdir(parents=True, exist_ok=True)
corr_df.to_csv(OUT_TABLES / f"moderator_correlations_{SUBSAMPLE}.csv", index=False)
print(f"\n  Saved: moderator_correlations_{SUBSAMPLE}.csv")


# ════════════════════════════════════════════════════════════════════════════
# STEP 2: Binscatter — q_pre vs. nonlocal_share_pre (main diagnostic)
# ════════════════════════════════════════════════════════════════════════════
print("\n" + "="*70)
print("STEP 2: Binscatter — q_pre vs. nonlocal_share_pre")
print("="*70)


def binscatter(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    n_bins: int = N_BINS,
    *,
    ax: plt.Axes,
    xlabel: str,
    ylabel: str,
    title: str,
    annotate_regions: bool = True,
    highlight_col: str | None = "is_santiago",
    highlight_label: str = "RM/Santiago",
    reg_line: bool = True,
    log_x: bool = False,
) -> dict:
    """
    Standard binscatter:
      1. Residualise both x and y on log(n_bids) (control for sample size)
         — actually just raw scatter here since we have 16 obs total.
      2. Bin x into n_bins equal-frequency bins.
      3. Plot mean-x vs mean-y per bin.
      4. Overlay individual region dots (with Santiago highlighted).
      5. Draw OLS regression line on raw data.

    Returns dict with regression stats.
    """
    sub = df[[x_col, y_col]].copy()
    if highlight_col:
        sub[highlight_col] = df[highlight_col].values
    if annotate_regions:
        sub["label"] = df["region_short"].values
    sub = sub.dropna(subset=[x_col, y_col])

    x_raw = np.log(sub[x_col]) if log_x else sub[x_col].to_numpy(float)
    y_raw = sub[y_col].to_numpy(float)

    # Linear fit (kept for summary stats / r / p-value)
    slope, intercept, r, pval, se = stats.linregress(x_raw, y_raw)

    # Degree-3 polynomial fit for the plotted curve
    poly_deg = 3
    poly_coefs = np.polyfit(x_raw, y_raw, poly_deg)
    poly_fn    = np.poly1d(poly_coefs)

    # Bins (equal-frequency)
    try:
        bin_labels = pd.qcut(x_raw, q=min(n_bins, len(sub) - 1), labels=False,
                             duplicates="drop")
    except ValueError:
        bin_labels = pd.cut(x_raw, bins=min(n_bins, len(sub) - 1), labels=False,
                            include_lowest=True)

    bin_df = pd.DataFrame({"x": x_raw, "y": y_raw, "bin": bin_labels})
    bin_means = bin_df.groupby("bin")[["x", "y"]].mean().reset_index()

    # Individual region scatter
    if highlight_col and highlight_col in sub.columns:
        is_hl = sub[highlight_col].fillna(0).astype(bool)
        ax.scatter(x_raw[~is_hl.values], y_raw[~is_hl.values],
                   color="#1f77b4", alpha=0.45, s=30, zorder=3, label="Other regions")
        ax.scatter(x_raw[is_hl.values], y_raw[is_hl.values],
                   color="#d62728", alpha=0.9, s=60, zorder=5,
                   marker="*", label=highlight_label)
    else:
        ax.scatter(x_raw, y_raw, color="#1f77b4", alpha=0.45, s=30, zorder=3)

    # Bin means
    ax.scatter(bin_means["x"], bin_means["y"],
               color="black", s=55, zorder=6, marker="D", label="Bin mean")

    # Polynomial fit line (degree 3)
    if reg_line:
        x_line = np.linspace(x_raw.min(), x_raw.max(), 200)
        y_line = poly_fn(x_line)
        ax.plot(x_line, y_line, color="#ff7f0e", linewidth=1.6, zorder=4,
                label=f"Poly(3) fit  (linear r={r:.2f}, p={pval:.3f})")

    # Region labels (small, only if few points)
    if annotate_regions and "label" in sub.columns and len(sub) <= 20:
        for xi, yi, lab in zip(x_raw, y_raw, sub["label"]):
            ax.annotate(
                lab, (xi, yi),
                fontsize=6.5, ha="left", va="bottom",
                xytext=(3, 2), textcoords="offset points", color="gray",
            )

    ax.set_xlabel(("log " if log_x else "") + xlabel, fontsize=9)
    ax.set_ylabel(ylabel, fontsize=9)
    ax.set_title(title, fontsize=9)
    ax.legend(fontsize=7, loc="best")
    ax.grid(color="#e8e8e8", linewidth=0.5)
    ax.tick_params(labelsize=8)

    return {"slope": slope, "intercept": intercept, "r": r, "pval": pval, "se": se}


OUT_FIGURES.mkdir(parents=True, exist_ok=True)

# ── Main diagnostic: q_pre vs. nonlocal_share_pre ────────────────────────────
fig, axes = plt.subplots(1, 2, figsize=(13, 5))
fig.suptitle(
    "Market thickness (q_pre) vs. non-local bidder share (nonlocal_share_pre)\n"
    "Municipalidades, treated band (30–100 UTM), pre-reform",
    fontsize=11, y=1.02,
)

# Left panel: raw scale
stats_raw = binscatter(
    mod, "q_pre", "nonlocal_share_pre",
    ax=axes[0],
    xlabel="Avg monthly tenders (q_pre)",
    ylabel="Share non-local bidders (pre-reform)",
    title="Raw scale",
    log_x=False,
)

# Right panel: log scale (handles Santiago outlier)
stats_log = binscatter(
    mod, "q_pre", "nonlocal_share_pre",
    ax=axes[1],
    xlabel="Avg monthly tenders (q_pre)",
    ylabel="Share non-local bidders (pre-reform)",
    title="Log scale (handles Santiago outlier)",
    log_x=True,
)

plt.tight_layout()
fpath = OUT_FIGURES / f"binscatter_q_nonlocal_{SUBSAMPLE}.png"
fig.savefig(fpath, dpi=150, bbox_inches="tight")
plt.close(fig)
print(f"  Saved: {fpath.name}")
print(f"  Raw OLS:  β={stats_raw['slope']:.4f}, r={stats_raw['r']:.3f}, p={stats_raw['pval']:.3f}")
print(f"  Log OLS:  β={stats_log['slope']:.4f}, r={stats_log['r']:.3f}, p={stats_log['pval']:.3f}")


# ════════════════════════════════════════════════════════════════════════════
# STEP 3: Full moderator-pair matrix (2×3 grid)
# ════════════════════════════════════════════════════════════════════════════
print("\n" + "="*70)
print("STEP 3: Full moderator-pair scatter matrix")
print("="*70)

PAIRS = [
    ("q_pre",               "nonlocal_share_pre",  False,
     "Avg monthly tenders", "Share non-local bidders"),
    ("q_pre",               "n_pot_local",         False,
     "Avg monthly tenders", "N potential local firms"),
    ("q_pre",               "totval_pre",           False,
     "Avg monthly tenders", "Avg monthly value (UTM)"),
    ("n_pot_local",         "nonlocal_share_pre",  False,
     "N potential local firms", "Share non-local bidders"),
    ("totval_pre",          "nonlocal_share_pre",  False,
     "Avg monthly value (UTM)", "Share non-local bidders"),
    ("n_pot_local",         "totval_pre",           False,
     "N potential local firms", "Avg monthly value (UTM)"),
]

fig2, axes2 = plt.subplots(2, 3, figsize=(15, 9))
fig2.suptitle(
    "Pairwise moderator relationships\n"
    "Municipalidades, treated band (30–100 UTM), pre-reform",
    fontsize=12, y=1.01,
)

for ax, (x_col, y_col, log_x, xlabel, ylabel) in zip(axes2.flat, PAIRS):
    sub_p = mod[[x_col, y_col, "is_santiago", "region_short"]].dropna()
    if len(sub_p) < 3:
        ax.text(0.5, 0.5, "insufficient data", transform=ax.transAxes,
                ha="center", va="center")
        continue
    binscatter(
        sub_p, x_col, y_col,
        ax=ax,
        xlabel=xlabel,
        ylabel=ylabel,
        title=f"{x_col} × {y_col}",
        log_x=log_x,
        annotate_regions=True,
        highlight_col="is_santiago",
        highlight_label="RM",
    )

plt.tight_layout()
fpath2 = OUT_FIGURES / f"binscatter_moderator_pairs_{SUBSAMPLE}.png"
fig2.savefig(fpath2, dpi=150, bbox_inches="tight")
plt.close(fig2)
print(f"  Saved: {fpath2.name}")


# ════════════════════════════════════════════════════════════════════════════
# STEP 4: Print summary for main text / appendix decision
# ════════════════════════════════════════════════════════════════════════════
print("\n" + "="*70)
print("STEP 4: Summary — moderator collinearity and Santiago leverage")
print("="*70)
print(corr_df[["var1", "var2", "pearson_r", "spearman_r", "n"]].to_string(index=False))

# Santiago leverage: how many SDs is RM above the mean on q_pre?
if "q_pre" in mod.columns and mod["is_santiago"].any():
    q_mu, q_sd = mod["q_pre"].mean(), mod["q_pre"].std()
    q_rm = mod.loc[mod["is_santiago"] == 1, "q_pre"].values
    if len(q_rm):
        print(f"\n  Santiago q_pre = {q_rm[0]:.1f}, mean = {q_mu:.1f}, SD = {q_sd:.1f}")
        print(f"  Santiago is {(q_rm[0] - q_mu) / q_sd:.1f} SDs above the mean → "
              f"{'HIGH' if abs(q_rm[0] - q_mu)/q_sd > 2 else 'MODERATE'} leverage")

print("\n" + "="*70)
print("DONE — all binscatter outputs saved.")
print("="*70)
