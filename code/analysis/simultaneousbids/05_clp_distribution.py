"""
05_clp_distribution.py
CLP earned per month: distribution plots and summary table by firm size, pre/post reform.
"""

import sys
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from scipy.stats import gaussian_kde
from pathlib import Path
import warnings, gc
warnings.filterwarnings("ignore")

# ── paths ─────────────────────────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config import OUTPUT_ROOT  # noqa: E402

ODIR = OUTPUT_ROOT / "simultaneousbids"
BID  = str(ODIR / "bid_level_simult.parquet")
TDIR = ODIR / "tables"; FDIR = ODIR / "figures"
TDIR.mkdir(parents=True, exist_ok=True); FDIR.mkdir(parents=True, exist_ok=True)

SIZE_ORDER  = ["micro", "small", "medium", "large"]
SIZE_COLORS = {"micro": "#4daf4a", "small": "#377eb8",
               "medium": "#ff7f00", "large": "#e41a1c"}
SIZE_LABELS = {"micro": "Micro", "small": "Small",
               "medium": "Medium", "large": "Large"}

# ── load & clean ──────────────────────────────────────────────────────────────
print("Loading …")
df = pd.read_parquet(BID, columns=[
    "rut_bidder", "ym", "tender_id",
    "monto_estimado", "is_selected",
    "size_group", "post"
])
df = df.dropna(subset=["monto_estimado", "size_group"])
df = df[df["monto_estimado"] > 0]
df["size_group"] = df["size_group"].astype(str)
df = df[df["size_group"].isin(SIZE_ORDER)]
df["monto_M"]  = df["monto_estimado"] / 1e6      # CLP millions
df["period"]   = np.where(df["post"] == 1, "Post-reform", "Pre-reform")
df["is_sel"]   = df["is_selected"].astype(int)
print(f"  {len(df):,} rows")

# winsorise monto at 99th pct for plots (preserve for table computation)
P99 = df["monto_M"].quantile(0.99)
print(f"  monto_M p99 = {P99:,.0f} M CLP  ({P99*1e6/1e9:.1f} billion CLP)")
df["monto_M_w"] = df["monto_M"].clip(upper=P99)   # winsorised version for plots

# ── firm-month aggregation ────────────────────────────────────────────────────
print("Aggregating to firm-month …")
key = ["rut_bidder", "ym", "size_group", "period"]

# total bids per firm-month
fm_all = (df.groupby(key)
            .agg(n_bids       = ("tender_id", "count"),
                 n_won        = ("is_sel",    "sum"),
                 monto_bid_M  = ("monto_M_w", "sum"),)   # total portfolio value
            .reset_index())

# won value per firm-month (from winner rows only)
won = df[df["is_sel"] == 1]
fm_won_val = (won.groupby(key)["monto_M_w"]
                  .sum()
                  .rename("monto_won_M")
                  .reset_index())

fm = fm_all.merge(fm_won_val, on=key, how="left")
fm["monto_won_M"] = fm["monto_won_M"].fillna(0.0)
fm["prob_win_fm"] = fm["n_won"] / fm["n_bids"]
print(f"  {len(fm):,} firm-month rows")

# ── SUMMARY TABLE ─────────────────────────────────────────────────────────────
print("\nBuilding summary table …")
rows = []
for period in ["Pre-reform", "Post-reform"]:
    for sg in SIZE_ORDER + ["All"]:
        if sg == "All":
            sub_fm = fm[fm["period"] == period]
            sub_df = df[df["period"] == period]
        else:
            sub_fm = fm[(fm["period"] == period) & (fm["size_group"] == sg)]
            sub_df = df[(df["period"] == period) & (df["size_group"] == sg)]
        rows.append({
            "period":              period,
            "size_group":          sg,
            "n_firm_months":       len(sub_fm),
            "mean_clp_won_M":      round(sub_fm["monto_won_M"].mean(), 1),
            "median_clp_won_M":    round(sub_fm["monto_won_M"].median(), 1),
            "mean_n_won":          round(sub_fm["n_won"].mean(), 3),
            "prob_win_pct":        round(sub_df["is_sel"].mean() * 100, 2),
        })

tbl = pd.DataFrame(rows)
tbl.to_csv(TDIR / "t19_clp_won_by_size_period.csv", index=False)
print(tbl.to_string(index=False))

# ── KDE helper ────────────────────────────────────────────────────────────────
def kde_log_plot(ax, series, label, color, bw=0.35, alpha=1.0):
    """KDE of log10(values > 0)."""
    v = np.log10(series[series > 0.01])
    if len(v) < 50:
        return
    lo, hi = v.quantile(0.01), v.quantile(0.995)
    kde = gaussian_kde(v, bw_method=bw)
    xs  = np.linspace(lo, hi, 500)
    ax.plot(xs, kde(xs), label=label, color=color, lw=2, alpha=alpha)

def format_x(ax):
    ax.set_xticks([0, 1, 2, 3, 4, 5])
    ax.set_xticklabels(["1", "10", "100", "1K", "10K", "100K"])
    ax.set_xlabel("CLP won per month (millions)", fontsize=10)
    ax.grid(axis="y", alpha=0.25)
    ax.set_xlim(-0.3, 5.5)

# ── FIGURE F7: all bidders (firm-months, incl. zeros) ─────────────────────────
print("\nFigure F7: all firm-months …")
fig, axes = plt.subplots(1, 2, figsize=(12, 5), sharey=False)
fig.suptitle("Distribution of CLP Won per Month — All Firm-Months\n"
             "(KDE of log₁₀ scale; includes months with zero wins; monto_estimado winsorised at p99)",
             fontsize=11, fontweight="bold")

for ax, period in zip(axes, ["Pre-reform", "Post-reform"]):
    sub = fm[fm["period"] == period]
    for sg in SIZE_ORDER:
        kde_log_plot(ax, sub.loc[sub["size_group"]==sg, "monto_won_M"],
                     SIZE_LABELS[sg], SIZE_COLORS[sg])
    ax.set_title(period, fontsize=12, fontweight="bold")
    format_x(ax)
    ax.set_ylabel("Density" if ax is axes[0] else "")
    ax.legend(title="Firm size")

plt.tight_layout()
fig.savefig(FDIR / "f7_clp_won_all_bidders.png", dpi=150, bbox_inches="tight")
plt.close()
print("  ✓ f7_clp_won_all_bidders.png")

# ── FIGURE F8: winners only ────────────────────────────────────────────────────
print("Figure F8: winners only …")
fm_win = fm[fm["n_won"] > 0].copy()

fig, axes = plt.subplots(1, 2, figsize=(12, 5), sharey=False)
fig.suptitle("Distribution of CLP Won per Month — Winner Firm-Months Only\n"
             "(KDE of log₁₀ scale; monto_estimado winsorised at p99)",
             fontsize=11, fontweight="bold")

for ax, period in zip(axes, ["Pre-reform", "Post-reform"]):
    sub = fm_win[fm_win["period"] == period]
    for sg in SIZE_ORDER:
        kde_log_plot(ax, sub.loc[sub["size_group"]==sg, "monto_won_M"],
                     SIZE_LABELS[sg], SIZE_COLORS[sg])
    ax.set_title(period, fontsize=12, fontweight="bold")
    format_x(ax)
    ax.set_ylabel("Density" if ax is axes[0] else "")
    ax.legend(title="Firm size")

plt.tight_layout()
fig.savefig(FDIR / "f8_clp_won_winners_only.png", dpi=150, bbox_inches="tight")
plt.close()
print("  ✓ f8_clp_won_winners_only.png")

# ── FIGURE F9: 4-panel combined (pre/post × all/winners) ─────────────────────
print("Figure F9: 4-panel …")
fig, axes = plt.subplots(2, 2, figsize=(13, 9))
COL_TITLES = ["All firm-months\n(including zero-win months)",
              "Winner firm-months only"]
ROW_TITLES = ["Pre-reform", "Post-reform"]

for ri, period in enumerate(["Pre-reform", "Post-reform"]):
    for ci, (src, colname) in enumerate([(fm, "monto_won_M"), (fm_win, "monto_won_M")]):
        ax  = axes[ri][ci]
        sub = src[src["period"] == period]
        for sg in SIZE_ORDER:
            kde_log_plot(ax, sub.loc[sub["size_group"]==sg, colname],
                         SIZE_LABELS[sg], SIZE_COLORS[sg])
        if ri == 0:
            ax.set_title(COL_TITLES[ci], fontsize=11, fontweight="bold")
        ax.text(-0.22, 0.5, ROW_TITLES[ri], transform=ax.transAxes,
                fontsize=11, fontweight="bold", va="center", rotation=90)
        format_x(ax)
        ax.set_ylabel("Density", fontsize=9)
        ax.legend(title="Firm size", fontsize=8, title_fontsize=8)

fig.suptitle("Monthly CLP Won by Firm Size: Pre- vs. Post-reform",
             fontsize=13, fontweight="bold", y=1.01)
plt.tight_layout(rect=[0.05, 0, 1, 1])
fig.savefig(FDIR / "f9_clp_won_4panel.png", dpi=150, bbox_inches="tight")
plt.close()
print("  ✓ f9_clp_won_4panel.png")

print("\n✓ All done.")
