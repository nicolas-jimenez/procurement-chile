"""
02_descriptives.py — memory-efficient version
"""
from __future__ import annotations
from pathlib import Path
import gc

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[3]
OUT  = ROOT / "output" / "simultaneousbids"
TBLS = OUT / "tables"
FIGS = OUT / "figures"
TBLS.mkdir(parents=True, exist_ok=True)
FIGS.mkdir(parents=True, exist_ok=True)

BID_PATH = str(OUT / "bid_level_simult.parquet")
FM_PATH  = str(OUT / "firm_month_panel.parquet")

def save_fig(fig, fname):
    path = FIGS / fname
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {path.name}")

# ── Load firm-month panel (small, ~545K rows) ─────────────────────────────────
print("Loading firm-month panel…")
fm = pd.read_parquet(FM_PATH)
fm["pre_post"] = np.where(fm["post"] == 1, "Post-reform", "Pre-reform")
fm["ym_dt"]    = pd.to_datetime(fm["ym"].astype(str) + "-01")
print(f"  {len(fm):,} rows, {fm['rut_bidder'].nunique():,} firms")

# ── Load only the columns needed from bids ────────────────────────────────────
print("Loading bid-level (selected columns)…")
BID_COLS = ["tender_id","rut_bidder","ym","sector","post","same_region",
            "dist_km","n_sim","n_sim_nonlocal","n_sim_local",
            "n_sim_lag1","n_sim_nl_lag1","log_bid_ratio","bid_ratio",
            "size_group","region_buyer_n","region_bidder_n",
            "monto_estimado","n_oferentes","first_bid_in_region"]
bids = pd.read_parquet(BID_PATH, columns=BID_COLS)
bids["pre_post"] = np.where(bids["post"] == 1, "Post-reform", "Pre-reform")
print(f"  {len(bids):,} rows")

# ─────────────────────────────────────────────────────────────────────────────
# T1: Distribution of monthly simultaneous bid count
# ─────────────────────────────────────────────────────────────────────────────
print("\n[T1] n_sim distribution…")
bins   = [0, 1, 2, 3, 5, 10, 20, 50, np.inf]
labels = ["1", "2", "3", "4–5", "6–10", "11–20", "21–50", "51+"]
fm["n_sim_bin"] = pd.cut(fm["n_sim"], bins=bins, labels=labels, right=True)

t1 = pd.DataFrame({
    "n_firm_months": fm["n_sim_bin"].value_counts().sort_index(),
    "pct":           fm["n_sim_bin"].value_counts(normalize=True).sort_index() * 100,
})

fm_m = fm[fm["n_sim_munic"] > 0].copy()
fm_m["bin"] = pd.cut(fm_m["n_sim_munic"], bins=bins, labels=labels, right=True)
t1["n_fm_Munic"] = fm_m["bin"].value_counts().sort_index()

fm_o = fm[fm["n_sim_obras"] > 0].copy()
fm_o["bin"] = pd.cut(fm_o["n_sim_obras"], bins=bins, labels=labels, right=True)
t1["n_fm_Obras"] = fm_o["bin"].value_counts().sort_index()

t1.index.name = "simultaneous_bids_per_month"
t1.to_csv(TBLS / "t1_n_sim_distribution.csv")
print(t1.to_string())
del fm_m, fm_o; gc.collect()

# ─────────────────────────────────────────────────────────────────────────────
# T2: By firm size
# ─────────────────────────────────────────────────────────────────────────────
print("\n[T2] By firm size…")
t2 = (
    fm.groupby("size_group")
    .agg(
        n_obs            = ("rut_bidder", "count"),
        mean_n_sim       = ("n_sim", "mean"),
        median_n_sim     = ("n_sim", "median"),
        p75_n_sim        = ("n_sim", lambda x: x.quantile(0.75)),
        p90_n_sim        = ("n_sim", lambda x: x.quantile(0.90)),
        p99_n_sim        = ("n_sim", lambda x: x.quantile(0.99)),
        share_gt1        = ("n_sim", lambda x: (x > 1).mean() * 100),
        share_gt5        = ("n_sim", lambda x: (x > 5).mean() * 100),
        mean_share_nonloc= ("share_nonlocal", "mean"),
        mean_n_regions   = ("n_regions_bid", "mean"),
        mean_avg_dist_km = ("avg_dist_km", "mean"),
    ).round(2)
)
t2.index.name = "size_group"
t2.to_csv(TBLS / "t2_n_sim_by_size.csv")
print(t2.to_string())

# ─────────────────────────────────────────────────────────────────────────────
# T3: By bidder home region
# ─────────────────────────────────────────────────────────────────────────────
print("\n[T3] By bidder home region…")
t3 = (
    fm.groupby("region_bidder_n")
    .agg(
        n_firm_months    = ("rut_bidder", "count"),
        n_firms          = ("rut_bidder", "nunique"),
        mean_n_sim       = ("n_sim", "mean"),
        median_n_sim     = ("n_sim", "median"),
        mean_share_nonloc= ("share_nonlocal", "mean"),
        mean_avg_dist_km = ("avg_dist_km", "mean"),
    ).round(2).sort_values("mean_n_sim", ascending=False)
)
t3.index.name = "bidder_region"
t3.to_csv(TBLS / "t3_n_sim_by_region.csv")
print(t3.to_string())

# ─────────────────────────────────────────────────────────────────────────────
# T4: By sector — aggregate first to firm-month for meaningful means
# ─────────────────────────────────────────────────────────────────────────────
print("\n[T4] By sector…")
# Unique tender-level stats (one row per tender-firm)
t4_rows = []
for sec in ["All", "Municipalidades", "Obras Públicas"]:
    sub = bids if sec == "All" else bids[bids["sector"] == sec]
    # firm-month deduplicated view for n_sim stats
    sub_fm = sub.drop_duplicates(subset=["rut_bidder", "ym"])
    for pp in ["Pre-reform", "Post-reform", "All"]:
        s  = sub    if pp == "All" else sub[sub["pre_post"] == pp]
        sf = sub_fm if pp == "All" else sub_fm[sub_fm["pre_post"] == pp]
        t4_rows.append({
            "sector": sec, "period": pp,
            "n_bid_rows":             len(s),
            "n_tenders":              s["tender_id"].nunique(),
            "n_firms":                s["rut_bidder"].nunique(),
            "mean_n_sim_per_fm":      sf["n_sim"].mean(),
            "median_n_sim_per_fm":    sf["n_sim"].median(),
            "mean_nonlocal_share_fm": sf["n_sim_nonlocal"].div(sf["n_sim"].where(sf["n_sim"]>0)).mean(),
            "share_same_region_bids": s["same_region"].mean(),
            "mean_log_bid_ratio":     s["log_bid_ratio"].mean(),
        })
t4 = pd.DataFrame(t4_rows).set_index(["sector","period"]).round(3)
t4.to_csv(TBLS / "t4_n_sim_by_sector.csv")
print(t4.to_string())

# ─────────────────────────────────────────────────────────────────────────────
# T5: Geographic distribution for non-local bids
# ─────────────────────────────────────────────────────────────────────────────
print("\n[T5] Distance distribution for non-local bids…")
nonlocal_bids = bids[(bids["same_region"] == 0.0) & bids["dist_km"].notna()].copy()

dist_bins   = [0, 100, 200, 400, 700, 1500, np.inf]
dist_labels = ["0–100 km", "101–200 km", "201–400 km", "401–700 km", "701–1500 km", "1500+ km"]
nonlocal_bids["dist_bin"] = pd.cut(nonlocal_bids["dist_km"], bins=dist_bins,
                               labels=dist_labels, right=True)

t5_all  = nonlocal_bids["dist_bin"].value_counts().sort_index()
t5_munic = nonlocal_bids[nonlocal_bids["sector"]=="Municipalidades"]["dist_bin"].value_counts().sort_index()
t5_obras = nonlocal_bids[nonlocal_bids["sector"]=="Obras Públicas"]["dist_bin"].value_counts().sort_index()
sim2     = nonlocal_bids[nonlocal_bids["n_sim"] > 1]
t5_sim  = sim2["dist_bin"].value_counts().sort_index()

t5 = pd.DataFrame({
    "n_all":       t5_all,
    "pct_all":     t5_all / t5_all.sum() * 100,
    "n_sim_gt1":   t5_sim,
    "pct_sim_gt1": t5_sim / t5_sim.sum() * 100,
    "n_munic":     t5_munic,
    "n_obras":     t5_obras,
}).round(1)
t5.index.name = "distance_range"
t5.to_csv(TBLS / "t5_geo_dist_nonlocal.csv")
print(t5.to_string())
del nonlocal_bids, sim2; gc.collect()

# ─────────────────────────────────────────────────────────────────────────────
# T6: Pre/post reform comparison (balanced windows)
# ─────────────────────────────────────────────────────────────────────────────
print("\n[T6] Pre/post reform…")
fm_bal = fm[
    ((fm["ym"] >= "2023-01") & (fm["ym"] <= "2024-11")) |
    ((fm["ym"] >= "2025-01") & (fm["ym"] <= "2025-12"))
].copy()

t6_rows = []
for pp in ["Pre-reform", "Post-reform"]:
    for sec_label, sec_col in [
        ("All", None),
        ("Municipalidades", "n_sim_munic"),
        ("Obras Públicas",  "n_sim_obras"),
    ]:
        sub = fm_bal[fm_bal["pre_post"] == pp]
        if sec_col:
            sub = sub[sub[sec_col] > 0]
        t6_rows.append({
            "period": pp, "sector": sec_label,
            "n_firm_months":         len(sub),
            "n_firms":               sub["rut_bidder"].nunique(),
            "mean_n_sim":            sub["n_sim"].mean(),
            "median_n_sim":          sub["n_sim"].median(),
            "mean_nonlocal_share":   sub["share_nonlocal"].mean(),
            "mean_n_regions":        sub["n_regions_bid"].mean(),
            "mean_avg_dist_km":      sub["avg_dist_km"].mean(),
        })
t6 = pd.DataFrame(t6_rows).set_index(["sector","period"]).round(3)
t6.to_csv(TBLS / "t6_pre_post_reform.csv")
print(t6.to_string())
del fm_bal; gc.collect()

# ─────────────────────────────────────────────────────────────────────────────
# T7: By size × sector
# ─────────────────────────────────────────────────────────────────────────────
print("\n[T7] By size × sector…")
sub7 = bids[bids["sector"].isin(["Municipalidades","Obras Públicas"])].copy()
t7 = (
    sub7.groupby(["sector","size_group"])
    .agg(
        n_bids             = ("tender_id", "count"),
        n_firms            = ("rut_bidder", "nunique"),
        mean_n_sim         = ("n_sim", "mean"),
        median_n_sim       = ("n_sim", "median"),
        share_nonlocal     = ("same_region", lambda x: (x == 0).mean()),
        mean_log_bid_ratio = ("log_bid_ratio", "mean"),
    ).round(3)
)
t7.to_csv(TBLS / "t7_n_sim_by_size_sector.csv")
print(t7.to_string())
del sub7; gc.collect()

# ─────────────────────────────────────────────────────────────────────────────
# FIGURES
# ─────────────────────────────────────────────────────────────────────────────
print("\n[F1] Distribution histogram…")
fig, ax = plt.subplots(figsize=(8,4))
vals = fm["n_sim"].clip(upper=25)
ax.hist(vals, bins=range(1,27), edgecolor="white", color="#4C72B0", alpha=0.85)
ax.set_xlabel("Simultaneous bids per firm-month (capped at 25)", fontsize=11)
ax.set_ylabel("Firm–month observations", fontsize=11)
ax.set_title("Distribution of simultaneous bid count per firm-month", fontsize=12)
ax.set_yscale("log")
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x,_: f"{int(x):,}"))
fig.tight_layout(); save_fig(fig, "f1_dist_n_sim.png")

print("[F2] By region bar chart…")
t3_p = t3.dropna(subset=["mean_n_sim"]).sort_values("mean_n_sim")
fig, ax = plt.subplots(figsize=(9,5))
ax.barh(t3_p.index, t3_p["mean_n_sim"], color="#4C72B0", alpha=0.85)
ax.set_xlabel("Mean simultaneous bids per firm-month", fontsize=11)
ax.set_title("Mean simultaneous bids by bidder home region", fontsize=12)
ax.axvline(fm["n_sim"].mean(), color="red", ls="--", lw=1.2, label="Overall mean")
ax.legend(); fig.tight_layout(); save_fig(fig, "f2_n_sim_by_region.png")

print("[F3] Geographic spread…")
t5_r = pd.read_csv(TBLS / "t5_geo_dist_nonlocal.csv", index_col=0)
fig, ax = plt.subplots(figsize=(8,4))
x = np.arange(len(t5_r))
w = 0.35
ax.bar(x-w/2, t5_r["pct_all"],     width=w, label="All non-local bids",    color="#4C72B0", alpha=0.85)
ax.bar(x+w/2, t5_r["pct_sim_gt1"], width=w, label="n_sim > 1",             color="#C44E52", alpha=0.85)
ax.set_xticks(x); ax.set_xticklabels(t5_r.index, fontsize=8)
ax.set_ylabel("% of non-local bids", fontsize=11)
ax.set_title("Geographic distribution of non-local bids", fontsize=12)
ax.legend(); fig.tight_layout(); save_fig(fig, "f3_geo_spread_nonlocal.png")

print("[F4] Pre/post…")
t6_r = pd.read_csv(TBLS / "t6_pre_post_reform.csv")
secs  = ["All","Municipalidades","Obras Públicas"]
pre   = [t6_r[(t6_r["sector"]==s)&(t6_r["period"]=="Pre-reform")]["mean_n_sim"].values[0] for s in secs]
post  = [t6_r[(t6_r["sector"]==s)&(t6_r["period"]=="Post-reform")]["mean_n_sim"].values[0] for s in secs]
x = np.arange(len(secs))
fig, ax = plt.subplots(figsize=(7,4))
ax.bar(x-0.2, pre,  width=0.35, label="Pre-reform",  color="#4C72B0", alpha=0.85)
ax.bar(x+0.2, post, width=0.35, label="Post-reform", color="#C44E52", alpha=0.85)
ax.set_xticks(x); ax.set_xticklabels(secs)
ax.set_ylabel("Mean simultaneous bids per firm-month", fontsize=11)
ax.set_title("Pre vs. post-reform simultaneous bidding", fontsize=12)
ax.legend(); fig.tight_layout(); save_fig(fig, "f4_pre_post_n_sim.png")

print("[F5] Time series…")
ts = (
    fm.groupby("ym_dt")
    .agg(mean_n_sim=("n_sim","mean"), n_fm=("rut_bidder","count"))
    .reset_index()
)
ts_m = (fm[fm["n_sim_munic"]>0].groupby("ym_dt")["n_sim_munic"].mean()
        .reset_index().rename(columns={"n_sim_munic":"mean_munic"}))
ts_o = (fm[fm["n_sim_obras"]>0].groupby("ym_dt")["n_sim_obras"].mean()
        .reset_index().rename(columns={"n_sim_obras":"mean_obras"}))
ts = ts.merge(ts_m, on="ym_dt", how="left").merge(ts_o, on="ym_dt", how="left")
fig, ax = plt.subplots(figsize=(10,4))
ax.plot(ts["ym_dt"], ts["mean_n_sim"],   label="All sectors",     color="#4C72B0", lw=1.8)
ax.plot(ts["ym_dt"], ts["mean_munic"],   label="Municipalidades", color="#55A868", lw=1.8, ls="--")
ax.plot(ts["ym_dt"], ts["mean_obras"],   label="Obras Públicas",  color="#C44E52", lw=1.8, ls=":")
ax.axvline(pd.Timestamp("2024-12-12"), color="black", lw=1.2, label="Reform")
ax.set_xlabel("Month"); ax.set_ylabel("Mean sim. bids / firm-month", fontsize=11)
ax.set_title("Monthly mean simultaneous bids", fontsize=12)
ax.legend(fontsize=9); fig.tight_layout(); save_fig(fig, "f5_n_sim_time_series.png")

print("[F6] Size × sector heatmap…")
t7_r = pd.read_csv(TBLS / "t7_n_sim_by_size_sector.csv")
t7_r.columns = t7_r.columns.str.strip()
pivot = t7_r.pivot(index="size_group", columns="sector", values="mean_n_sim")
order = [g for g in ["micro","small","medium","large"] if g in pivot.index]
pivot = pivot.reindex(order)
fig, ax = plt.subplots(figsize=(6,4))
im = ax.imshow(pivot.values, aspect="auto", cmap="YlOrRd")
ax.set_xticks(range(len(pivot.columns))); ax.set_xticklabels(pivot.columns)
ax.set_yticks(range(len(pivot.index))); ax.set_yticklabels(pivot.index)
for i in range(len(pivot.index)):
    for j in range(len(pivot.columns)):
        v = pivot.values[i,j]
        if not np.isnan(v):
            ax.text(j, i, f"{v:.1f}", ha="center", va="center", fontsize=10)
plt.colorbar(im, ax=ax, label="Mean sim bids (bid-row weighted)")
ax.set_title("Simultaneous bids by firm size and sector", fontsize=11)
fig.tight_layout(); save_fig(fig, "f6_n_sim_by_size_sector.png")

print("\nAll descriptives complete.")
