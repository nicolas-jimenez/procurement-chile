"""
clean/09_quarterly_trends.py
─────────────────────────────────────────────────────────────────────────────
Quarterly trend analysis: how procurement activity (# tenders, total value,
average value) has changed over time, split by dataset and by sub-categories.

Input:  data/clean/combined_sii_merged_filtered.parquet
Figures (all → data/diagnostics/figures/):
  trends_01_count_by_dataset.png
  trends_02_value_by_dataset.png
  trends_03_count_by_sector.png     (licitaciones only — has sector)
  trends_04_value_by_sector.png     (licitaciones only)
  trends_05_count_by_tamano.png     (compra ágil only — has Tamano)
  trends_06_avg_value.png           — avg value per tender by dataset
  trends_06b_avg_value_compra_agil.png — avg value per cotización (compra ágil only)
  trends_06c_value_compra_agil_p25_p75.png — mean with P25–P75 whiskers (compra ágil)
  trends_07_share_above_500utm.png  — share above 500 UTM (Obras Públicas, licitaciones)
  trends_08_count_by_region_compra_agil.png  — compra ágil count by buyer region
  trends_09_value_by_region_compra_agil.png  — compra ágil value by buyer region
  trends_10_compra_agil_bidders_size_region_sii.png — bidder composition counts (compra ágil)
  trends_summary.png                — 2x3 summary panel
"""

from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick

ROOT     = Path(__file__).resolve().parents[2]
IN_FILE  = ROOT / "data" / "clean" / "combined_sii_merged_filtered.parquet"
UTM_FILE = ROOT / "data" / "raw" / "other" / "utm_clp_2022_2025.csv"
FIG_DIR  = ROOT / "data" / "diagnostics" / "figures"
FIG_DIR.mkdir(parents=True, exist_ok=True)

DS_COLORS = {"licitaciones": "#1f77b4", "compra_agil": "#d62728"}
DS_LABELS = {"licitaciones": "Licitaciones", "compra_agil": "Compra Ágil"}
DS_STYLES = {"licitaciones": "-", "compra_agil": "--"}
xtick_kw  = dict(rotation=45, ha="right", fontsize=7)

OBRA_SECTOR = "Obras Públicas"
UTM_THRESH  = 500

# ══════════════════════════════════════════════════════════════════════════
print("=" * 70)
print("STEP 1 — Loading data")
print("=" * 70)

df = pd.read_parquet(IN_FILE, columns=[
    "dataset", "tender_id",
    "fecha_pub", "source_year", "source_month",
    "monto_estimado", "is_key_dup",
    "tipo", "sector", "tamano", "region_buyer",
])
print(f"  Rows: {len(df):,}")
df["fecha_pub"] = pd.to_datetime(df["fecha_pub"], errors="coerce")
df["quarter"]   = df["fecha_pub"].dt.to_period("Q")
df = df[~df["is_key_dup"]].copy()

# Tender-level dedup (one row per unique tender, keeping first)
tender = df.drop_duplicates(["dataset","tender_id"]).copy()
print(f"  Unique tenders: {len(tender):,}")
print(f"  By dataset:\n{tender['dataset'].value_counts().to_string()}")

all_quarters = sorted(tender["quarter"].dropna().unique())
all_q_str    = [str(q) for q in all_quarters]
xi           = range(len(all_q_str))

# ── UTM for Obras 500-UTM threshold ──────────────────────────────────────
utm_tbl = pd.read_csv(UTM_FILE)
utm_tbl = utm_tbl.rename(columns={"month_num":"source_month","utm_clp":"utm_clp_rate"})
utm_tbl["source_year"]  = utm_tbl["year"].astype(int)
utm_tbl["source_month"] = utm_tbl["source_month"].astype(int)

def savefig(fname):
    plt.tight_layout()
    plt.savefig(FIG_DIR / fname, dpi=150, bbox_inches="tight")
    print(f"  Saved: {fname}")
    plt.close()


def _first_nonnull(s: pd.Series):
    s = s.dropna()
    if len(s) == 0:
        return np.nan
    return s.iloc[0]


def _build_bidder_id(df: pd.DataFrame) -> pd.Series:
    idx = df.index
    rut_num = pd.to_numeric(df.get("rut_bidder"), errors="coerce").round().astype("Int64")
    dv = (
        df.get("dv_bidder", pd.Series(pd.NA, index=idx, dtype="string"))
        .astype("string")
        .str.strip()
        .str.upper()
        .str.extract(r"([0-9K])")[0]
    )
    raw = df.get("rut_bidder_raw", pd.Series(pd.NA, index=idx, dtype="string")).astype("string")
    raw_clean = raw.str.strip().str.upper().str.replace(r"[^0-9K]", "", regex=True)
    raw_rut = pd.to_numeric(raw_clean.str.extract(r"^(\d+)")[0], errors="coerce").round().astype("Int64")
    raw_dv = raw_clean.str.extract(r"([0-9K])$")[0]
    rut_num = rut_num.where(rut_num.notna(), raw_rut)
    dv = dv.where(dv.notna(), raw_dv)
    bidder_id = rut_num.astype("string") + "-" + dv.astype("string")
    bad = rut_num.isna() | dv.isna()
    if bad.any():
        synthetic = "__missing_bidder_" + pd.Series(np.arange(len(df)), index=idx).astype(str)
        bidder_id = bidder_id.copy()
        bidder_id.loc[bad] = synthetic.loc[bad].astype("string")
    return bidder_id.astype("string")

# ══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("STEP 2 — Quarterly trend plots")
print("=" * 70)

# ── 01: Count by dataset ──────────────────────────────────────────────────
fig, axes = plt.subplots(1, 2, figsize=(16, 5))

ax = axes[0]
for ds in ["licitaciones","compra_agil"]:
    vals = (tender[tender["dataset"]==ds]
                .groupby("quarter")["tender_id"]
                .nunique()
                .reindex(all_quarters)
                .fillna(0))
    ax.plot(list(xi), vals, marker="o", ms=4, lw=2,
            color=DS_COLORS[ds], ls=DS_STYLES[ds], label=DS_LABELS[ds])
ax.set_title("Unique tenders/cotizaciones per quarter", fontweight="bold")
ax.set_xlabel("Quarter"); ax.set_ylabel("Count")
ax.set_xticks(list(xi)); ax.set_xticklabels(all_q_str, **xtick_kw)
ax.yaxis.set_major_formatter(mtick.FuncFormatter(lambda x,_: f"{int(x):,}"))
ax.legend(fontsize=9); ax.grid(axis="y", alpha=0.3); ax.spines[["top","right"]].set_visible(False)

ax = axes[1]
for ds in ["licitaciones","compra_agil"]:
    lic_v = (tender[tender["dataset"]==ds]
                 .groupby("quarter")["tender_id"]
                 .nunique()
                 .reindex(all_quarters)
                 .fillna(0))
    baseline = lic_v.iloc[0] if lic_v.iloc[0] != 0 else 1
    ax.plot(list(xi), lic_v / baseline * 100,
            marker="o", ms=4, lw=2,
            color=DS_COLORS[ds], ls=DS_STYLES[ds], label=DS_LABELS[ds])
ax.axhline(100, color="grey", lw=1, ls=":", alpha=0.7)
ax.set_title("Tender count: index (2022Q1 = 100)", fontweight="bold")
ax.set_xlabel("Quarter"); ax.set_ylabel("Index")
ax.set_xticks(list(xi)); ax.set_xticklabels(all_q_str, **xtick_kw)
ax.legend(fontsize=9); ax.grid(axis="y", alpha=0.3); ax.spines[["top","right"]].set_visible(False)

fig.suptitle("Procurement activity by number of tenders — quarterly trends",
             fontsize=12, fontweight="bold")
savefig("trends_01_count_by_dataset.png")

# ── 02: Value by dataset ──────────────────────────────────────────────────
fig, axes = plt.subplots(1, 2, figsize=(16, 5))

ax = axes[0]
for ds in ["licitaciones","compra_agil"]:
    vals = (tender[tender["dataset"]==ds]
                .groupby("quarter")["monto_estimado"]
                .sum()
                .reindex(all_quarters)
                .fillna(0) / 1e9)
    ax.plot(list(xi), vals, marker="o", ms=4, lw=2,
            color=DS_COLORS[ds], ls=DS_STYLES[ds], label=DS_LABELS[ds])
ax.set_title("Total estimated budget per quarter (bn CLP)", fontweight="bold")
ax.set_xlabel("Quarter"); ax.set_ylabel("Billion CLP")
ax.set_xticks(list(xi)); ax.set_xticklabels(all_q_str, **xtick_kw)
ax.legend(fontsize=9); ax.grid(axis="y", alpha=0.3); ax.spines[["top","right"]].set_visible(False)

ax = axes[1]
for ds in ["licitaciones","compra_agil"]:
    vals = (tender[tender["dataset"]==ds]
                .groupby("quarter")["monto_estimado"]
                .sum()
                .reindex(all_quarters)
                .fillna(0) / 1e9)
    baseline = vals.iloc[0] if vals.iloc[0] != 0 else 1
    ax.plot(list(xi), vals / baseline * 100,
            marker="o", ms=4, lw=2,
            color=DS_COLORS[ds], ls=DS_STYLES[ds], label=DS_LABELS[ds])
ax.axhline(100, color="grey", lw=1, ls=":", alpha=0.7)
ax.set_title("Total budget: index (2022Q1 = 100)", fontweight="bold")
ax.set_xlabel("Quarter"); ax.set_ylabel("Index")
ax.set_xticks(list(xi)); ax.set_xticklabels(all_q_str, **xtick_kw)
ax.legend(fontsize=9); ax.grid(axis="y", alpha=0.3); ax.spines[["top","right"]].set_visible(False)

fig.suptitle("Procurement activity by budget value — quarterly trends",
             fontsize=12, fontweight="bold")
savefig("trends_02_value_by_dataset.png")

# ── 03: Count by sector (licitaciones only) ───────────────────────────────
lic = tender[tender["dataset"]=="licitaciones"].copy()
fig, ax = plt.subplots(figsize=(13, 5))
sector_colors = ["#1f77b4","#ff7f0e","#2ca02c","#d62728","#9467bd","#8c564b","#e377c2","#17becf"]
top_sectors = (lic["sector"].value_counts().head(7).index.tolist() + [None])
for i, sec in enumerate(lic["sector"].value_counts().head(8).index):
    sub = lic[lic["sector"]==sec].groupby("quarter")["tender_id"].nunique().reindex(all_quarters).fillna(0)
    ax.plot(list(xi), sub, marker="o", ms=3, lw=2, color=sector_colors[i%len(sector_colors)], label=sec)
ax.set_title("Licitaciones: tender count by sector per quarter", fontweight="bold")
ax.set_xlabel("Quarter"); ax.set_ylabel("Count")
ax.set_xticks(list(xi)); ax.set_xticklabels(all_q_str, **xtick_kw)
ax.yaxis.set_major_formatter(mtick.FuncFormatter(lambda x,_: f"{int(x):,}"))
ax.legend(fontsize=8, bbox_to_anchor=(1.01,1), loc="upper left")
ax.grid(axis="y", alpha=0.3); ax.spines[["top","right"]].set_visible(False)
fig.suptitle("Licitaciones: tender volume by sector over time", fontsize=12, fontweight="bold")
savefig("trends_03_count_by_sector.png")

# ── 04: Value by sector (licitaciones only) ────────────────────────────────
fig, ax = plt.subplots(figsize=(13, 5))
for i, sec in enumerate(lic["sector"].value_counts().head(8).index):
    sub = lic[lic["sector"]==sec].groupby("quarter")["monto_estimado"].sum().reindex(all_quarters).fillna(0) / 1e9
    ax.plot(list(xi), sub, marker="o", ms=3, lw=2, color=sector_colors[i%len(sector_colors)], label=sec)
ax.set_title("Licitaciones: total budget by sector per quarter (bn CLP)", fontweight="bold")
ax.set_xlabel("Quarter"); ax.set_ylabel("Billion CLP")
ax.set_xticks(list(xi)); ax.set_xticklabels(all_q_str, **xtick_kw)
ax.legend(fontsize=8, bbox_to_anchor=(1.01,1), loc="upper left")
ax.grid(axis="y", alpha=0.3); ax.spines[["top","right"]].set_visible(False)
fig.suptitle("Licitaciones: total budget by sector over time", fontsize=12, fontweight="bold")
savefig("trends_04_value_by_sector.png")

# ── 05: Count by Tamano (compra ágil only) ────────────────────────────────
ca = tender[tender["dataset"]=="compra_agil"].copy()
fig, ax = plt.subplots(figsize=(12, 5))
for label, color in [("MiPyme","#1f77b4"),("Grande","#d62728")]:
    sub = ca[ca["tamano"]==label].groupby("quarter")["tender_id"].nunique().reindex(all_quarters).fillna(0)
    ax.plot(list(xi), sub, marker="o", ms=4, lw=2, color=color, label=label)
ax.set_title("Compra Ágil: cotizaciones by bidder size (Tamano)", fontweight="bold")
ax.set_xlabel("Quarter"); ax.set_ylabel("Count")
ax.set_xticks(list(xi)); ax.set_xticklabels(all_q_str, **xtick_kw)
ax.yaxis.set_major_formatter(mtick.FuncFormatter(lambda x,_: f"{int(x):,}"))
ax.legend(fontsize=9); ax.grid(axis="y", alpha=0.3); ax.spines[["top","right"]].set_visible(False)
fig.suptitle("Compra Ágil: cotización volume by Tamano", fontsize=12, fontweight="bold")
savefig("trends_05_count_by_tamano.png")

# ── 06: Average value per tender ─────────────────────────────────────────
fig, ax = plt.subplots(figsize=(12, 5))
for ds in ["licitaciones","compra_agil"]:
    sub = tender[tender["dataset"]==ds]
    total = sub.groupby("quarter")["monto_estimado"].sum().reindex(all_quarters).fillna(np.nan)
    count = sub.groupby("quarter")["tender_id"].nunique().reindex(all_quarters).replace(0, np.nan)
    avg   = (total / count) / 1e6  # millions CLP
    ax.plot(list(xi), avg, marker="o", ms=4, lw=2,
            color=DS_COLORS[ds], ls=DS_STYLES[ds], label=DS_LABELS[ds])
ax.set_title("Average budget per tender/cotización (million CLP)", fontweight="bold")
ax.set_xlabel("Quarter"); ax.set_ylabel("Avg budget (mm CLP)")
ax.set_xticks(list(xi)); ax.set_xticklabels(all_q_str, **xtick_kw)
ax.legend(fontsize=9); ax.grid(axis="y", alpha=0.3); ax.spines[["top","right"]].set_visible(False)
fig.suptitle("Average tender value over time by dataset", fontsize=12, fontweight="bold")
savefig("trends_06_avg_value.png")

# ── 06b: Average value per cotización (compra ágil only) ─────────────────
fig, ax = plt.subplots(figsize=(12, 5))
ca_avg = tender[tender["dataset"] == "compra_agil"].copy()
ca_total = ca_avg.groupby("quarter")["monto_estimado"].sum().reindex(all_quarters).fillna(np.nan)
ca_count = ca_avg.groupby("quarter")["tender_id"].nunique().reindex(all_quarters).replace(0, np.nan)
ca_mean = (ca_total / ca_count) / 1e6  # millions CLP

ax.plot(list(xi), ca_mean, marker="o", ms=5, lw=2, color="#d62728", label="Compra Ágil")
ax.set_title("Compra Ágil: average budget per cotización (million CLP)", fontweight="bold")
ax.set_xlabel("Quarter"); ax.set_ylabel("Avg budget (mm CLP)")
ax.set_xticks(list(xi)); ax.set_xticklabels(all_q_str, **xtick_kw)
ax.legend(fontsize=9)
ax.grid(axis="y", alpha=0.3); ax.spines[["top","right"]].set_visible(False)
fig.suptitle("Average cotización value over time — Compra Ágil", fontsize=12, fontweight="bold")
savefig("trends_06b_avg_value_compra_agil.png")

# ── 06c: Compra Ágil value distribution (P25–P75 whiskers + mean) ───────
fig, ax = plt.subplots(figsize=(12, 5))
ca_dist = (
    ca_avg.groupby("quarter")["monto_estimado"]
    .agg(
        mean="mean",
        p25=lambda s: s.quantile(0.25),
        p75=lambda s: s.quantile(0.75),
    )
    .reindex(all_quarters)
)
ca_dist_mm = ca_dist / 1e6  # million CLP
y = ca_dist_mm["mean"].to_numpy()
yerr_low = (ca_dist_mm["mean"] - ca_dist_mm["p25"]).to_numpy()
yerr_high = (ca_dist_mm["p75"] - ca_dist_mm["mean"]).to_numpy()

ax.errorbar(
    list(xi),
    y,
    yerr=np.vstack([yerr_low, yerr_high]),
    fmt="o",
    color="#d62728",
    ecolor="#8c8c8c",
    elinewidth=2,
    capsize=4,
    ms=5,
    label="Mean with P25–P75 whiskers",
)
ax.plot(list(xi), y, color="#d62728", lw=1.5, alpha=0.8)
ax.set_title("Compra Ágil: tender value distribution (P25–P75, million CLP)", fontweight="bold")
ax.set_xlabel("Quarter")
ax.set_ylabel("Value (mm CLP)")
ax.set_xticks(list(xi))
ax.set_xticklabels(all_q_str, **xtick_kw)
ax.legend(fontsize=9)
ax.grid(axis="y", alpha=0.3)
ax.spines[["top", "right"]].set_visible(False)
fig.suptitle("Compra Ágil tender values over time: mean and 25th–75th percentile range",
             fontsize=12, fontweight="bold")
savefig("trends_06c_value_compra_agil_p25_p75.png")

# ── 07: Share above 500 UTM for Obras Públicas (licitaciones) ─────────────
obras = lic[lic["sector"]==OBRA_SECTOR].copy()
obras = obras.merge(utm_tbl[["source_year","source_month","utm_clp_rate"]],
                    on=["source_year","source_month"], how="left")
obras["monto_utm"] = obras["monto_estimado"] / obras["utm_clp_rate"]
obras["above_500"] = obras["monto_utm"] >= UTM_THRESH

above_q = obras[obras["above_500"]].groupby("quarter")["tender_id"].nunique().reindex(all_quarters).fillna(0)
total_q = obras.groupby("quarter")["tender_id"].nunique().reindex(all_quarters).replace(0, np.nan)
share_q = above_q / total_q * 100

fig, axes = plt.subplots(1, 2, figsize=(16, 5))
ax = axes[0]
ax.bar(list(xi), total_q.fillna(0), color="#9467bd", alpha=0.5, label="All Obras")
ax.bar(list(xi), above_q, color="#d62728", alpha=0.85, label="≥500 UTM")
ax.set_title("Obras Públicas: count by UTM tier", fontweight="bold")
ax.set_xticks(list(xi)); ax.set_xticklabels(all_q_str, **xtick_kw)
ax.yaxis.set_major_formatter(mtick.FuncFormatter(lambda x,_: f"{int(x):,}"))
ax.legend(fontsize=9); ax.grid(axis="y", alpha=0.3); ax.spines[["top","right"]].set_visible(False)

ax = axes[1]
ax.plot(list(xi), share_q, marker="o", ms=5, lw=2, color="#d62728")
ax.axhline(50, color="grey", lw=1, ls=":", alpha=0.7)
ax.set_title("Share of Obras ≥500 UTM over time", fontweight="bold")
ax.set_xlabel("Quarter"); ax.set_ylabel("Share (%)")
ax.yaxis.set_major_formatter(mtick.PercentFormatter(decimals=0))
ax.set_ylim(0,100)
ax.set_xticks(list(xi)); ax.set_xticklabels(all_q_str, **xtick_kw)
ax.grid(axis="y", alpha=0.3); ax.spines[["top","right"]].set_visible(False)

fig.suptitle("Obras Públicas: UTM threshold composition over time", fontsize=12, fontweight="bold")
savefig("trends_07_share_above_500utm.png")

# ── 08: Count by buyer region (compra ágil only) ────────────────────────
fig, ax = plt.subplots(figsize=(14, 6))
region_colors = ["#1f77b4","#ff7f0e","#2ca02c","#d62728","#9467bd","#8c564b","#e377c2","#17becf"]
ca_regions = ca.copy()
ca_regions["region_buyer"] = ca_regions["region_buyer"].fillna("Sin región")
top_regions = ca_regions["region_buyer"].value_counts().head(8).index.tolist()
for i, region in enumerate(top_regions):
    sub = (
        ca_regions[ca_regions["region_buyer"] == region]
        .groupby("quarter")["tender_id"]
        .nunique()
        .reindex(all_quarters)
        .fillna(0)
    )
    ax.plot(
        list(xi),
        sub,
        marker="o",
        ms=3,
        lw=2,
        color=region_colors[i % len(region_colors)],
        label=region,
    )
ax.set_title("Compra Ágil: cotización count by buyer region per quarter", fontweight="bold")
ax.set_xlabel("Quarter"); ax.set_ylabel("Count")
ax.set_xticks(list(xi)); ax.set_xticklabels(all_q_str, **xtick_kw)
ax.yaxis.set_major_formatter(mtick.FuncFormatter(lambda x,_: f"{int(x):,}"))
ax.legend(fontsize=8, bbox_to_anchor=(1.01,1), loc="upper left")
ax.grid(axis="y", alpha=0.3); ax.spines[["top","right"]].set_visible(False)
fig.suptitle("Compra Ágil: cotización volume by buyer region over time", fontsize=12, fontweight="bold")
savefig("trends_08_count_by_region_compra_agil.png")

# ── 09: Value by buyer region (compra ágil only) ────────────────────────
fig, ax = plt.subplots(figsize=(14, 6))
top_regions_val = (
    ca_regions.groupby("region_buyer")["monto_estimado"]
    .sum()
    .sort_values(ascending=False)
    .head(8)
    .index
    .tolist()
)
for i, region in enumerate(top_regions_val):
    sub = (
        ca_regions[ca_regions["region_buyer"] == region]
        .groupby("quarter")["monto_estimado"]
        .sum()
        .reindex(all_quarters)
        .fillna(0) / 1e9
    )
    ax.plot(
        list(xi),
        sub,
        marker="o",
        ms=3,
        lw=2,
        color=region_colors[i % len(region_colors)],
        label=region,
    )
ax.set_title("Compra Ágil: budget by buyer region per quarter (bn CLP)", fontweight="bold")
ax.set_xlabel("Quarter"); ax.set_ylabel("Billion CLP")
ax.set_xticks(list(xi)); ax.set_xticklabels(all_q_str, **xtick_kw)
ax.legend(fontsize=8, bbox_to_anchor=(1.01,1), loc="upper left")
ax.grid(axis="y", alpha=0.3); ax.spines[["top","right"]].set_visible(False)
fig.suptitle("Compra Ágil: budget value by buyer region over time", fontsize=12, fontweight="bold")
savefig("trends_09_value_by_region_compra_agil.png")

# ── 10: Compra Ágil bidder composition counts per tender ─────────────────
print("\n  Loading bidder-level Compra Ágil rows for composition graph ...")
ca_bid = pd.read_parquet(
    IN_FILE,
    columns=[
        "dataset", "tender_id", "fecha_pub", "is_key_dup",
        "same_region", "tramoventas", "razonsocial",
        "rut_bidder", "dv_bidder", "rut_bidder_raw",
    ],
    filters=[("dataset", "=", "compra_agil")],
)
ca_bid = ca_bid[~ca_bid["is_key_dup"]].copy()
ca_bid["fecha_pub"] = pd.to_datetime(ca_bid["fecha_pub"], errors="coerce")
ca_bid["quarter"] = ca_bid["fecha_pub"].dt.to_period("Q")
ca_bid = ca_bid[ca_bid["quarter"].notna()].copy()

# One row per (tender, bidder) to avoid inflated bidder counts.
ca_bid["bidder_id"] = _build_bidder_id(ca_bid)
ca_bid["same_region_num"] = pd.to_numeric(ca_bid["same_region"], errors="coerce")
ca_bid["tramoventas_num"] = pd.to_numeric(ca_bid["tramoventas"], errors="coerce")
ca_bid = (
    ca_bid.groupby(["quarter", "tender_id", "bidder_id"], sort=False)
    .agg(
        same_region_num=("same_region_num", "first"),
        tramoventas=("tramoventas_num", "first"),
        razonsocial=("razonsocial", "first"),
    )
    .reset_index()
)

is_same = ca_bid["same_region_num"] == 1
is_diff = ca_bid["same_region_num"] == 0
is_mipyme = ca_bid["tramoventas"].between(1, 9, inclusive="both")
is_grande = ca_bid["tramoventas"].between(10, 13, inclusive="both")
is_unmatched = ca_bid["razonsocial"].isna()

ca_bid["same_mipyme"] = (is_same & is_mipyme).astype(int)
ca_bid["same_grande"] = (is_same & is_grande).astype(int)
ca_bid["diff_mipyme"] = (is_diff & is_mipyme).astype(int)
ca_bid["diff_grande"] = (is_diff & is_grande).astype(int)
ca_bid["not_merged_sii"] = is_unmatched.astype(int)

cat_cols = [
    "same_mipyme",
    "same_grande",
    "diff_mipyme",
    "diff_grande",
    "not_merged_sii",
]
cat_labels = {
    "same_mipyme": "Same region × MiPYME",
    "same_grande": "Same region × Grande",
    "diff_mipyme": "Different region × MiPYME",
    "diff_grande": "Different region × Grande",
    "not_merged_sii": "Not merged with SII",
}
cat_colors = {
    "same_mipyme": "#1f77b4",
    "same_grande": "#d62728",
    "diff_mipyme": "#9fb9dd",
    "diff_grande": "#e9a8a8",
    "not_merged_sii": "#7f7f7f",
}

ca_tender_cat = (
    ca_bid.groupby(["quarter", "tender_id"])[cat_cols]
    .sum()
    .reset_index()
)
ca_q_cat = (
    ca_tender_cat.groupby("quarter")[cat_cols]
    .mean()
    .reindex(all_quarters)
)

fig, axes = plt.subplots(2, 1, figsize=(14, 10), sharex=True)

ax = axes[0]
for col in cat_cols:
    ax.plot(
        list(xi),
        ca_q_cat[col].to_numpy(),
        marker="o",
        ms=4,
        lw=2,
        color=cat_colors[col],
        label=cat_labels[col],
    )
ax.set_title("Compra Ágil: avg # bidders per tender by region × size + SII unmatched",
             fontweight="bold")
ax.set_ylabel("Avg # bidders per tender")
ax.legend(fontsize=9, ncol=2)
ax.grid(axis="y", alpha=0.3)
ax.spines[["top", "right"]].set_visible(False)

ax = axes[1]
bottom = np.zeros(len(all_q_str))
for col in cat_cols:
    vals = ca_q_cat[col].fillna(0).to_numpy()
    ax.bar(
        list(xi),
        vals,
        bottom=bottom,
        color=cat_colors[col],
        alpha=0.85,
        label=cat_labels[col],
    )
    bottom += vals
ax.set_ylabel("Avg # bidders per tender")
ax.set_xlabel("Quarter")
ax.set_xticks(list(xi))
ax.set_xticklabels(all_q_str, **xtick_kw)
ax.legend(fontsize=9, ncol=2)
ax.grid(axis="y", alpha=0.3)
ax.spines[["top", "right"]].set_visible(False)

fig.suptitle("Compra Ágil bidder composition over time (quarterly averages per tender)",
             fontsize=12, fontweight="bold")
savefig("trends_10_compra_agil_bidders_size_region_sii.png")

# ── Summary 2x3 panel ─────────────────────────────────────────────────────
fig, axes = plt.subplots(2, 3, figsize=(20, 10))

# [0,0] Count lines
ax = axes[0,0]
for ds in ["licitaciones","compra_agil"]:
    vals = (tender[tender["dataset"]==ds].groupby("quarter")["tender_id"].nunique()
                .reindex(all_quarters).fillna(0))
    ax.plot(list(xi), vals, marker="o", ms=3, lw=2,
            color=DS_COLORS[ds], ls=DS_STYLES[ds], label=DS_LABELS[ds])
ax.set_title("(A) Tender count", fontweight="bold")
ax.set_xticks(list(xi)); ax.set_xticklabels(all_q_str, **xtick_kw)
ax.yaxis.set_major_formatter(mtick.FuncFormatter(lambda x,_: f"{int(x):,}"))
ax.legend(fontsize=8); ax.grid(axis="y", alpha=0.3); ax.spines[["top","right"]].set_visible(False)

# [0,1] Value
ax = axes[0,1]
for ds in ["licitaciones","compra_agil"]:
    vals = (tender[tender["dataset"]==ds].groupby("quarter")["monto_estimado"].sum()
                .reindex(all_quarters).fillna(0) / 1e9)
    ax.plot(list(xi), vals, marker="o", ms=3, lw=2,
            color=DS_COLORS[ds], ls=DS_STYLES[ds], label=DS_LABELS[ds])
ax.set_title("(B) Budget (bn CLP)", fontweight="bold")
ax.set_xticks(list(xi)); ax.set_xticklabels(all_q_str, **xtick_kw)
ax.legend(fontsize=8); ax.grid(axis="y", alpha=0.3); ax.spines[["top","right"]].set_visible(False)

# [0,2] Avg value
ax = axes[0,2]
for ds in ["licitaciones","compra_agil"]:
    sub = tender[tender["dataset"]==ds]
    total = sub.groupby("quarter")["monto_estimado"].sum().reindex(all_quarters).fillna(np.nan)
    count = sub.groupby("quarter")["tender_id"].nunique().reindex(all_quarters).replace(0,np.nan)
    ax.plot(list(xi), total/count/1e6, marker="o", ms=3, lw=2,
            color=DS_COLORS[ds], ls=DS_STYLES[ds], label=DS_LABELS[ds])
ax.set_title("(C) Avg value (mm CLP)", fontweight="bold")
ax.set_xticks(list(xi)); ax.set_xticklabels(all_q_str, **xtick_kw)
ax.legend(fontsize=8); ax.grid(axis="y", alpha=0.3); ax.spines[["top","right"]].set_visible(False)

# [1,0] Count index
ax = axes[1,0]
for ds in ["licitaciones","compra_agil"]:
    vals = (tender[tender["dataset"]==ds].groupby("quarter")["tender_id"].nunique()
                .reindex(all_quarters).fillna(0))
    base = vals.iloc[0] if vals.iloc[0] != 0 else 1
    ax.plot(list(xi), vals/base*100, marker="o", ms=3, lw=2,
            color=DS_COLORS[ds], ls=DS_STYLES[ds], label=DS_LABELS[ds])
ax.axhline(100, color="grey", lw=1, ls=":", alpha=0.6)
ax.set_title("(D) Count index (2022Q1=100)", fontweight="bold")
ax.set_xticks(list(xi)); ax.set_xticklabels(all_q_str, **xtick_kw)
ax.legend(fontsize=8); ax.grid(axis="y", alpha=0.3); ax.spines[["top","right"]].set_visible(False)

# [1,1] Value index
ax = axes[1,1]
for ds in ["licitaciones","compra_agil"]:
    vals = (tender[tender["dataset"]==ds].groupby("quarter")["monto_estimado"].sum()
                .reindex(all_quarters).fillna(0) / 1e9)
    base = vals.iloc[0] if vals.iloc[0] != 0 else 1
    ax.plot(list(xi), vals/base*100, marker="o", ms=3, lw=2,
            color=DS_COLORS[ds], ls=DS_STYLES[ds], label=DS_LABELS[ds])
ax.axhline(100, color="grey", lw=1, ls=":", alpha=0.6)
ax.set_title("(E) Budget index (2022Q1=100)", fontweight="bold")
ax.set_xticks(list(xi)); ax.set_xticklabels(all_q_str, **xtick_kw)
ax.legend(fontsize=8); ax.grid(axis="y", alpha=0.3); ax.spines[["top","right"]].set_visible(False)

# [1,2] Obras UTM share
ax = axes[1,2]
ax.plot(list(xi), share_q, marker="o", ms=4, lw=2, color="#d62728")
ax.axhline(50, color="grey", lw=1, ls=":", alpha=0.6)
ax.set_title("(F) Obras ≥500 UTM share", fontweight="bold")
ax.yaxis.set_major_formatter(mtick.PercentFormatter(decimals=0))
ax.set_ylim(0,100)
ax.set_xticks(list(xi)); ax.set_xticklabels(all_q_str, **xtick_kw)
ax.grid(axis="y", alpha=0.3); ax.spines[["top","right"]].set_visible(False)

fig.suptitle("Quarterly Procurement Trends — Summary", fontsize=14, fontweight="bold")
savefig("trends_summary.png")

print("\nAll done.")
