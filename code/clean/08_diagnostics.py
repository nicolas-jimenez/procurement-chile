"""
clean/08_diagnostics.py
─────────────────────────────────────────────────────────────────────────────
Diagnostic statistics and figures for the combined panel (licitaciones +
compra ágil), with all outputs split by dataset type.

Input:  data/clean/combined_sii_merged_filtered.parquet
Outputs (all to data/diagnostics/figures/):
  combined_01_volume.png        — tenders per quarter, by dataset
  combined_02_value.png         — total budget per quarter, by dataset
  combined_03_local.png         — same_region bidder share over time, by dataset
  combined_04_tamano.png        — firm size (tramoventas + Tamano) by dataset
  combined_05_sii_match.png     — SII match rate by dataset and year
  combined_06_winner_type.png   — winner characteristics by dataset
  combined_summary.png          — 2x3 summary panel
CSV outputs:
  data/diagnostics/combined_locality_by_dataset.csv
  data/diagnostics/combined_sii_match_rates.csv
"""

from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config import DATA_CLEAN, OUTPUT_ROOT  # noqa: E402

IN_FILE  = DATA_CLEAN / "combined_sii_merged_filtered.parquet"
DIAG_DIR = OUTPUT_ROOT / "cleaning_diagnostics"
FIG_DIR  = DIAG_DIR / "figures"
FIG_DIR.mkdir(parents=True, exist_ok=True)

xtick_kw  = dict(rotation=45, ha="right", fontsize=7)
DS_COLORS = {"licitaciones": "#1f77b4", "compra_agil": "#d62728"}
DS_LABELS = {"licitaciones": "Licitaciones", "compra_agil": "Compra Ágil"}
DS_STYLES = {"licitaciones": "-", "compra_agil": "--"}

TRAMO_SIZE = {
    1:"Sin ventas", 2:"Micro", 3:"Micro", 4:"Micro",
    5:"Pequeña", 6:"Pequeña", 7:"Pequeña",
    8:"Mediana", 9:"Mediana",
    10:"Grande", 11:"Grande", 12:"Grande", 13:"Grande",
}

# ══════════════════════════════════════════════════════════════════════════
print("=" * 70)
print("STEP 1 — Loading combined SII-merged panel")
print("=" * 70)

df = pd.read_parquet(IN_FILE, columns=[
    "dataset", "tender_id", "rut_bidder",
    "fecha_pub", "source_year", "source_month",
    "monto_estimado", "monto_oferta",
    "is_selected", "is_key_dup",
    "tipo", "sector", "tamano",
    "same_region", "tramoventas", "razonsocial",
])
print(f"  Rows: {len(df):,}")
print(f"  dataset:\n{df['dataset'].value_counts().to_string()}")

df["fecha_pub"] = pd.to_datetime(df["fecha_pub"], errors="coerce")
df["quarter"]   = df["fecha_pub"].dt.to_period("Q")
df["size_label"]= df["tramoventas"].map(TRAMO_SIZE).fillna(
    df["tamano"].where(df["tamano"].notna(), other="Unknown"))
df["is_grande"] = df["size_label"] == "Grande"
df["is_mipyme"] = df["size_label"].isin(["Micro","Pequeña","Mediana","Sin ventas"])
df["sii_matched"]= df["razonsocial"].notna()

# Dedup bids (not key-dups)
df_valid = df[~df["is_key_dup"]].copy()

all_quarters = sorted(df_valid["quarter"].dropna().unique())
all_q_str    = [str(q) for q in all_quarters]
xi           = range(len(all_q_str))

def savefig(fname):
    plt.tight_layout()
    plt.savefig(FIG_DIR / fname, dpi=150, bbox_inches="tight")
    print(f"  Saved: {fname}")
    plt.close()

# ── Helper: quarterly series per dataset ──────────────────────────────────
def qseries(data, col, agg="mean", ds_filter=None):
    sub = data if ds_filter is None else data[data["dataset"]==ds_filter]
    return (sub.groupby("quarter")[col]
               .agg(agg)
               .reindex(all_quarters)
               .fillna(np.nan))

def n_tenders_q(data, ds_filter=None):
    sub = data if ds_filter is None else data[data["dataset"]==ds_filter]
    return (sub.groupby("quarter")["tender_id"]
               .nunique()
               .reindex(all_quarters)
               .fillna(0))

# ══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("STEP 2 — Tender-level stats")
print("=" * 70)

# Collapse to tender level for volume/value
tender = df_valid.drop_duplicates(["dataset","tender_id"]).copy()
print(f"  Unique (dataset, tender_id): {len(tender):,}")
print(f"  By dataset:\n{tender['dataset'].value_counts().to_string()}")

# ══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("STEP 3 — Plotting")
print("=" * 70)

# ── 01: Volume over time ──────────────────────────────────────────────────
fig, axes = plt.subplots(1, 2, figsize=(16, 5))

ax = axes[0]
for ds in ["licitaciones","compra_agil"]:
    vals = n_tenders_q(tender, ds)
    ax.plot(list(xi), vals, marker="o", ms=4, lw=2,
            color=DS_COLORS[ds], ls=DS_STYLES[ds], label=DS_LABELS[ds])
ax.set_title("Unique tenders/cotizaciones per quarter", fontsize=10, fontweight="bold")
ax.set_xlabel("Quarter"); ax.set_ylabel("Count")
ax.set_xticks(list(xi)); ax.set_xticklabels(all_q_str, **xtick_kw)
ax.yaxis.set_major_formatter(mtick.FuncFormatter(lambda x,_: f"{int(x):,}"))
ax.legend(fontsize=9); ax.grid(axis="y", alpha=0.3)
ax.spines[["top","right"]].set_visible(False)

ax = axes[1]
# Stacked bar: licitaciones + compra ágil
lic_v = n_tenders_q(tender,"licitaciones").fillna(0)
ca_v  = n_tenders_q(tender,"compra_agil").fillna(0)
ax.bar(list(xi), lic_v, color=DS_COLORS["licitaciones"], alpha=0.85, label="Licitaciones")
ax.bar(list(xi), ca_v, bottom=lic_v, color=DS_COLORS["compra_agil"], alpha=0.85, label="Compra Ágil")
ax.set_title("Combined tender volume (stacked)", fontsize=10, fontweight="bold")
ax.set_xlabel("Quarter"); ax.set_ylabel("Count")
ax.set_xticks(list(xi)); ax.set_xticklabels(all_q_str, **xtick_kw)
ax.yaxis.set_major_formatter(mtick.FuncFormatter(lambda x,_: f"{int(x):,}"))
ax.legend(fontsize=9); ax.grid(axis="y", alpha=0.3)
ax.spines[["top","right"]].set_visible(False)

fig.suptitle("Tender volume over time by dataset", fontsize=13, fontweight="bold")
savefig("combined_01_volume.png")

# ── 02: Value over time ───────────────────────────────────────────────────
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
ax.set_title("Total estimated budget per quarter (bn CLP)", fontsize=10, fontweight="bold")
ax.set_xlabel("Quarter"); ax.set_ylabel("bn CLP")
ax.set_xticks(list(xi)); ax.set_xticklabels(all_q_str, **xtick_kw)
ax.legend(fontsize=9); ax.grid(axis="y", alpha=0.3)
ax.spines[["top","right"]].set_visible(False)

ax = axes[1]
lic_bv = tender[tender["dataset"]=="licitaciones"].groupby("quarter")["monto_estimado"].sum().reindex(all_quarters).fillna(0) / 1e9
ca_bv  = tender[tender["dataset"]=="compra_agil"].groupby("quarter")["monto_estimado"].sum().reindex(all_quarters).fillna(0) / 1e9
ax.bar(list(xi), lic_bv, color=DS_COLORS["licitaciones"], alpha=0.85, label="Licitaciones")
ax.bar(list(xi), ca_bv, bottom=lic_bv, color=DS_COLORS["compra_agil"], alpha=0.85, label="Compra Ágil")
ax.set_title("Combined total budget (stacked, bn CLP)", fontsize=10, fontweight="bold")
ax.set_xlabel("Quarter"); ax.set_ylabel("bn CLP")
ax.set_xticks(list(xi)); ax.set_xticklabels(all_q_str, **xtick_kw)
ax.legend(fontsize=9); ax.grid(axis="y", alpha=0.3)
ax.spines[["top","right"]].set_visible(False)

fig.suptitle("Total procurement budget over time by dataset", fontsize=13, fontweight="bold")
savefig("combined_02_value.png")

# ── 03: Locality (same_region) by dataset ────────────────────────────────
fig, ax = plt.subplots(figsize=(12, 5))
for ds in ["licitaciones","compra_agil"]:
    vals = (df_valid[df_valid["dataset"]==ds]
                .groupby("quarter")["same_region"]
                .mean()
                .reindex(all_quarters)
                .fillna(np.nan) * 100)
    ax.plot(list(xi), vals, marker="o", ms=4, lw=2,
            color=DS_COLORS[ds], ls=DS_STYLES[ds], label=DS_LABELS[ds])
ax.set_title("Same-region bidder share over time by dataset", fontsize=10, fontweight="bold")
ax.set_xlabel("Quarter"); ax.set_ylabel("Share (%)")
ax.yaxis.set_major_formatter(mtick.PercentFormatter(decimals=0))
ax.set_xticks(list(xi)); ax.set_xticklabels(all_q_str, **xtick_kw)
ax.legend(fontsize=9); ax.grid(axis="y", alpha=0.3)
ax.spines[["top","right"]].set_visible(False)
fig.suptitle("Local (same-region) bidder share: licitaciones vs compra ágil",
             fontsize=12, fontweight="bold")
savefig("combined_03_local.png")

# Save locality CSV
loc_csv = (df_valid.groupby(["dataset","source_year"])
               .agg(mean_same_region=("same_region","mean"),
                    n_bids=("same_region","count"))
               .reset_index())
loc_csv.to_csv(DIAG_DIR / "combined_locality_by_dataset.csv", index=False)
print(f"  Saved: combined_locality_by_dataset.csv")

# ── 04: Firm size by dataset ──────────────────────────────────────────────
fig, axes = plt.subplots(1, 2, figsize=(16, 5))

for ax, ds in zip(axes, ["licitaciones","compra_agil"]):
    sub = df_valid[df_valid["dataset"]==ds]
    for label, color in [("Grande","#d62728"),("Micro","#1f77b4"),("Pequeña","#ff7f0e"),("Mediana","#2ca02c")]:
        ind = (sub["size_label"] == label).astype(float)
        vals = (sub.assign(_ind=ind)
                   .groupby("quarter")["_ind"]
                   .mean()
                   .reindex(all_quarters)
                   .fillna(np.nan) * 100)
        ax.plot(list(xi), vals, marker="o", ms=3, lw=2, color=color, label=label)
    ax.set_title(f"Bidder size share — {DS_LABELS[ds]}", fontsize=10, fontweight="bold")
    ax.set_xlabel("Quarter"); ax.set_ylabel("Share (%)")
    ax.yaxis.set_major_formatter(mtick.PercentFormatter(decimals=0))
    ax.set_xticks(list(xi)); ax.set_xticklabels(all_q_str, **xtick_kw)
    ax.legend(fontsize=9, title="Size"); ax.grid(axis="y", alpha=0.3)
    ax.spines[["top","right"]].set_visible(False)

fig.suptitle("Bidder size composition over time by dataset (SII tramoventas / Tamano)",
             fontsize=12, fontweight="bold")
savefig("combined_04_tamano.png")

# ── 05: SII match rate by dataset ─────────────────────────────────────────
match_csv = (df_valid.groupby(["dataset","source_year"])
                 .agg(match_rate=("sii_matched","mean"),
                      n_bids=("sii_matched","count"))
                 .reset_index())
match_csv.to_csv(DIAG_DIR / "combined_sii_match_rates.csv", index=False)
print(f"  Saved: combined_sii_match_rates.csv")

fig, ax = plt.subplots(figsize=(12, 5))
for ds in ["licitaciones","compra_agil"]:
    sub = match_csv[match_csv["dataset"]==ds]
    ax.plot(sub["source_year"], sub["match_rate"]*100,
            marker="o", ms=5, lw=2,
            color=DS_COLORS[ds], ls=DS_STYLES[ds], label=DS_LABELS[ds])
ax.set_title("SII match rate by dataset and year", fontsize=10, fontweight="bold")
ax.set_xlabel("Year"); ax.set_ylabel("Match rate (%)")
ax.yaxis.set_major_formatter(mtick.PercentFormatter(decimals=0))
ax.legend(fontsize=9); ax.grid(axis="y", alpha=0.3)
ax.spines[["top","right"]].set_visible(False)
fig.suptitle("Share of bidder rows matched to SII: licitaciones vs compra ágil",
             fontsize=12, fontweight="bold")
savefig("combined_05_sii_match.png")

# ── 06: Winner characteristics by dataset ────────────────────────────────
winners = df_valid[df_valid["is_selected"]].copy()
fig, axes = plt.subplots(1, 3, figsize=(20, 5))
metrics = [
    ("same_region", "Local (same-region) winner share"),
    ("is_grande",   "Grande winner share"),
    ("is_mipyme",   "MIPYME winner share"),
]
for ax, (col, title) in zip(axes, metrics):
    for ds in ["licitaciones","compra_agil"]:
        vals = (winners[winners["dataset"]==ds]
                    .groupby("quarter")[col]
                    .mean()
                    .reindex(all_quarters)
                    .fillna(np.nan) * 100)
        ax.plot(list(xi), vals, marker="o", ms=4, lw=2,
                color=DS_COLORS[ds], ls=DS_STYLES[ds], label=DS_LABELS[ds])
    ax.set_title(title, fontsize=9, fontweight="bold")
    ax.set_xlabel("Quarter"); ax.set_ylabel("Share (%)")
    ax.yaxis.set_major_formatter(mtick.PercentFormatter(decimals=0))
    ax.set_xticks(list(xi)); ax.set_xticklabels(all_q_str, **xtick_kw)
    ax.legend(fontsize=8); ax.grid(axis="y", alpha=0.3)
    ax.spines[["top","right"]].set_visible(False)
fig.suptitle("Winner characteristics over time by dataset", fontsize=12, fontweight="bold")
savefig("combined_06_winner_type.png")

# ── Summary 2x3 panel ────────────────────────────────────────────────────
fig, axes = plt.subplots(2, 3, figsize=(20, 10))

# [0,0] Volume lines
ax = axes[0,0]
for ds in ["licitaciones","compra_agil"]:
    vals = n_tenders_q(tender, ds)
    ax.plot(list(xi), vals, marker="o", ms=3, lw=2,
            color=DS_COLORS[ds], ls=DS_STYLES[ds], label=DS_LABELS[ds])
ax.set_title("(A) Tender volume", fontweight="bold")
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

# [0,2] Locality
ax = axes[0,2]
for ds in ["licitaciones","compra_agil"]:
    vals = (df_valid[df_valid["dataset"]==ds].groupby("quarter")["same_region"].mean()
                .reindex(all_quarters).fillna(np.nan) * 100)
    ax.plot(list(xi), vals, marker="o", ms=3, lw=2,
            color=DS_COLORS[ds], ls=DS_STYLES[ds], label=DS_LABELS[ds])
ax.set_title("(C) Local bidder share", fontweight="bold")
ax.yaxis.set_major_formatter(mtick.PercentFormatter(decimals=0))
ax.set_xticks(list(xi)); ax.set_xticklabels(all_q_str, **xtick_kw)
ax.legend(fontsize=8); ax.grid(axis="y", alpha=0.3); ax.spines[["top","right"]].set_visible(False)

# [1,0] Grande share
ax = axes[1,0]
for ds in ["licitaciones","compra_agil"]:
    vals = (df_valid[df_valid["dataset"]==ds].groupby("quarter")["is_grande"].mean()
                .reindex(all_quarters).fillna(np.nan) * 100)
    ax.plot(list(xi), vals, marker="o", ms=3, lw=2,
            color=DS_COLORS[ds], ls=DS_STYLES[ds], label=DS_LABELS[ds])
ax.set_title("(D) Grande bidder share", fontweight="bold")
ax.yaxis.set_major_formatter(mtick.PercentFormatter(decimals=0))
ax.set_xticks(list(xi)); ax.set_xticklabels(all_q_str, **xtick_kw)
ax.legend(fontsize=8); ax.grid(axis="y", alpha=0.3); ax.spines[["top","right"]].set_visible(False)

# [1,1] SII match
ax = axes[1,1]
for ds in ["licitaciones","compra_agil"]:
    sub = match_csv[match_csv["dataset"]==ds]
    ax.plot(sub["source_year"], sub["match_rate"]*100, marker="o", ms=4, lw=2,
            color=DS_COLORS[ds], ls=DS_STYLES[ds], label=DS_LABELS[ds])
ax.set_title("(E) SII match rate", fontweight="bold")
ax.yaxis.set_major_formatter(mtick.PercentFormatter(decimals=0))
ax.legend(fontsize=8); ax.grid(axis="y", alpha=0.3); ax.spines[["top","right"]].set_visible(False)

# [1,2] Local winner share
ax = axes[1,2]
for ds in ["licitaciones","compra_agil"]:
    vals = (winners[winners["dataset"]==ds].groupby("quarter")["same_region"].mean()
                .reindex(all_quarters).fillna(np.nan) * 100)
    ax.plot(list(xi), vals, marker="o", ms=3, lw=2,
            color=DS_COLORS[ds], ls=DS_STYLES[ds], label=DS_LABELS[ds])
ax.set_title("(F) Local winner share", fontweight="bold")
ax.yaxis.set_major_formatter(mtick.PercentFormatter(decimals=0))
ax.set_xticks(list(xi)); ax.set_xticklabels(all_q_str, **xtick_kw)
ax.legend(fontsize=8); ax.grid(axis="y", alpha=0.3); ax.spines[["top","right"]].set_visible(False)

fig.suptitle("Combined Panel Diagnostics: Licitaciones vs Compra Ágil",
             fontsize=14, fontweight="bold")
savefig("combined_summary.png")

print("\nAll done.")
