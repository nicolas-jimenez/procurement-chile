"""
09_compra_agil_diagnostics.py
─────────────────────────────────────────────────────────────────────────────
Descriptive statistics and appendability diagnostics for Compra Ágil data.

Outputs (all to output/diagnostics/figures/ and output/summary_stats/):
  Panel A – Volume over time
    A1. Monthly/quarterly cotización volume (row count and unique cotizaciones)
    A2. Total estimated budget (MontoTotalDisponble) by quarter

  Panel B – Buyer / Seller characteristics
    B1. Buyer region distribution (top 15)
    B2. Seller size (Tamano): MiPyme vs Grande over time
    B3. Selection rate (is_selected) by size over time
    B4. Award rate: share of cotizaciones with at least one selected bid

  Panel C – Outcome / status
    C1. Estado distribution
    C2. MontoTotal distribution (log scale)

  Panel D – Appendability vs. licitaciones
    D1. Summary table of shared identifiers
    D2. Volume comparison: compra_agil vs licitaciones over time
    D3. Buyer RUT overlap: share of compra_agil buyers also in licitaciones
    D4. Share of projects that are compra_agil by UTM bucket over time
    D5. Share of local bidders by UTM bucket over time
    D6. Share of Empresa Grande (Ventas) by UTM bucket over time
    D7. Share of Empresa Grande (Empleo) by UTM bucket over time
    D8. Avg # bidders per tender by bidder type (30–100 UTM only)
    D9. Avg # bidders per tender by bidder type (0–30 UTM only)
    D10. Avg # bidders per tender by bidder type (100–500 UTM only)
    D11. Avg # bidders per tender by bidder type (500+ UTM only)
    D12. Avg # bidders per tender by estimated-cost bucket (single chart)
    D13. Monthly stacked tender counts by dataset, separately for each UTM bucket
    D14. Monthly stacked estimated value by dataset, separately for each UTM bucket
    D16. Avg # winning bidders per tender by winner type (30–100 UTM only)
    D17. Avg # winning bidders per tender by winner type (0–30 UTM only)
    D18. Avg # winning bidders per tender by winner type (100–500 UTM only)
    D19. Avg # winning bidders per tender by winner type (500+ UTM only)
    D20. Share of bidders by bidder type within tender (30–100 UTM only, stacked)
    D21. Share of bidders by bidder type within tender (0–30 UTM only, stacked)
    D22. Share of bidders by bidder type within tender (100–500 UTM only, stacked)
    D23. Share of bidders by bidder type within tender (500+ UTM only, stacked)
    D24. Share of winners by winner type within tender (30–100 UTM only, stacked)
    D25. Share of winners by winner type within tender (0–30 UTM only, stacked)
    D26. Share of winners by winner type within tender (100–500 UTM only, stacked)
    D27. Share of winners by winner type within tender (500+ UTM only, stacked)

CLI option:
  --bidder-type-freq {quarter,month}
    Controls time aggregation for D8-D12 (default: quarter).
"""

import argparse
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config import DATA_CLEAN, DATA_RAW_OTHER, OUTPUT_ROOT  # noqa: E402

CA_FILE  = DATA_CLEAN / "compra_agil_panel.parquet"
LIC_FILE = DATA_CLEAN / "chilecompra_panel.parquet"
COMBINED_FILTERED = DATA_CLEAN / "combined_sii_merged_filtered.parquet"
UTM_FILE = DATA_RAW_OTHER / "utm_clp_2022_2025.csv"
DIAG_DIR = OUTPUT_ROOT / "diagnostics"
FIG_DIR  = DIAG_DIR / "figures"
SUMMARY_DIR = OUTPUT_ROOT / "summary_stats"
FIG_DIR.mkdir(parents=True, exist_ok=True)
SUMMARY_DIR.mkdir(parents=True, exist_ok=True)

xtick_kw = dict(rotation=45, ha="right", fontsize=7)

# Optional frequency for bidder-type count graphs (D8-D12).
parser = argparse.ArgumentParser(add_help=True)
parser.add_argument(
    "--bidder-type-freq",
    choices=["quarter", "month"],
    default="quarter",
    help="Time aggregation for D8-D12 bidder-type count graphs.",
)
ARGS = parser.parse_args()
BIDDER_TYPE_FREQ = ARGS.bidder_type_freq
BIDDER_TYPE_PERIOD_COL = "month" if BIDDER_TYPE_FREQ == "month" else "quarter"
BIDDER_TYPE_PERIOD_LABEL = "Month" if BIDDER_TYPE_FREQ == "month" else "Quarter"
BIDDER_TYPE_PLOT_LABEL = "monthly" if BIDDER_TYPE_FREQ == "month" else "quarterly"
BIDDER_TYPE_SUFFIX = "_month" if BIDDER_TYPE_FREQ == "month" else ""

# Load only fields used downstream to keep memory bounded.
CA_COLS = [
    "CodigoCotizacion",
    "FechaPublicacionParaCotizar",
    "MontoTotalDisponble",
    "MontoTotal",
    "is_selected",
    "Tamano",
    "Estado",
    "Region",
    "RUTUnidaddeCompra",
    "RUTProveedor",
]

LIC_COLS = [
    "Codigo",
    "FechaPublicacion",
    "MontoEstimado",
    "RutUnidad",
]

# ═══════════════════════════════════════════════════════════════════════════
print(f"Option — D8-D12 frequency: {BIDDER_TYPE_FREQ}")
print()
print("=" * 70)
print("STEP 1 — Loading Compra Ágil panel")
print("=" * 70)

ca = pd.read_parquet(CA_FILE, columns=CA_COLS)
print(f"  Total rows: {len(ca):,}")
print(f"  Columns   : {len(ca.columns)}")
print(f"  Columns   : {list(ca.columns)}")

ca["FechaPublicacionParaCotizar"] = pd.to_datetime(ca["FechaPublicacionParaCotizar"], errors="coerce")
for cat_col in ["Tamano", "Estado", "Region"]:
    ca[cat_col] = ca[cat_col].astype("category")

ca["quarter"] = ca["FechaPublicacionParaCotizar"].dt.to_period("Q")
ca["ym"]      = ca["FechaPublicacionParaCotizar"].dt.to_period("M")

print(f"  Quarter range: {ca['quarter'].dropna().min()} – {ca['quarter'].dropna().max()}")
print(f"  Unique cotizaciones: {ca['CodigoCotizacion'].nunique():,}")
print(f"  Selected bids: {ca['is_selected'].sum():,} ({100*ca['is_selected'].mean():.1f}%)")
print(f"  Tamano:\n{ca['Tamano'].value_counts(dropna=False).to_string()}")
print(f"  Estado:\n{ca['Estado'].value_counts(dropna=False).head(10).to_string()}")

# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("STEP 2 — Tender-level aggregations (one row = one cotización)")
print("=" * 70)

# Collapse to cotización level using vectorized aggregations only.
ca["is_mipyme"] = (ca["Tamano"] == "MiPyme").astype("int8")
ca["is_grande"] = (ca["Tamano"] == "Grande").astype("int8")

cot = ca.groupby("CodigoCotizacion", sort=False).agg(
    quarter         = ("quarter", "first"),
    ym              = ("ym", "first"),
    Region          = ("Region", "first"),
    MontoDisponible = ("MontoTotalDisponble", "first"),
    n_bids          = ("CodigoCotizacion", "size"),  # rows (bids) per cotización
    n_selected      = ("is_selected", "sum"),
    n_mipyme        = ("is_mipyme", "sum"),
    n_grande        = ("is_grande", "sum"),
    Estado          = ("Estado", "first"),
).reset_index()

cot["has_award"]  = cot["n_selected"] >= 1
cot["share_mipyme"] = cot["n_mipyme"] / (cot["n_mipyme"] + cot["n_grande"]).replace(0, np.nan)

print(f"  Unique cotizaciones: {len(cot):,}")
print(f"  With award:          {cot['has_award'].sum():,} ({100*cot['has_award'].mean():.1f}%)")

all_quarters = sorted(cot["quarter"].dropna().unique())
all_q_str    = [str(q) for q in all_quarters]
x_idx        = range(len(all_q_str))

# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("STEP 3 — Loading licitaciones panel for appendability check")
print("=" * 70)

lic = pd.read_parquet(LIC_FILE, columns=LIC_COLS)
lic = lic.drop_duplicates("Codigo")
lic["FechaPublicacion"] = pd.to_datetime(lic["FechaPublicacion"], errors="coerce")
lic["quarter"] = lic["FechaPublicacion"].dt.to_period("Q")
print(f"  Licitaciones unique tenders: {len(lic):,}")

# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("STEP 4 — Plotting")
print("=" * 70)

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


def _extract_rut_dv(df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    """
    Extract canonical numeric RUT and DV, using explicit columns first and
    falling back to parsing rut_bidder_raw.
    """
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
    dv = dv.where(dv.notna(), raw_dv).astype("string")
    return rut_num.astype("Int64"), dv


def _rut_expected_dv(rut: int) -> str:
    """Compute expected Chilean RUT verifier digit (DV) using Mod-11."""
    if rut <= 0:
        return ""
    total = 0
    mult = 2
    n = int(rut)
    while n > 0:
        total += (n % 10) * mult
        n //= 10
        mult = 2 if mult == 7 else mult + 1
    rem = 11 - (total % 11)
    if rem == 11:
        return "0"
    if rem == 10:
        return "K"
    return str(rem)


def _valid_rut_flag(rut: pd.Series, dv: pd.Series) -> pd.Series:
    """Return True when RUT-DV passes Mod-11 validation."""
    rut_num = pd.to_numeric(rut, errors="coerce").round().astype("Int64")
    dv_norm = (
        dv.astype("string")
        .str.strip()
        .str.upper()
        .str.extract(r"([0-9K])")[0]
        .astype("string")
    )

    unique_rut = pd.Series(rut_num.dropna().unique(), dtype="Int64")
    expected_map = {
        int(r): _rut_expected_dv(int(r))
        for r in unique_rut
        if int(r) > 0
    }
    expected = rut_num.map(expected_map).astype("string")
    valid = (
        rut_num.notna()
        & (rut_num > 0)
        & dv_norm.notna()
        & expected.notna()
        & dv_norm.eq(expected)
    )
    return valid.fillna(False)


def _build_bidder_id(df: pd.DataFrame) -> pd.Series:
    """
    Canonical bidder id for unique-bidder counting in RUT-DV form.
    Priority:
      1) rut_bidder + dv_bidder
      2) parsed RUT-DV from rut_bidder_raw
      3) synthetic per-row id when any part is missing
    """
    idx = df.index
    rut_num, dv = _extract_rut_dv(df)

    bidder_id = rut_num.astype("string") + "-" + dv.astype("string")
    bad = rut_num.isna() | dv.isna()
    if bad.any():
        synthetic = "__missing_bidder_" + pd.Series(np.arange(len(df)), index=idx).astype(str)
        bidder_id = bidder_id.copy()
        bidder_id.loc[bad] = synthetic.loc[bad].astype("string")
    return bidder_id.astype("string")

# ── A1: Volume over time ───────────────────────────────────────────────────
q_vol = cot.groupby("quarter").agg(
    n_cot  = ("CodigoCotizacion", "count"),
).reset_index()
q_vol["quarter_str"] = q_vol["quarter"].astype(str)

fig, axes = plt.subplots(1, 2, figsize=(16, 5))

ax = axes[0]
vals = q_vol.set_index("quarter_str")["n_cot"].reindex(all_q_str).fillna(0)
ax.bar(list(x_idx), vals, color="#1f77b4", alpha=0.85)
ax.set_title("Compra Ágil: unique cotizaciones per quarter", fontsize=10, fontweight="bold")
ax.set_xlabel("Quarter"); ax.set_ylabel("Number of cotizaciones")
ax.set_xticks(list(x_idx)); ax.set_xticklabels(all_q_str, **xtick_kw)
ax.yaxis.set_major_formatter(mtick.FuncFormatter(lambda x,_: f"{int(x):,}"))
ax.grid(axis="y", alpha=0.3); ax.spines[["top","right"]].set_visible(False)

# Monthly view
ax = axes[1]
m_vol = ca.groupby("ym")["CodigoCotizacion"].nunique().reset_index()
m_vol["ym_str"] = m_vol["ym"].astype(str)
m_vol = m_vol.sort_values("ym")
ax.bar(range(len(m_vol)), m_vol["CodigoCotizacion"], color="#ff7f0e", alpha=0.85)
ax.set_title("Compra Ágil: unique cotizaciones per month", fontsize=10, fontweight="bold")
ax.set_xlabel("Month"); ax.set_ylabel("Number of cotizaciones")
tick_every = max(1, len(m_vol)//16)
ax.set_xticks(range(0,len(m_vol),tick_every))
ax.set_xticklabels(m_vol["ym_str"].iloc[::tick_every], **xtick_kw)
ax.yaxis.set_major_formatter(mtick.FuncFormatter(lambda x,_: f"{int(x):,}"))
ax.grid(axis="y", alpha=0.3); ax.spines[["top","right"]].set_visible(False)

fig.suptitle("Compra Ágil volume over time", fontsize=13, fontweight="bold")
savefig("ca_A1_volume.png")

# ── A2: Total budget by quarter ────────────────────────────────────────────
q_bud = cot.groupby("quarter")["MontoDisponible"].sum().reset_index()
q_bud["quarter_str"] = q_bud["quarter"].astype(str)
q_bud_val = q_bud.set_index("quarter_str")["MontoDisponible"].reindex(all_q_str).fillna(0)

fig, ax = plt.subplots(figsize=(12, 5))
ax.bar(list(x_idx), q_bud_val / 1e9, color="#2ca02c", alpha=0.85)
ax.set_title("Compra Ágil: total estimated budget (MontoTotalDisponble) by quarter",
             fontsize=10, fontweight="bold")
ax.set_xlabel("Quarter"); ax.set_ylabel("Total budget (billion CLP)")
ax.set_xticks(list(x_idx)); ax.set_xticklabels(all_q_str, **xtick_kw)
ax.grid(axis="y", alpha=0.3); ax.spines[["top","right"]].set_visible(False)
savefig("ca_A2_budget.png")

# ── B1: Buyer region distribution ─────────────────────────────────────────
top_regions = (cot["Region"].value_counts()
                   .head(15)
                   .sort_values(ascending=True))

fig, ax = plt.subplots(figsize=(10, 7))
ax.barh(top_regions.index, top_regions.values, color="#9467bd", alpha=0.85)
ax.set_title("Compra Ágil: cotizaciones by buyer region (top 15)",
             fontsize=10, fontweight="bold")
ax.set_xlabel("Number of cotizaciones")
ax.xaxis.set_major_formatter(mtick.FuncFormatter(lambda x,_: f"{int(x):,}"))
ax.grid(axis="x", alpha=0.3); ax.spines[["top","right"]].set_visible(False)
savefig("ca_B1_region.png")

# ── B2: Tamano over time ───────────────────────────────────────────────────
# For each quarter: share of cotizaciones (with any bid) that are MiPyme-majority
q_size = cot.groupby("quarter").agg(
    share_mipyme = ("share_mipyme", "mean"),
).reset_index()
q_size["quarter_str"] = q_size["quarter"].astype(str)
vals_mp = q_size.set_index("quarter_str")["share_mipyme"].reindex(all_q_str) * 100

# Also bidder-level share from raw data (not collapsed)
bids_q = ca.groupby(["quarter","Tamano"]).size().reset_index(name="n")
bids_q["quarter_str"] = bids_q["quarter"].astype(str)
bids_tot = bids_q.groupby("quarter")["n"].transform("sum")
bids_q["share"] = 100 * bids_q["n"] / bids_tot

fig, axes = plt.subplots(1, 2, figsize=(16, 5))

ax = axes[0]
for label, color in [("MiPyme","#1f77b4"),("Grande","#d62728")]:
    sub = bids_q[bids_q["Tamano"]==label].set_index("quarter_str")["share"].reindex(all_q_str)
    ax.plot(list(x_idx), sub, marker="o", ms=4, lw=2, color=color, label=label)
ax.set_title("Bidder size over time (share of bids)", fontsize=10, fontweight="bold")
ax.set_xlabel("Quarter"); ax.set_ylabel("Share of bids (%)")
ax.yaxis.set_major_formatter(mtick.PercentFormatter(decimals=0))
ax.set_xticks(list(x_idx)); ax.set_xticklabels(all_q_str, **xtick_kw)
ax.legend(fontsize=10); ax.grid(axis="y", alpha=0.3)
ax.spines[["top","right"]].set_visible(False)

ax = axes[1]
ax.plot(list(x_idx), vals_mp, marker="o", ms=4, lw=2, color="#1f77b4",
        label="Avg MiPyme share per cotización")
ax.set_title("MiPyme bidder share per cotización (avg across cotizaciones)",
             fontsize=10, fontweight="bold")
ax.set_xlabel("Quarter"); ax.set_ylabel("Avg share (%)")
ax.yaxis.set_major_formatter(mtick.PercentFormatter(decimals=0))
ax.set_xticks(list(x_idx)); ax.set_xticklabels(all_q_str, **xtick_kw)
ax.legend(fontsize=10); ax.grid(axis="y", alpha=0.3)
ax.spines[["top","right"]].set_visible(False)

fig.suptitle("Compra Ágil: bidder size (MiPyme vs Grande) over time",
             fontsize=12, fontweight="bold")
savefig("ca_B2_tamano.png")

# ── B3: Selection rate by Tamano ───────────────────────────────────────────
sel_q = ca.groupby(["quarter","Tamano"])["is_selected"].mean().reset_index()
sel_q["quarter_str"] = sel_q["quarter"].astype(str)

fig, ax = plt.subplots(figsize=(12, 5))
for label, color in [("MiPyme","#1f77b4"),("Grande","#d62728")]:
    sub = sel_q[sel_q["Tamano"]==label].set_index("quarter_str")["is_selected"].reindex(all_q_str) * 100
    ax.plot(list(x_idx), sub, marker="o", ms=4, lw=2, color=color, label=label)
ax.set_title("Selection rate by bidder size (share of bids that are selected)",
             fontsize=10, fontweight="bold")
ax.set_xlabel("Quarter"); ax.set_ylabel("Selection rate (%)")
ax.yaxis.set_major_formatter(mtick.PercentFormatter(decimals=1))
ax.set_xticks(list(x_idx)); ax.set_xticklabels(all_q_str, **xtick_kw)
ax.legend(fontsize=10); ax.grid(axis="y", alpha=0.3)
ax.spines[["top","right"]].set_visible(False)
fig.suptitle("Compra Ágil: selection rate by bidder size", fontsize=12, fontweight="bold")
savefig("ca_B3_selection_rate.png")

# ── B4: Award rate over time ───────────────────────────────────────────────
award_q = cot.groupby("quarter")["has_award"].mean().reset_index()
award_q["quarter_str"] = award_q["quarter"].astype(str)

fig, ax = plt.subplots(figsize=(12, 5))
vals_aw = award_q.set_index("quarter_str")["has_award"].reindex(all_q_str) * 100
ax.plot(list(x_idx), vals_aw, marker="o", ms=5, lw=2, color="#2ca02c")
ax.set_title("Share of cotizaciones with at least one selected bid (award rate)",
             fontsize=10, fontweight="bold")
ax.set_xlabel("Quarter"); ax.set_ylabel("Award rate (%)")
ax.yaxis.set_major_formatter(mtick.PercentFormatter(decimals=0))
ax.set_ylim(0, 100)
ax.set_xticks(list(x_idx)); ax.set_xticklabels(all_q_str, **xtick_kw)
ax.grid(axis="y", alpha=0.3); ax.spines[["top","right"]].set_visible(False)
fig.suptitle("Compra Ágil: award rate over time", fontsize=12, fontweight="bold")
savefig("ca_B4_award_rate.png")

# ── C1: Estado distribution ────────────────────────────────────────────────
estado_counts = (ca["Estado"].value_counts()
                     .sort_values(ascending=True))

fig, ax = plt.subplots(figsize=(10, max(4, len(estado_counts)*0.4)))
ax.barh(estado_counts.index, estado_counts.values, color="#8c564b", alpha=0.85)
ax.set_title("Compra Ágil: bid status (Estado) distribution",
             fontsize=10, fontweight="bold")
ax.set_xlabel("Number of bids")
ax.xaxis.set_major_formatter(mtick.FuncFormatter(lambda x,_: f"{int(x):,}"))
ax.grid(axis="x", alpha=0.3); ax.spines[["top","right"]].set_visible(False)
savefig("ca_C1_estado.png")

# ── C2: MontoTotal distribution ────────────────────────────────────────────
amounts = ca["MontoTotal"].dropna()
amounts = amounts[amounts > 0]

fig, axes = plt.subplots(1, 2, figsize=(14, 5))

ax = axes[0]
ax.hist(np.log10(amounts), bins=60, color="#e377c2", edgecolor="white", linewidth=0.3)
ax.set_title("Distribution of bid amount (log₁₀ CLP)", fontsize=10, fontweight="bold")
ax.set_xlabel("log₁₀(MontoTotal CLP)"); ax.set_ylabel("Count")
ax.grid(axis="y", alpha=0.3); ax.spines[["top","right"]].set_visible(False)

ax = axes[1]
ax.hist(np.log10(amounts.clip(1)), bins=60, color="#17becf", edgecolor="white", linewidth=0.3)
pcts = np.percentile(amounts, [10,25,50,75,90])
for p, lab in zip(np.log10(pcts+1), ["P10","P25","P50","P75","P90"]):
    ax.axvline(p, color="red", lw=1, ls="--", alpha=0.7)
    ax.text(p, ax.get_ylim()[1]*0.9, lab, fontsize=7, ha="center", color="red")
ax.set_title("Bid amounts with percentiles", fontsize=10, fontweight="bold")
ax.set_xlabel("log₁₀(MontoTotal CLP)"); ax.set_ylabel("Count")
ax.grid(axis="y", alpha=0.3); ax.spines[["top","right"]].set_visible(False)

fig.suptitle("Compra Ágil: bid amount distribution", fontsize=12, fontweight="bold")
savefig("ca_C2_amounts.png")

# ═══════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("STEP 5 — Appendability diagnostics vs. licitaciones")
print("=" * 70)

# D2: Volume comparison over time (quarterly)
lic_q = lic.groupby("quarter").size().reset_index(name="n_lic")
lic_q["quarter_str"] = lic_q["quarter"].astype(str)

ca_q_cot = cot.groupby("quarter").size().reset_index(name="n_ca")
ca_q_cot["quarter_str"] = ca_q_cot["quarter"].astype(str)

all_q_both = sorted(set(lic_q["quarter"].tolist() + ca_q_cot["quarter"].tolist()))
all_q_both_str = [str(q) for q in all_q_both]
x_both = range(len(all_q_both_str))

lic_vals = lic_q.set_index("quarter_str")["n_lic"].reindex(all_q_both_str).fillna(0)
ca_vals  = ca_q_cot.set_index("quarter_str")["n_ca"].reindex(all_q_both_str).fillna(0)

fig, ax = plt.subplots(figsize=(14, 5))
ax.plot(list(x_both), lic_vals, marker="o", ms=4, lw=2, color="#1f77b4",
        label="Licitaciones (unique tenders)")
ax.plot(list(x_both), ca_vals, marker="s", ms=4, lw=2, color="#d62728",
        label="Compra Ágil (unique cotizaciones)")
ax.set_title("Volume comparison: Licitaciones vs. Compra Ágil (unique tenders/cotizaciones)",
             fontsize=10, fontweight="bold")
ax.set_xlabel("Quarter"); ax.set_ylabel("Number of contracts/cotizaciones")
ax.set_xticks(list(x_both)); ax.set_xticklabels(all_q_both_str, **xtick_kw)
ax.yaxis.set_major_formatter(mtick.FuncFormatter(lambda x,_: f"{int(x):,}"))
ax.legend(fontsize=10); ax.grid(axis="y", alpha=0.3)
ax.spines[["top","right"]].set_visible(False)
fig.suptitle("Compra Ágil vs. Licitaciones: tender volume over time",
             fontsize=12, fontweight="bold")
savefig("ca_D2_volume_comparison.png")

# D3: Buyer RUT overlap
ca_ruts  = set(ca["RUTUnidaddeCompra"].dropna().unique())
lic_ruts = set(lic["RutUnidad"].dropna().unique()) if "RutUnidad" in lic.columns else set()

n_ca_buyers  = len(ca_ruts)
n_lic_buyers = len(lic_ruts)
n_overlap    = len(ca_ruts & lic_ruts)

print(f"  CA buyer RUTs:         {n_ca_buyers:,}")
print(f"  Licitaciones buyer RUTs: {n_lic_buyers:,}")
print(f"  Overlap:               {n_overlap:,} ({100*n_overlap/max(n_ca_buyers,1):.1f}% of CA buyers also in licitaciones)")

# D1: Appendability summary table
append_rows = [
    ("CodigoCotizacion / Codigo",   "Different format; not directly joinable",       "❌"),
    ("RUTUnidaddeCompra / RutUnidad","Buyer RUT – can link buying units",             "✅"),
    ("RUTProveedor",                 "Bidder RUT – can link to SII/merged",           "✅"),
    ("Region",                       "Buyer region – same geography",                 "✅"),
    ("Tamano",                       "Size in CA; need SII match for licitaciones",   "~"),
    ("FechaPublicacion",             "Date – same period (2022–2025)",                "✅"),
    ("MontoTotal / MontoEstimado",   "Amounts – different concepts",                  "~"),
    ("CodigoOC",                     "OC code – may link awarded CA → licitacion OC", "~"),
    ("sector / tipo",                "No direct sector tag in CA",                    "❌"),
    ("is_selected / Oferta selec.",  "Both have winner indicator",                    "✅"),
]

print("\n  APPENDABILITY SUMMARY")
print(f"  {'Field':40s} {'Note':50s} {'Link'}")
print(f"  {'-'*40} {'-'*50} {'-'*4}")
for row in append_rows:
    print(f"  {row[0]:40s} {row[1]:50s} {row[2]}")

append_df = pd.DataFrame(append_rows, columns=["Field","Note","Linkable"])
append_df.to_csv(SUMMARY_DIR / "ca_appendability.csv", index=False)
print(f"\n  Saved: ca_appendability.csv")

# Overlay budget comparison
if COMBINED_FILTERED.exists():
    print("  Budget source: combined_sii_merged_filtered.parquet")
    budget_df = pd.read_parquet(COMBINED_FILTERED, columns=[
        "dataset", "tender_id", "fecha_pub", "source_year", "source_month",
        "monto_estimado", "is_key_dup",
    ])
    budget_df["fecha_pub"] = pd.to_datetime(budget_df["fecha_pub"], errors="coerce")
    budget_df["quarter"] = budget_df["fecha_pub"].dt.to_period("Q")
    budget_df = budget_df[~budget_df["is_key_dup"]].copy()
    budget_df = budget_df.drop_duplicates(["dataset", "tender_id"])

    lic_bud = (
        budget_df[budget_df["dataset"] == "licitaciones"]
        .groupby("quarter")["monto_estimado"]
        .sum()
        .reset_index(name="budget")
    )
    ca_bud = (
        budget_df[budget_df["dataset"] == "compra_agil"]
        .groupby("quarter")["monto_estimado"]
        .sum()
        .reset_index(name="budget")
    )
else:
    print("  [WARN] Missing combined_sii_merged_filtered.parquet; using raw panel budgets.")
    lic_bud = lic.groupby("quarter")["MontoEstimado"].sum().reset_index(name="budget")
    ca_bud = cot.groupby("quarter")["MontoDisponible"].sum().reset_index(name="budget")

lic_bud["quarter_str"] = lic_bud["quarter"].astype(str)
ca_bud["quarter_str"] = ca_bud["quarter"].astype(str)

lic_bv = lic_bud.set_index("quarter_str")["budget"].reindex(all_q_both_str).fillna(0)
ca_bv  = ca_bud.set_index("quarter_str")["budget"].reindex(all_q_both_str).fillna(0)

fig, ax = plt.subplots(figsize=(14, 5))
ax.plot(list(x_both), lic_bv / 1e9, marker="o", ms=4, lw=2, color="#1f77b4",
        label="Licitaciones MontoEstimado")
ax.plot(list(x_both), ca_bv / 1e9, marker="s", ms=4, lw=2, color="#d62728",
        label="Compra Ágil MontoTotalDisponble")
ax.set_title("Budget comparison: Licitaciones vs. Compra Ágil (billion CLP)",
             fontsize=10, fontweight="bold")
ax.set_xlabel("Quarter"); ax.set_ylabel("Total budget (billion CLP)")
ax.set_xticks(list(x_both)); ax.set_xticklabels(all_q_both_str, **xtick_kw)
ax.legend(fontsize=10); ax.grid(axis="y", alpha=0.3)
ax.spines[["top","right"]].set_visible(False)
fig.suptitle("Compra Ágil vs. Licitaciones: budget over time",
             fontsize=12, fontweight="bold")
savefig("ca_D3_budget_comparison.png")

# D4: Share of projects that are Compra Ágil by estimated-cost UTM bucket
print("  Building UTM-bucket share graph (Compra Ágil share among all projects)")
if COMBINED_FILTERED.exists():
    utm_base = budget_df[
        ["dataset", "tender_id", "fecha_pub", "quarter", "source_year", "source_month", "monto_estimado"]
    ].copy()
else:
    # Fallback path if combined filtered panel is unavailable.
    lic_tmp = (
        lic.assign(
            dataset="licitaciones",
            tender_id=lic["Codigo"].astype(str),
            fecha_pub=pd.to_datetime(lic["FechaPublicacion"], errors="coerce"),
            monto_estimado=pd.to_numeric(lic["MontoEstimado"], errors="coerce"),
        )[
            ["dataset", "tender_id", "fecha_pub", "monto_estimado"]
        ]
        .drop_duplicates(["dataset", "tender_id"])
    )
    ca_tmp = (
        ca.assign(
            dataset="compra_agil",
            tender_id=ca["CodigoCotizacion"].astype(str),
            fecha_pub=pd.to_datetime(ca["FechaPublicacionParaCotizar"], errors="coerce"),
            monto_estimado=pd.to_numeric(ca["MontoTotalDisponble"], errors="coerce"),
        )[
            ["dataset", "tender_id", "fecha_pub", "monto_estimado"]
        ]
        .drop_duplicates(["dataset", "tender_id"])
    )
    utm_base = pd.concat([lic_tmp, ca_tmp], axis=0, ignore_index=True)
    utm_base["quarter"] = utm_base["fecha_pub"].dt.to_period("Q")
    utm_base["source_year"] = utm_base["fecha_pub"].dt.year
    utm_base["source_month"] = utm_base["fecha_pub"].dt.month

utm_tbl = pd.read_csv(UTM_FILE)
utm_tbl = utm_tbl.rename(columns={"month_num": "source_month", "utm_clp": "utm_clp_rate"})
utm_tbl["source_year"] = utm_tbl["year"].astype(int)
utm_tbl["source_month"] = utm_tbl["source_month"].astype(int)
utm_tbl = utm_tbl[["source_year", "source_month", "utm_clp_rate"]].copy()

utm_base["source_year"] = pd.to_numeric(utm_base["source_year"], errors="coerce")
utm_base["source_month"] = pd.to_numeric(utm_base["source_month"], errors="coerce")
ym_missing = utm_base["source_year"].isna() | utm_base["source_month"].isna()
utm_base.loc[ym_missing, "source_year"] = utm_base.loc[ym_missing, "fecha_pub"].dt.year
utm_base.loc[ym_missing, "source_month"] = utm_base.loc[ym_missing, "fecha_pub"].dt.month
utm_base["source_year"] = utm_base["source_year"].astype("Int64")
utm_base["source_month"] = utm_base["source_month"].astype("Int64")

utm_base = utm_base.merge(utm_tbl, on=["source_year", "source_month"], how="left")
utm_base["monto_utm"] = utm_base["monto_estimado"] / utm_base["utm_clp_rate"]
utm_base = utm_base[
    utm_base["quarter"].notna()
    & utm_base["monto_utm"].notna()
    & np.isfinite(utm_base["monto_utm"])
    & (utm_base["monto_utm"] > 0)
].copy()

bucket_bins = [0, 30, 100, np.inf]
bucket_labels = ["0-30 UTM", "30-100 UTM", "100+ UTM"]
utm_base["utm_bucket"] = pd.cut(
    utm_base["monto_utm"],
    bins=bucket_bins,
    labels=bucket_labels,
    include_lowest=True,
    right=True,
)
utm_base = utm_base[utm_base["utm_bucket"].notna()].copy()

bucket_counts = (
    utm_base.groupby(["quarter", "utm_bucket", "dataset"], observed=True)["tender_id"]
    .nunique()
    .reset_index(name="n_projects")
)
bucket_wide = bucket_counts.pivot_table(
    index=["quarter", "utm_bucket"],
    columns="dataset",
    values="n_projects",
    aggfunc="sum",
    fill_value=0,
).reset_index()
bucket_wide.columns.name = None
if "compra_agil" not in bucket_wide.columns:
    bucket_wide["compra_agil"] = 0
if "licitaciones" not in bucket_wide.columns:
    bucket_wide["licitaciones"] = 0
bucket_wide["total_projects"] = bucket_wide["compra_agil"] + bucket_wide["licitaciones"]
bucket_wide["share_compra_agil_pct"] = np.where(
    bucket_wide["total_projects"] > 0,
    100.0 * bucket_wide["compra_agil"] / bucket_wide["total_projects"],
    np.nan,
)
bucket_wide["quarter_str"] = bucket_wide["quarter"].astype(str)
bucket_wide = bucket_wide.sort_values(["quarter", "utm_bucket"]).copy()

bucket_wide[
    [
        "quarter",
        "quarter_str",
        "utm_bucket",
        "compra_agil",
        "licitaciones",
        "total_projects",
        "share_compra_agil_pct",
    ]
].to_csv(SUMMARY_DIR / "ca_share_compra_agil_by_utm_bucket_quarter.csv", index=False)
print("  Saved: ca_share_compra_agil_by_utm_bucket_quarter.csv")

plot_q = sorted(bucket_wide["quarter"].dropna().unique())
plot_q_str = [str(q) for q in plot_q]
x_share = range(len(plot_q_str))
bucket_colors = {"0-30 UTM": "#1f77b4", "30-100 UTM": "#ff7f0e", "100+ UTM": "#2ca02c"}

fig, ax = plt.subplots(figsize=(14, 5))
for b in bucket_labels:
    sub = bucket_wide[bucket_wide["utm_bucket"].astype(str) == b]
    series = sub.set_index("quarter_str")["share_compra_agil_pct"].reindex(plot_q_str)
    ax.plot(
        list(x_share),
        series,
        marker="o",
        ms=4,
        lw=2,
        color=bucket_colors[b],
        label=b,
    )
ax.set_title(
    "Share of projects that are Compra Ágil by estimated-cost bucket",
    fontsize=10,
    fontweight="bold",
)
ax.set_xlabel("Quarter")
ax.set_ylabel("Share that is Compra Ágil (%)")
ax.set_xticks(list(x_share))
ax.set_xticklabels(plot_q_str, **xtick_kw)
ax.set_ylim(0, 100)
ax.yaxis.set_major_formatter(mtick.PercentFormatter(decimals=0))
ax.legend(fontsize=9, title="Estimated cost bucket")
ax.grid(axis="y", alpha=0.3)
ax.spines[["top", "right"]].set_visible(False)
fig.suptitle(
    "Compra Ágil penetration over time by UTM bucket (appended projects)",
    fontsize=12,
    fontweight="bold",
)
savefig("ca_D4_share_compra_agil_by_utm_bucket.png")

# D13-D14: Monthly stacked counts and values by UTM bucket
print("  Building monthly stacked bars by UTM bucket (counts and values)")
utm_month = utm_base.copy()
utm_month["month"] = utm_month["fecha_pub"].dt.to_period("M")
utm_month = utm_month[utm_month["month"].notna()].copy()

stack_bucket_bins = [0, 30, 100, 500, np.inf]
stack_bucket_labels = ["0-30 UTM", "30-100 UTM", "100-500 UTM", "500+ UTM"]
stack_bucket_colors = {
    "0-30 UTM": "#1f77b4",
    "30-100 UTM": "#ff7f0e",
    "100-500 UTM": "#2ca02c",
    "500+ UTM": "#9467bd",
}
stack_dataset_colors = {"licitaciones": "#1f77b4", "compra_agil": "#d62728"}
stack_dataset_labels = {
    "licitaciones": "Licitaciones",
    "compra_agil": "Compra Ágil",
}
stack_bucket_slug = {
    "0-30 UTM": "0_30utm",
    "30-100 UTM": "30_100utm",
    "100-500 UTM": "100_500utm",
    "500+ UTM": "500_plus_utm",
}
utm_month["utm_bucket_4"] = pd.cut(
    utm_month["monto_utm"],
    bins=stack_bucket_bins,
    labels=stack_bucket_labels,
    include_lowest=True,
    right=True,
)
utm_month = utm_month[utm_month["utm_bucket_4"].notna()].copy()

monthly_counts = (
    utm_month.groupby(["month", "utm_bucket_4", "dataset"], observed=True)["tender_id"]
    .nunique()
    .reset_index(name="n_tenders")
)
monthly_counts["month_str"] = monthly_counts["month"].astype(str)
monthly_counts = monthly_counts.sort_values(["month", "utm_bucket_4", "dataset"]).copy()
monthly_counts.to_csv(
    SUMMARY_DIR / "ca_monthly_tender_counts_by_dataset_utm_bucket.csv",
    index=False,
)
print("  Saved: ca_monthly_tender_counts_by_dataset_utm_bucket.csv")

monthly_values = (
    utm_month.groupby(["month", "utm_bucket_4", "dataset"], observed=True)["monto_estimado"]
    .sum()
    .reset_index(name="total_value_clp")
)
monthly_values["total_value_bn_clp"] = monthly_values["total_value_clp"] / 1e9
monthly_values["month_str"] = monthly_values["month"].astype(str)
monthly_values = monthly_values.sort_values(["month", "utm_bucket_4", "dataset"]).copy()
monthly_values.to_csv(
    SUMMARY_DIR / "ca_monthly_tender_values_by_dataset_utm_bucket.csv",
    index=False,
)
print("  Saved: ca_monthly_tender_values_by_dataset_utm_bucket.csv")

all_months = sorted(utm_month["month"].dropna().unique())
all_months_str = [str(m) for m in all_months]
x_month = list(range(len(all_months_str)))
month_tick_kw = dict(rotation=45, ha="right", fontsize=7)

for b in stack_bucket_labels:
    sub_counts = monthly_counts[monthly_counts["utm_bucket_4"].astype(str) == b].copy()
    c_wide = sub_counts.pivot_table(
        index="month_str",
        columns="dataset",
        values="n_tenders",
        aggfunc="sum",
        fill_value=0,
    ).reindex(all_months_str).fillna(0)
    for col in ["licitaciones", "compra_agil"]:
        if col not in c_wide.columns:
            c_wide[col] = 0
    c_wide = c_wide[["licitaciones", "compra_agil"]]

    fig, ax = plt.subplots(figsize=(16, 6))
    ax.bar(
        x_month,
        c_wide["licitaciones"].to_numpy(),
        color=stack_dataset_colors["licitaciones"],
        alpha=0.9,
        label=stack_dataset_labels["licitaciones"],
    )
    ax.bar(
        x_month,
        c_wide["compra_agil"].to_numpy(),
        bottom=c_wide["licitaciones"].to_numpy(),
        color=stack_dataset_colors["compra_agil"],
        alpha=0.9,
        label=stack_dataset_labels["compra_agil"],
    )
    ax.set_title(f"{b}: monthly tender counts by dataset (stacked)", fontsize=10, fontweight="bold")
    ax.set_xlabel("Month")
    ax.set_ylabel("Number of tenders")
    ax.set_xticks(x_month)
    ax.set_xticklabels(all_months_str, **month_tick_kw)
    ax.yaxis.set_major_formatter(mtick.FuncFormatter(lambda x, _: f"{int(x):,}"))
    ax.grid(axis="y", alpha=0.3)
    ax.legend(fontsize=9)
    ax.spines[["top", "right"]].set_visible(False)
    fig.suptitle(
        f"Tender counts by dataset in {b} projects (monthly, stacked)",
        fontsize=12,
        fontweight="bold",
    )
    savefig(f"ca_D13_monthly_tender_counts_stacked_{stack_bucket_slug[b]}.png")

    sub_values = monthly_values[monthly_values["utm_bucket_4"].astype(str) == b].copy()
    v_wide = sub_values.pivot_table(
        index="month_str",
        columns="dataset",
        values="total_value_bn_clp",
        aggfunc="sum",
        fill_value=0.0,
    ).reindex(all_months_str).fillna(0.0)
    for col in ["licitaciones", "compra_agil"]:
        if col not in v_wide.columns:
            v_wide[col] = 0.0
    v_wide = v_wide[["licitaciones", "compra_agil"]]

    fig, ax = plt.subplots(figsize=(16, 6))
    ax.bar(
        x_month,
        v_wide["licitaciones"].to_numpy(),
        color=stack_dataset_colors["licitaciones"],
        alpha=0.9,
        label=stack_dataset_labels["licitaciones"],
    )
    ax.bar(
        x_month,
        v_wide["compra_agil"].to_numpy(),
        bottom=v_wide["licitaciones"].to_numpy(),
        color=stack_dataset_colors["compra_agil"],
        alpha=0.9,
        label=stack_dataset_labels["compra_agil"],
    )
    ax.set_title(f"{b}: monthly total estimated value by dataset (stacked)", fontsize=10, fontweight="bold")
    ax.set_xlabel("Month")
    ax.set_ylabel("Total estimated value (bn CLP)")
    ax.set_xticks(x_month)
    ax.set_xticklabels(all_months_str, **month_tick_kw)
    ax.yaxis.set_major_formatter(mtick.FuncFormatter(lambda x, _: f"{x:,.1f}"))
    ax.grid(axis="y", alpha=0.3)
    ax.legend(fontsize=9)
    ax.spines[["top", "right"]].set_visible(False)
    fig.suptitle(
        f"Tender value by dataset in {b} projects (monthly, stacked)",
        fontsize=12,
        fontweight="bold",
    )
    savefig(f"ca_D14_monthly_tender_value_stacked_{stack_bucket_slug[b]}.png")

# D5-D7: Bidder-composition shares by estimated-cost UTM bucket
print("  Building bidder-composition share graphs by UTM bucket")
if COMBINED_FILTERED.exists():
    bucket_df = pd.read_parquet(
        COMBINED_FILTERED,
        columns=[
            "dataset",
            "tender_id",
            "fecha_pub",
            "source_year",
            "source_month",
            "monto_estimado",
            "rut_bidder",
            "dv_bidder",
            "rut_bidder_raw",
            "is_selected",
            "same_region",
            "tramoventas",
            "ntrabajadores",
            "tipodecontribuyente",
            "is_key_dup",
        ],
    )
    bucket_df = bucket_df[~bucket_df["is_key_dup"]].copy()
else:
    bucket_df = pd.DataFrame()

if not bucket_df.empty:
    bucket_df["fecha_pub"] = pd.to_datetime(bucket_df["fecha_pub"], errors="coerce")
    bucket_df["quarter"] = bucket_df["fecha_pub"].dt.to_period("Q")
    bucket_df["month"] = bucket_df["fecha_pub"].dt.to_period("M")
    bucket_df["source_year"] = pd.to_numeric(bucket_df["source_year"], errors="coerce")
    bucket_df["source_month"] = pd.to_numeric(bucket_df["source_month"], errors="coerce")
    ym_missing = bucket_df["source_year"].isna() | bucket_df["source_month"].isna()
    bucket_df.loc[ym_missing, "source_year"] = bucket_df.loc[ym_missing, "fecha_pub"].dt.year
    bucket_df.loc[ym_missing, "source_month"] = bucket_df.loc[ym_missing, "fecha_pub"].dt.month
    bucket_df["source_year"] = bucket_df["source_year"].astype("Int64")
    bucket_df["source_month"] = bucket_df["source_month"].astype("Int64")
    bucket_df = bucket_df.merge(utm_tbl, on=["source_year", "source_month"], how="left")
    bucket_df["monto_utm"] = bucket_df["monto_estimado"] / bucket_df["utm_clp_rate"]
    bucket_df = bucket_df[
        bucket_df["quarter"].notna()
        & bucket_df["monto_utm"].notna()
        & np.isfinite(bucket_df["monto_utm"])
        & (bucket_df["monto_utm"] > 0)
    ].copy()

    bucket_bins = [0, 30, 100, 500, np.inf]
    bucket_labels = ["0-30 UTM", "30-100 UTM", "100-500 UTM", "500+ UTM"]
    bucket_colors = {
        "0-30 UTM": "#1f77b4",
        "30-100 UTM": "#ff7f0e",
        "100-500 UTM": "#2ca02c",
        "500+ UTM": "#9467bd",
    }
    bucket_df["utm_bucket"] = pd.cut(
        bucket_df["monto_utm"],
        bins=bucket_bins,
        labels=bucket_labels,
        include_lowest=True,
        right=True,
    )
    bucket_df = bucket_df[bucket_df["utm_bucket"].notna()].copy()

    # One row per (tender, bidder) so bidder counts/shares are not inflated by
    # repeated product-line rows within the same tender.
    bucket_df["same_region_num"] = pd.to_numeric(bucket_df["same_region"], errors="coerce")
    bucket_df["tramoventas_num"] = pd.to_numeric(bucket_df["tramoventas"], errors="coerce")
    bucket_df["ntrabajadores_num"] = pd.to_numeric(bucket_df["ntrabajadores"], errors="coerce")
    bucket_df["is_selected_flag"] = pd.to_numeric(bucket_df["is_selected"], errors="coerce")
    bucket_df["rut_num"], bucket_df["rut_dv"] = _extract_rut_dv(bucket_df)
    bucket_df["bidder_id"] = _build_bidder_id(bucket_df)
    bucket_bidder = (
        bucket_df.groupby(
            ["quarter", "month", "dataset", "tender_id", "utm_bucket", "bidder_id"],
            observed=True,
            sort=False,
        )
        .agg(
            same_region_num=("same_region_num", "first"),
            tramoventas_raw=("tramoventas", "first"),
            tramoventas_num=("tramoventas_num", "first"),
            ntrabajadores_num=("ntrabajadores_num", "first"),
            tipodecontribuyente=("tipodecontribuyente", "first"),
            selected_flag=("is_selected_flag", "max"),
            rut_num=("rut_num", "first"),
            rut_dv=("rut_dv", "first"),
        )
        .reset_index()
    )

    def make_bucket_share_plot(
        data: pd.DataFrame,
        indicator_col: str,
        out_csv: str,
        out_png: str,
        title: str,
        suptitle: str,
        y_label: str,
    ) -> None:
        if data.empty or data[indicator_col].notna().sum() == 0:
            print(f"  [WARN] Skipping {out_png}: no usable rows.")
            return
        q = (
            data.groupby(["quarter", "utm_bucket"], observed=True)
            .agg(
                n_bids=(indicator_col, "size"),
                share_pct=(indicator_col, lambda s: 100.0 * s.mean()),
            )
            .reset_index()
        )
        q["quarter_str"] = q["quarter"].astype(str)
        q = q.sort_values(["quarter", "utm_bucket"]).copy()
        q.to_csv(SUMMARY_DIR / out_csv, index=False)
        print(f"  Saved: {out_csv}")

        plot_q = sorted(q["quarter"].dropna().unique())
        plot_q_str = [str(v) for v in plot_q]
        x_vals = range(len(plot_q_str))

        fig, ax = plt.subplots(figsize=(14, 5))
        for b in bucket_labels:
            sub = q[q["utm_bucket"].astype(str) == b]
            series = sub.set_index("quarter_str")["share_pct"].reindex(plot_q_str)
            ax.plot(
                list(x_vals),
                series,
                marker="o",
                ms=4,
                lw=2,
                color=bucket_colors[b],
                label=b,
            )
        ax.set_title(title, fontsize=10, fontweight="bold")
        ax.set_xlabel("Quarter")
        ax.set_ylabel(y_label)
        ax.set_xticks(list(x_vals))
        ax.set_xticklabels(plot_q_str, **xtick_kw)
        ax.set_ylim(0, 100)
        ax.yaxis.set_major_formatter(mtick.PercentFormatter(decimals=0))
        ax.legend(fontsize=9, title="Estimated cost bucket")
        ax.grid(axis="y", alpha=0.3)
        ax.spines[["top", "right"]].set_visible(False)
        fig.suptitle(suptitle, fontsize=12, fontweight="bold")
        savefig(out_png)

    # D5: Local bidder share
    d5 = bucket_bidder.copy()
    d5 = d5[d5["same_region_num"].isin([0, 1])].copy()
    d5["is_local"] = (d5["same_region_num"] == 1).astype(float)
    make_bucket_share_plot(
        d5,
        "is_local",
        "ca_local_bidder_share_by_utm_bucket_quarter.csv",
        "ca_D5_local_bidder_share_by_utm_bucket.png",
        "Share of local bidders by estimated-cost bucket",
        "Local bidder share over time by UTM bucket (appended bids)",
        "Local bidders (%)",
    )

    # D6: Empresa Grande (Ventas), based on tramoventas.
    d6 = bucket_bidder.copy()
    tv_num = d6["tramoventas_num"]
    d6["is_grande_ventas"] = np.nan
    known_tv = tv_num.between(1, 13)  # 0 or missing = sin información / unknown
    d6.loc[known_tv, "is_grande_ventas"] = (tv_num[known_tv] >= 10).astype(float)
    # Fallback in case tramoventas is stored as text labels.
    tv_txt = d6["tramoventas_raw"].astype(str).str.strip().str.lower()
    still_na = d6["is_grande_ventas"].isna()
    d6.loc[still_na & tv_txt.str.contains("grande", na=False), "is_grande_ventas"] = 1.0
    d6.loc[
        still_na & tv_txt.str.contains("micro|peque|mediana|mipyme", na=False),
        "is_grande_ventas",
    ] = 0.0
    d6 = d6[d6["is_grande_ventas"].notna()].copy()
    make_bucket_share_plot(
        d6,
        "is_grande_ventas",
        "ca_grande_ventas_share_by_utm_bucket_quarter.csv",
        "ca_D6_grande_ventas_share_by_utm_bucket.png",
        "Share of Empresa Grande (Ventas) by estimated-cost bucket",
        "Empresa Grande (Ventas) share over time by UTM bucket (appended bids)",
        "Empresa Grande (Ventas) (%)",
    )

    # D7: Empresa Grande (Empleo), based on ntrabajadores >= 200.
    d7 = bucket_bidder.copy()
    nt = d7["ntrabajadores_num"]
    d7["is_grande_empleo"] = np.where(
        nt.notna() & (nt >= 0),
        (nt >= 200).astype(float),
        np.nan,
    )
    d7 = d7[d7["is_grande_empleo"].notna()].copy()
    make_bucket_share_plot(
        d7,
        "is_grande_empleo",
        "ca_grande_empleo_share_by_utm_bucket_quarter.csv",
        "ca_D7_grande_empleo_share_by_utm_bucket.png",
        "Share of Empresa Grande (Empleo) by estimated-cost bucket",
        "Empresa Grande (Empleo) share over time by UTM bucket (appended bids)",
        "Empresa Grande (Empleo) (%)",
    )

    # D8-D11: Avg # bidders per tender by bidder type, line-only,
    # split by UTM bucket.
    # Size buckets use only SII tramoventas coding.
    d8_base = bucket_bidder.copy()
    tv_num = d8_base["tramoventas_num"]
    is_same = d8_base["same_region_num"] == 1
    is_diff = d8_base["same_region_num"] == 0

    # SII tramoventas mapping:
    #   1..9 = MiPYME, 10..13 = Grande.
    is_mipyme = tv_num.between(1, 9, inclusive="both")
    is_grande = tv_num.between(10, 13, inclusive="both")

    # Proxy for "not merged with SII" on bidder rows.
    has_sii_info = (
        d8_base["tramoventas_num"].notna()
        | d8_base["ntrabajadores_num"].notna()
        | d8_base["tipodecontribuyente"].notna()
    )
    no_sii_merge = ~has_sii_info

    rut_num = pd.to_numeric(d8_base["rut_num"], errors="coerce")
    valid_rut = pd.Series(False, index=d8_base.index)
    if no_sii_merge.any():
        valid_rut.loc[no_sii_merge] = _valid_rut_flag(
            d8_base.loc[no_sii_merge, "rut_num"],
            d8_base.loc[no_sii_merge, "rut_dv"],
        ).to_numpy()
    is_persona_natural = no_sii_merge & valid_rut & rut_num.notna() & (rut_num < 20_000_000)

    # Mutually exclusive bidder-type categories.
    d8_base["grande_local"] = (is_grande & is_same).astype(int)
    d8_base["mipyme_local"] = (is_mipyme & is_same).astype(int)
    d8_base["grande_nolocal"] = (is_grande & is_diff).astype(int)
    d8_base["mipyme_nolocal"] = (is_mipyme & is_diff).astype(int)
    d8_base["persona_natural"] = is_persona_natural.astype(int)
    assigned = (
        d8_base["grande_local"]
        + d8_base["mipyme_local"]
        + d8_base["grande_nolocal"]
        + d8_base["mipyme_nolocal"]
        + d8_base["persona_natural"]
    )
    d8_base["other_rut"] = (assigned == 0).astype(int)

    overlap_n = int((assigned > 1).sum())
    if overlap_n > 0:
        print(f"  [WARN] {overlap_n:,} bidder rows fall in multiple D8 categories.")

    d8_def_summary = pd.DataFrame(
        [
            ("rows_total", len(d8_base)),
            ("rows_no_sii_merge", int(no_sii_merge.sum())),
            ("rows_valid_rut", int(valid_rut.sum())),
            ("rows_persona_natural", int(d8_base["persona_natural"].sum())),
            ("rows_no_clasificables_sii_2024", int(d8_base["other_rut"].sum())),
        ],
        columns=["metric", "value"],
    )
    d8_def_summary.to_csv(
        SUMMARY_DIR / f"ca_bidder_type_definition_summary{BIDDER_TYPE_SUFFIX}.csv",
        index=False,
    )
    print(f"  Saved: ca_bidder_type_definition_summary{BIDDER_TYPE_SUFFIX}.csv")

    d8_cats = [
        ("grande_local", "Empresa Grande - Local", "#d62728"),
        ("mipyme_local", "Empresa MiPYME - Local", "#1f77b4"),
        ("grande_nolocal", "Empresa Grande - No Local", "#e9a8a8"),
        ("mipyme_nolocal", "Empresa MiPYME - No Local", "#9fb9dd"),
        ("persona_natural", "Persona Natural", "#9467bd"),
        ("other_rut", "No clasificables por SII (2024)", "#7f7f7f"),
    ]
    d8_cols = [k for k, _, _ in d8_cats]

    all_periods = sorted(d8_base[BIDDER_TYPE_PERIOD_COL].dropna().unique())
    all_periods_str = [str(v) for v in all_periods]

    def make_bidder_type_lines_by_bucket(bucket_label: str, out_csv: str, out_png: str, panel_tag: str) -> None:
        sub = d8_base[d8_base["utm_bucket"].astype(str) == bucket_label].copy()
        if len(sub) == 0:
            print(f"  [WARN] Skipping {panel_tag} ({bucket_label}): no bidder rows.")
            return

        tender_level = (
            sub.groupby([BIDDER_TYPE_PERIOD_COL, "dataset", "tender_id"], observed=True)[d8_cols]
            .sum()
            .reset_index()
        )
        q = (
            tender_level.groupby(BIDDER_TYPE_PERIOD_COL, observed=True)[d8_cols]
            .mean()
            .reindex(all_periods)
            .reset_index()
        )
        q["period_str"] = q[BIDDER_TYPE_PERIOD_COL].astype(str)
        q["utm_bucket"] = bucket_label
        q["time_frequency"] = BIDDER_TYPE_FREQ
        q.to_csv(SUMMARY_DIR / out_csv, index=False)
        print(f"  Saved: {out_csv}")

        x_vals = range(len(all_periods_str))
        fig, ax = plt.subplots(figsize=(14, 6))
        for key, label, color in d8_cats:
            ax.plot(
                list(x_vals),
                q[key].to_numpy(),
                marker="o",
                ms=4,
                lw=2,
                color=color,
                label=label,
            )
        ax.set_title(f"{bucket_label}: avg # bidders per tender by bidder type", fontweight="bold")
        ax.set_ylabel("Avg # bidders per tender")
        ax.set_xlabel(BIDDER_TYPE_PERIOD_LABEL)
        ax.set_xticks(list(x_vals))
        ax.set_xticklabels(all_periods_str, **xtick_kw)
        ax.grid(axis="y", alpha=0.3)
        ax.legend(fontsize=9, ncol=2)
        ax.spines[["top", "right"]].set_visible(False)
        fig.suptitle(
            f"Bidder composition in {bucket_label} tenders ({BIDDER_TYPE_PLOT_LABEL} averages)",
            fontsize=12,
            fontweight="bold",
        )
        savefig(out_png)

    make_bidder_type_lines_by_bucket(
        "30-100 UTM",
        f"ca_bidder_types_avg_count_30_100utm{BIDDER_TYPE_SUFFIX}.csv",
        f"ca_D8_bidder_types_avg_count_30_100utm{BIDDER_TYPE_SUFFIX}.png",
        "D8",
    )
    make_bidder_type_lines_by_bucket(
        "0-30 UTM",
        f"ca_bidder_types_avg_count_0_30utm{BIDDER_TYPE_SUFFIX}.csv",
        f"ca_D9_bidder_types_avg_count_0_30utm{BIDDER_TYPE_SUFFIX}.png",
        "D9",
    )
    make_bidder_type_lines_by_bucket(
        "100-500 UTM",
        f"ca_bidder_types_avg_count_100_500utm{BIDDER_TYPE_SUFFIX}.csv",
        f"ca_D10_bidder_types_avg_count_100_500utm{BIDDER_TYPE_SUFFIX}.png",
        "D10",
    )
    make_bidder_type_lines_by_bucket(
        "500+ UTM",
        f"ca_bidder_types_avg_count_500_plus_utm{BIDDER_TYPE_SUFFIX}.csv",
        f"ca_D11_bidder_types_avg_count_500_plus_utm{BIDDER_TYPE_SUFFIX}.png",
        "D11",
    )

    # D16-D19: Avg # winners per tender by winner type, line-only,
    # split by UTM bucket.
    def make_winner_type_lines_by_bucket(bucket_label: str, out_csv: str, out_png: str, panel_tag: str) -> None:
        sub_all = d8_base[d8_base["utm_bucket"].astype(str) == bucket_label].copy()
        if len(sub_all) == 0:
            print(f"  [WARN] Skipping {panel_tag} ({bucket_label}): no bidder rows.")
            return

        winners = sub_all[sub_all["selected_flag"] == 1].copy()
        win_counts = (
            winners.groupby([BIDDER_TYPE_PERIOD_COL, "dataset", "tender_id"], observed=True)[d8_cols]
            .sum()
            .reset_index()
        )
        all_tenders = sub_all[[BIDDER_TYPE_PERIOD_COL, "dataset", "tender_id"]].drop_duplicates()
        tender_level = all_tenders.merge(
            win_counts,
            on=[BIDDER_TYPE_PERIOD_COL, "dataset", "tender_id"],
            how="left",
        )
        tender_level[d8_cols] = tender_level[d8_cols].fillna(0.0)

        q = (
            tender_level.groupby(BIDDER_TYPE_PERIOD_COL, observed=True)[d8_cols]
            .mean()
            .reindex(all_periods)
            .reset_index()
        )
        q["period_str"] = q[BIDDER_TYPE_PERIOD_COL].astype(str)
        q["utm_bucket"] = bucket_label
        q["time_frequency"] = BIDDER_TYPE_FREQ
        q.to_csv(SUMMARY_DIR / out_csv, index=False)
        print(f"  Saved: {out_csv}")

        x_vals = range(len(all_periods_str))
        fig, ax = plt.subplots(figsize=(14, 6))
        for key, label, color in d8_cats:
            ax.plot(
                list(x_vals),
                q[key].to_numpy(),
                marker="o",
                ms=4,
                lw=2,
                color=color,
                label=label,
            )
        ax.set_title(f"{bucket_label}: avg # winning bidders per tender by winner type", fontweight="bold")
        ax.set_ylabel("Avg # winning bidders per tender")
        ax.set_xlabel(BIDDER_TYPE_PERIOD_LABEL)
        ax.set_xticks(list(x_vals))
        ax.set_xticklabels(all_periods_str, **xtick_kw)
        ax.grid(axis="y", alpha=0.3)
        ax.legend(fontsize=9, ncol=2)
        ax.spines[["top", "right"]].set_visible(False)
        fig.suptitle(
            f"Winner composition in {bucket_label} tenders ({BIDDER_TYPE_PLOT_LABEL} averages)",
            fontsize=12,
            fontweight="bold",
        )
        savefig(out_png)

    make_winner_type_lines_by_bucket(
        "30-100 UTM",
        f"ca_winner_types_avg_count_30_100utm{BIDDER_TYPE_SUFFIX}.csv",
        f"ca_D16_winner_types_avg_count_30_100utm{BIDDER_TYPE_SUFFIX}.png",
        "D16",
    )
    make_winner_type_lines_by_bucket(
        "0-30 UTM",
        f"ca_winner_types_avg_count_0_30utm{BIDDER_TYPE_SUFFIX}.csv",
        f"ca_D17_winner_types_avg_count_0_30utm{BIDDER_TYPE_SUFFIX}.png",
        "D17",
    )
    make_winner_type_lines_by_bucket(
        "100-500 UTM",
        f"ca_winner_types_avg_count_100_500utm{BIDDER_TYPE_SUFFIX}.csv",
        f"ca_D18_winner_types_avg_count_100_500utm{BIDDER_TYPE_SUFFIX}.png",
        "D18",
    )
    make_winner_type_lines_by_bucket(
        "500+ UTM",
        f"ca_winner_types_avg_count_500_plus_utm{BIDDER_TYPE_SUFFIX}.csv",
        f"ca_D19_winner_types_avg_count_500_plus_utm{BIDDER_TYPE_SUFFIX}.png",
        "D19",
    )

    # D20-D27: Within-tender type shares (bidders and winners), stacked to 100%.
    share_cols = [f"{c}_share" for c in d8_cols]
    share_map = dict(zip(d8_cols, share_cols))

    def _stacked_share_plot(
        q_share: pd.DataFrame,
        out_png: str,
        title: str,
        suptitle: str,
        y_label: str,
    ) -> None:
        x_vals = range(len(all_periods_str))
        fig, ax = plt.subplots(figsize=(14, 6))
        bottom = np.zeros(len(all_periods_str), dtype=float)
        for key, label, color in d8_cats:
            share_key = share_map[key]
            vals = (
                q_share[share_key].to_numpy(dtype=float) * 100.0
                if share_key in q_share.columns
                else np.full(len(all_periods_str), np.nan)
            )
            vals_plot = np.nan_to_num(vals, nan=0.0)
            ax.bar(
                list(x_vals),
                vals_plot,
                bottom=bottom,
                width=0.85,
                color=color,
                alpha=0.95,
                label=label,
            )
            bottom = bottom + vals_plot
        ax.set_title(title, fontweight="bold")
        ax.set_ylabel(y_label)
        ax.set_xlabel(BIDDER_TYPE_PERIOD_LABEL)
        ax.set_xticks(list(x_vals))
        ax.set_xticklabels(all_periods_str, **xtick_kw)
        ax.set_ylim(0, 100)
        ax.yaxis.set_major_formatter(mtick.PercentFormatter(decimals=0))
        ax.grid(axis="y", alpha=0.3)
        ax.legend(fontsize=9, ncol=2)
        ax.spines[["top", "right"]].set_visible(False)
        fig.suptitle(suptitle, fontsize=12, fontweight="bold")
        savefig(out_png)

    def make_bidder_type_share_stacked_by_bucket(bucket_label: str, out_csv: str, out_png: str, panel_tag: str) -> None:
        sub = d8_base[d8_base["utm_bucket"].astype(str) == bucket_label].copy()
        if len(sub) == 0:
            print(f"  [WARN] Skipping {panel_tag} ({bucket_label}): no bidder rows.")
            return

        tender_counts = (
            sub.groupby([BIDDER_TYPE_PERIOD_COL, "dataset", "tender_id"], observed=True)[d8_cols]
            .sum()
            .reset_index()
        )
        total_counts = tender_counts[d8_cols].sum(axis=1)
        for c in d8_cols:
            tender_counts[share_map[c]] = np.where(total_counts > 0, tender_counts[c] / total_counts, np.nan)

        q = (
            tender_counts.groupby(BIDDER_TYPE_PERIOD_COL, observed=True)[share_cols]
            .mean()
            .reindex(all_periods)
            .reset_index()
        )
        q["period_str"] = q[BIDDER_TYPE_PERIOD_COL].astype(str)
        q["utm_bucket"] = bucket_label
        q["time_frequency"] = BIDDER_TYPE_FREQ
        q["share_sum_pct"] = 100.0 * q[share_cols].sum(axis=1)
        q.to_csv(SUMMARY_DIR / out_csv, index=False)
        print(f"  Saved: {out_csv}")

        _stacked_share_plot(
            q,
            out_png,
            f"{bucket_label}: bidder-type share within tender (stacked)",
            f"Bidder composition shares in {bucket_label} tenders ({BIDDER_TYPE_PLOT_LABEL})",
            "Average share within tender (%)",
        )

    def make_winner_type_share_stacked_by_bucket(bucket_label: str, out_csv: str, out_png: str, panel_tag: str) -> None:
        sub_all = d8_base[d8_base["utm_bucket"].astype(str) == bucket_label].copy()
        if len(sub_all) == 0:
            print(f"  [WARN] Skipping {panel_tag} ({bucket_label}): no bidder rows.")
            return
        winners = sub_all[sub_all["selected_flag"] == 1].copy()
        if len(winners) == 0:
            print(f"  [WARN] Skipping {panel_tag} ({bucket_label}): no winner rows.")
            return

        win_counts = (
            winners.groupby([BIDDER_TYPE_PERIOD_COL, "dataset", "tender_id"], observed=True)[d8_cols]
            .sum()
            .reset_index()
        )
        total_winners = win_counts[d8_cols].sum(axis=1)
        win_counts = win_counts[total_winners > 0].copy()
        total_winners = win_counts[d8_cols].sum(axis=1)
        for c in d8_cols:
            win_counts[share_map[c]] = win_counts[c] / total_winners

        q = (
            win_counts.groupby(BIDDER_TYPE_PERIOD_COL, observed=True)[share_cols]
            .mean()
            .reindex(all_periods)
            .reset_index()
        )
        q["period_str"] = q[BIDDER_TYPE_PERIOD_COL].astype(str)
        q["utm_bucket"] = bucket_label
        q["time_frequency"] = BIDDER_TYPE_FREQ
        q["share_sum_pct"] = 100.0 * q[share_cols].sum(axis=1)
        q.to_csv(SUMMARY_DIR / out_csv, index=False)
        print(f"  Saved: {out_csv}")

        _stacked_share_plot(
            q,
            out_png,
            f"{bucket_label}: winner-type share within tender (stacked)",
            f"Winner composition shares in {bucket_label} tenders ({BIDDER_TYPE_PLOT_LABEL})",
            "Average share within tender (%)",
        )

    make_bidder_type_share_stacked_by_bucket(
        "30-100 UTM",
        f"ca_bidder_type_share_30_100utm{BIDDER_TYPE_SUFFIX}.csv",
        f"ca_D20_bidder_type_share_30_100utm{BIDDER_TYPE_SUFFIX}.png",
        "D20",
    )
    make_bidder_type_share_stacked_by_bucket(
        "0-30 UTM",
        f"ca_bidder_type_share_0_30utm{BIDDER_TYPE_SUFFIX}.csv",
        f"ca_D21_bidder_type_share_0_30utm{BIDDER_TYPE_SUFFIX}.png",
        "D21",
    )
    make_bidder_type_share_stacked_by_bucket(
        "100-500 UTM",
        f"ca_bidder_type_share_100_500utm{BIDDER_TYPE_SUFFIX}.csv",
        f"ca_D22_bidder_type_share_100_500utm{BIDDER_TYPE_SUFFIX}.png",
        "D22",
    )
    make_bidder_type_share_stacked_by_bucket(
        "500+ UTM",
        f"ca_bidder_type_share_500_plus_utm{BIDDER_TYPE_SUFFIX}.csv",
        f"ca_D23_bidder_type_share_500_plus_utm{BIDDER_TYPE_SUFFIX}.png",
        "D23",
    )

    make_winner_type_share_stacked_by_bucket(
        "30-100 UTM",
        f"ca_winner_type_share_30_100utm{BIDDER_TYPE_SUFFIX}.csv",
        f"ca_D24_winner_type_share_30_100utm{BIDDER_TYPE_SUFFIX}.png",
        "D24",
    )
    make_winner_type_share_stacked_by_bucket(
        "0-30 UTM",
        f"ca_winner_type_share_0_30utm{BIDDER_TYPE_SUFFIX}.csv",
        f"ca_D25_winner_type_share_0_30utm{BIDDER_TYPE_SUFFIX}.png",
        "D25",
    )
    make_winner_type_share_stacked_by_bucket(
        "100-500 UTM",
        f"ca_winner_type_share_100_500utm{BIDDER_TYPE_SUFFIX}.csv",
        f"ca_D26_winner_type_share_100_500utm{BIDDER_TYPE_SUFFIX}.png",
        "D26",
    )
    make_winner_type_share_stacked_by_bucket(
        "500+ UTM",
        f"ca_winner_type_share_500_plus_utm{BIDDER_TYPE_SUFFIX}.csv",
        f"ca_D27_winner_type_share_500_plus_utm{BIDDER_TYPE_SUFFIX}.png",
        "D27",
    )

    # D12: Single chart — avg # bidders per tender by estimated-cost bucket.
    # Compute bucket-by-bucket to keep memory bounded in monthly mode.
    d12_wide = pd.DataFrame(index=all_periods_str)
    for b in bucket_labels:
        sub = d8_base[d8_base["utm_bucket"].astype(str) == b].copy()
        if len(sub) == 0:
            d12_wide[b] = np.nan
            continue
        tender_counts = (
            sub.groupby([BIDDER_TYPE_PERIOD_COL, "dataset", "tender_id"], observed=True)
            .size()
            .reset_index(name="n_bidders")
        )
        avg_series = (
            tender_counts.groupby(BIDDER_TYPE_PERIOD_COL, observed=True)["n_bidders"]
            .mean()
            .reindex(all_periods)
        )
        avg_series.index = avg_series.index.astype(str)
        d12_wide[b] = avg_series.reindex(all_periods_str).to_numpy()

    d12 = (
        d12_wide.reset_index()
        .rename(columns={"index": "period_str"})
        .melt(id_vars=["period_str"], var_name="utm_bucket", value_name="avg_bidders_per_tender")
        .sort_values(["period_str", "utm_bucket"])
    )
    d12["time_frequency"] = BIDDER_TYPE_FREQ
    d12.to_csv(SUMMARY_DIR / f"ca_avg_bidders_per_tender_by_utm_bucket{BIDDER_TYPE_SUFFIX}.csv", index=False)
    print(f"  Saved: ca_avg_bidders_per_tender_by_utm_bucket{BIDDER_TYPE_SUFFIX}.csv")

    fig, ax = plt.subplots(figsize=(14, 6))
    for b in bucket_labels:
        ax.plot(
            list(range(len(all_periods_str))),
            d12_wide[b].to_numpy() if b in d12_wide.columns else np.full(len(all_periods_str), np.nan),
            marker="o",
            ms=4,
            lw=2,
            color=bucket_colors[b],
            label=b,
        )
    ax.set_title("Avg # bidders per tender by estimated-cost bucket", fontweight="bold")
    ax.set_xlabel(BIDDER_TYPE_PERIOD_LABEL)
    ax.set_ylabel("Avg # bidders per tender")
    ax.set_xticks(list(range(len(all_periods_str))))
    ax.set_xticklabels(all_periods_str, **xtick_kw)
    ax.grid(axis="y", alpha=0.3)
    ax.legend(fontsize=9, title="Estimated cost bucket")
    ax.spines[["top", "right"]].set_visible(False)
    fig.suptitle(
        f"Average bidders per tender by estimated cost ({BIDDER_TYPE_PLOT_LABEL})",
        fontsize=12,
        fontweight="bold",
    )
    savefig(f"ca_D12_avg_bidders_by_utm_bucket{BIDDER_TYPE_SUFFIX}.png")
else:
    print("  [WARN] Could not build D5-D12 bidder graphs (missing combined filtered panel).")

# ── Combined summary figure ────────────────────────────────────────────────
fig, axes = plt.subplots(2, 3, figsize=(20, 10))

# Top-left: volume
ax = axes[0,0]
ax.bar(list(x_idx), ca_vals.reindex(all_q_str).fillna(0),
       color="#1f77b4", alpha=0.85)
ax.set_title("(A) Cotización volume by quarter", fontweight="bold")
ax.set_xticks(list(x_idx)); ax.set_xticklabels(all_q_str, **xtick_kw)
ax.yaxis.set_major_formatter(mtick.FuncFormatter(lambda x,_: f"{int(x):,}"))
ax.grid(axis="y", alpha=0.3); ax.spines[["top","right"]].set_visible(False)

# Top-center: MiPyme share
ax = axes[0,1]
for label, color in [("MiPyme","#1f77b4"),("Grande","#d62728")]:
    sub = bids_q[bids_q["Tamano"]==label].set_index("quarter_str")["share"].reindex(all_q_str)
    ax.plot(list(x_idx), sub, marker="o", ms=3, lw=2, color=color, label=label)
ax.set_title("(B) Bidder size share", fontweight="bold")
ax.yaxis.set_major_formatter(mtick.PercentFormatter(decimals=0))
ax.set_xticks(list(x_idx)); ax.set_xticklabels(all_q_str, **xtick_kw)
ax.legend(fontsize=9); ax.grid(axis="y", alpha=0.3)
ax.spines[["top","right"]].set_visible(False)

# Top-right: selection rate
ax = axes[0,2]
for label, color in [("MiPyme","#1f77b4"),("Grande","#d62728")]:
    sub = sel_q[sel_q["Tamano"]==label].set_index("quarter_str")["is_selected"].reindex(all_q_str) * 100
    ax.plot(list(x_idx), sub, marker="o", ms=3, lw=2, color=color, label=label)
ax.set_title("(C) Selection rate by size", fontweight="bold")
ax.yaxis.set_major_formatter(mtick.PercentFormatter(decimals=1))
ax.set_xticks(list(x_idx)); ax.set_xticklabels(all_q_str, **xtick_kw)
ax.legend(fontsize=9); ax.grid(axis="y", alpha=0.3)
ax.spines[["top","right"]].set_visible(False)

# Bottom-left: award rate
ax = axes[1,0]
ax.plot(list(x_idx), vals_aw, marker="o", ms=3, lw=2, color="#2ca02c")
ax.set_title("(D) Award rate", fontweight="bold")
ax.yaxis.set_major_formatter(mtick.PercentFormatter(decimals=0))
ax.set_ylim(0, 100)
ax.set_xticks(list(x_idx)); ax.set_xticklabels(all_q_str, **xtick_kw)
ax.grid(axis="y", alpha=0.3); ax.spines[["top","right"]].set_visible(False)

# Bottom-center: volume vs lic
ax = axes[1,1]
ax.plot(list(x_both), lic_vals, marker="o", ms=3, lw=2, color="#1f77b4",
        label="Licitaciones")
ax.plot(list(x_both), ca_vals.reindex(all_q_both_str).fillna(0),
        marker="s", ms=3, lw=2, color="#d62728", label="Compra Ágil")
ax.set_title("(E) Volume vs Licitaciones", fontweight="bold")
ax.set_xticks(list(x_both)); ax.set_xticklabels(all_q_both_str, **xtick_kw)
ax.yaxis.set_major_formatter(mtick.FuncFormatter(lambda x,_: f"{int(x):,}"))
ax.legend(fontsize=8); ax.grid(axis="y", alpha=0.3)
ax.spines[["top","right"]].set_visible(False)

# Bottom-right: budget vs lic
ax = axes[1,2]
ax.plot(list(x_both), lic_bv / 1e9, marker="o", ms=3, lw=2, color="#1f77b4",
        label="Licitaciones")
ax.plot(list(x_both), ca_bv.reindex(all_q_both_str).fillna(0) / 1e9,
        marker="s", ms=3, lw=2, color="#d62728", label="Compra Ágil")
ax.set_title("(F) Budget vs Licitaciones (bn CLP)", fontweight="bold")
ax.set_xticks(list(x_both)); ax.set_xticklabels(all_q_both_str, **xtick_kw)
ax.legend(fontsize=8); ax.grid(axis="y", alpha=0.3)
ax.spines[["top","right"]].set_visible(False)

fig.suptitle("Compra Ágil — Summary Diagnostics", fontsize=14, fontweight="bold")
savefig("ca_summary.png")

# ── Print summary stats table ──────────────────────────────────────────────
print("\n" + "=" * 70)
print("COMPRA ÁGIL SUMMARY STATISTICS")
print("=" * 70)
print(f"  Total bid rows:              {len(ca):,}")
print(f"  Unique cotizaciones:         {ca['CodigoCotizacion'].nunique():,}")
print(f"  Unique buyer units (RUT):    {ca['RUTUnidaddeCompra'].nunique():,}")
print(f"  Unique bidder RUTs:          {ca['RUTProveedor'].nunique():,}")
print(f"  Date range (pub):            {ca['FechaPublicacionParaCotizar'].min().date()} – {ca['FechaPublicacionParaCotizar'].max().date()}")
print(f"  MiPyme bids share:           {100*(ca['Tamano']=='MiPyme').mean():.1f}%")
print(f"  Grande bids share:           {100*(ca['Tamano']=='Grande').mean():.1f}%")
print(f"  Selected bid rate:           {100*ca['is_selected'].mean():.1f}%")
print(f"  Cotizaciones with award:     {100*cot['has_award'].mean():.1f}%")
print(f"  Median bid amount (CLP):     {ca['MontoTotal'].median():,.0f}")
print()
print(f"  CA buyer overlap with lic:   {n_overlap:,}/{n_ca_buyers:,} ({100*n_overlap/max(n_ca_buyers,1):.1f}%)")
print()
print("All done.")
