"""
08_distance_moderator.py
─────────────────────────────────────────────────────────────────────────────
Add "distance from Santiago" as an exogenous geographic moderator.

Motivation
  Distance proxies for bilateral travel cost (δ·d in the spatial auction
  model).  Regions close to Santiago have low travel costs → Santiago firms
  bid there frequently pre-reform → high nonlocal_share_pre → larger
  exclusion effect post-reform.  Because distance is fixed and predetermined,
  it is arguably a more exogenous moderator than the endogenous pre-reform
  nonlocal share.

Steps
  1. Hard-code Chilean regional centroids (capital city lat/lon).
     Optional: load commune-level centroids from INE CSV if available.
  2. Compute Haversine distance matrix (16×16 regions).
  3. Extract dist_from_santiago_r for each region.
  4. Re-run the moderator descriptives from 07 with distance added.
  5. Binscatters:
       dist_from_santiago × nonlocal_share_pre
       dist_from_santiago × q_pre
       dist_from_santiago × n_pot_local
  6. OLS first-stage: nonlocal_share_pre ~ log(dist) + log(q_pre)
     to show that distance explains the nonlocal share cross-section.
  7. Save dist_from_santiago_r as a CSV for use in
     05_heterogeneity_region.py (run that script afterward with the
     new moderator appended).

Outputs
  output/did/tables/region_distances_munic.csv     (16×16 distance matrix)
  output/did/tables/moderator_with_distance.csv    (region-level moderators + dist)
  output/did/figures/binscatter_dist_nonlocal_munic.png
  output/did/figures/binscatter_dist_moderators_munic.png

Commune-level extension
  If you have a CSV with Chilean commune centroids (columns: nombre_comuna,
  lat, lon), set COMMUNE_CENTROID_CSV below. The script will compute
  dist_from_santiago for each commune and save it as
  output/did/tables/commune_distances.csv, ready to merge on
  ComunaUnidad or rut_unidad via a crosswalk.
  Such CSVs are freely available from INE Chile:
    https://www.ine.gob.cl/herramientas/portal-de-mapas/geodatos-abiertos
  or from the geocl/comunas repository on GitHub.
"""

from __future__ import annotations

import sys
from math import asin, cos, radians, sin, sqrt
from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
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
    load_utm_table,
    add_utm_value,
)

# ════════════════════════════════════════════════════════════════════════════
# ── Configuration ─────────────────────────────────────────────────────────
# ════════════════════════════════════════════════════════════════════════════
SUBSAMPLE       = "munic"
SUBSAMPLE_KW    = "municipal"
PRE_CUTOFF      = pd.Timestamp("2024-12-01")
TREAT_UTM_MIN   = 30.0
TREAT_UTM_MAX   = 100.0
MIN_PRE_TENDERS = 30

# ── Optional: path to commune centroid CSV ────────────────────────────────────
# Expected columns: nombre_comuna (str), lat (float), lon (float)
# Leave as None to skip the commune-level analysis.
# Example: COMMUNE_CENTROID_CSV = Path("data/raw/other/comunas_centroids.csv")
COMMUNE_CENTROID_CSV = None


# ════════════════════════════════════════════════════════════════════════════
# ── Chilean regional centroids (capital city coordinates) ─────────────────
# Source: Google Maps / Wikipedia verified to ±0.1°
# ════════════════════════════════════════════════════════════════════════════
REGION_CENTROIDS = {
    # region_buyer string (as it appears in the ChileCompra data) : (lat, lon)
    "Arica y Parinacota":            (-18.478, -70.322),
    "Tarapacá":                       (-20.213, -70.152),
    "Antofagasta":                    (-23.652, -70.396),
    "Atacama":                        (-27.366, -70.329),
    "Coquimbo":                       (-29.909, -71.254),
    "Valparaíso":                     (-33.047, -71.619),
    "Metropolitana de Santiago":      (-33.459, -70.648),
    "Libertador General Bernardo O'Higgins": (-34.170, -70.744),
    "Maule":                          (-35.426, -71.672),
    "Ñuble":                          (-36.607, -72.103),
    "Biobío":                         (-36.827, -73.049),
    "La Araucanía":                   (-38.739, -72.590),
    "Los Ríos":                       (-39.814, -73.245),
    "Los Lagos":                      (-41.472, -72.936),
    "Aysén del General Carlos Ibáñez del Campo": (-45.571, -72.066),
    "Magallanes y de la Antártica Chilena": (-53.164, -70.911),
}

# Alternate spellings that appear in the data
_REGION_ALIASES = {
    "Región de Arica y Parinacota":            "Arica y Parinacota",
    "Región de Tarapacá":                       "Tarapacá",
    "Región de Antofagasta":                    "Antofagasta",
    "Región de Atacama":                        "Atacama",
    "Región de Coquimbo":                       "Coquimbo",
    "Región de Valparaíso":                     "Valparaíso",
    "Región Metropolitana de Santiago":         "Metropolitana de Santiago",
    "Región del Libertador General Bernardo O'Higgins": "Libertador General Bernardo O'Higgins",
    "Región del Libertador General Bernardo O\u2019Higgins": "Libertador General Bernardo O'Higgins",
    "Región del Maule":                         "Maule",
    "Región de Ñuble":                          "Ñuble",
    "Región del Biobío":                        "Biobío",
    "Biob\u00edo":                              "Biobío",
    "Región de La Araucanía":                   "La Araucanía",
    "Araucan\u00eda":                           "La Araucanía",
    "La Araucan\u00eda":                        "La Araucanía",
    "Región de Los Ríos":                       "Los Ríos",
    "Los R\u00edos":                            "Los Ríos",
    "Región de Los Lagos":                      "Los Lagos",
    "Región de Aysén del General Carlos Ibáñez del Campo": "Aysén del General Carlos Ibáñez del Campo",
    "Aysén del General Carlos Ibáñez del Campo": "Aysén del General Carlos Ibáñez del Campo",
    "Ays\u00e9n del General Carlos Ib\u00e1\u00f1ez del Campo": "Aysén del General Carlos Ibáñez del Campo",
    "Aysen del General Carlos Ibanez del Campo": "Aysén del General Carlos Ibáñez del Campo",
    "Región de Magallanes y de la Antártica Chilena": "Magallanes y de la Antártica Chilena",
    "Magallanes y de la Ant\u00e1rtica Chilena": "Magallanes y de la Antártica Chilena",
    "Magallanes y de la Antartica Chilena":    "Magallanes y de la Antártica Chilena",
    "N\u00f1uble":                              "Ñuble",
}

SANTIAGO_KEY = "Metropolitana de Santiago"


def _normalize_region(name: str) -> str:
    """Return the canonical region key used in REGION_CENTROIDS."""
    n = str(name).strip()
    if n in REGION_CENTROIDS:
        return n
    if n in _REGION_ALIASES:
        return _REGION_ALIASES[n]
    # Fuzzy: strip common prefix
    for pfx in ("Región de los ", "Región de las ", "Región del ",
                 "Región de la ", "Región de ", "Región "):
        if n.startswith(pfx):
            short = n[len(pfx):]
            if short in REGION_CENTROIDS:
                return short
            if short in _REGION_ALIASES:
                return _REGION_ALIASES[short]
    return n  # return as-is (will be NaN if not matched)


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in km between two (lat, lon) points."""
    R = 6371.0
    φ1, φ2 = radians(lat1), radians(lat2)
    Δφ = radians(lat2 - lat1)
    Δλ = radians(lon2 - lon1)
    a = sin(Δφ / 2)**2 + cos(φ1) * cos(φ2) * sin(Δλ / 2)**2
    return 2 * R * asin(sqrt(a))


# ════════════════════════════════════════════════════════════════════════════
# STEP 1: Build region-level distance matrix
# ════════════════════════════════════════════════════════════════════════════
print("\n" + "="*70)
print("STEP 1: Region-level distance matrix")
print("="*70)

regions = list(REGION_CENTROIDS.keys())
n_r     = len(regions)

dist_matrix = np.zeros((n_r, n_r))
for i, r1 in enumerate(regions):
    for j, r2 in enumerate(regions):
        if i != j:
            lat1, lon1 = REGION_CENTROIDS[r1]
            lat2, lon2 = REGION_CENTROIDS[r2]
            dist_matrix[i, j] = haversine_km(lat1, lon1, lat2, lon2)

dist_df = pd.DataFrame(dist_matrix, index=regions, columns=regions)
OUT_TABLES.mkdir(parents=True, exist_ok=True)
dist_df.to_csv(OUT_TABLES / "region_distances.csv")
print(f"  Saved 16×16 distance matrix → region_distances.csv")

# Distance from Santiago for each region
santiago_idx = regions.index(SANTIAGO_KEY)
dist_from_santiago = {
    r: dist_matrix[i, santiago_idx] for i, r in enumerate(regions)
}
dist_from_santiago[SANTIAGO_KEY] = 0.0

print("\n  Distance from Santiago (km):")
for r, d in sorted(dist_from_santiago.items(), key=lambda x: x[1]):
    print(f"    {r:<55} {d:>8.0f} km")


# ════════════════════════════════════════════════════════════════════════════
# STEP 2: Load pre-reform moderators (mirrors 07_binscatter_moderators.py)
# ════════════════════════════════════════════════════════════════════════════
print("\n" + "="*70)
print("STEP 2: Load pre-reform data and re-compute moderators")
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
if "source_year" in raw.columns:
    raw["source_year"] = pd.to_numeric(raw["source_year"], errors="coerce")
    raw = raw[raw["source_year"] >= 2022].copy()
else:
    raw = raw[raw["fecha_pub"].dt.year >= 2022].copy()
if "source_month" not in raw.columns:
    raw["source_month"] = raw["fecha_pub"].dt.month
print(f"  {len(raw):,} rows (2022+)")

utm = load_utm_table()
raw = add_utm_value(raw, utm)

# Filter to municipalidades, treated band (30-100 UTM), pre-reform
pre = raw[
    raw["sector"].astype(str).str.lower().str.contains(SUBSAMPLE_KW, na=False)
    & (raw["monto_utm"] >= TREAT_UTM_MIN)
    & (raw["monto_utm"] <= TREAT_UTM_MAX)
    & (raw["fecha_pub"] < PRE_CUTOFF)
].copy()
print(f"  {pre['tender_id'].nunique():,} pre-reform treated-band tenders (munic)")

pre["year_month"] = pre["fecha_pub"].dt.to_period("M")

# q_pre
q_pre_df = (
    pre.groupby(["region_buyer", "year_month"])["tender_id"].nunique()
    .groupby(level="region_buyer").mean()
    .reset_index(name="q_pre")
)

# total tenders per region (for MIN_PRE_TENDERS filter)
total_tenders = (
    pre.groupby("region_buyer")["tender_id"].nunique()
    .reset_index(name="total_tenders")
)

# nonlocal_share_pre
pre["same_region_num"] = pd.to_numeric(
    pre.get("same_region", pd.Series(np.nan, index=pre.index)), errors="coerce"
)
nonlocal_df = (
    pre.groupby("region_buyer")
    .agg(n_bids=("rut_bidder", "count"),
         n_nonlocal=("same_region_num", lambda x: (x == 0).sum()))
    .reset_index()
)
nonlocal_df["nonlocal_share_pre"] = (
    nonlocal_df["n_nonlocal"] / nonlocal_df["n_bids"].replace(0, np.nan)
).clip(0, 1)

# n_pot_local
local_bids = pre[pre["same_region_num"] == 1]
n_pot_local_df = (
    local_bids.groupby("region_buyer")["rut_bidder"].nunique()
    .reset_index(name="n_pot_local")
)

# totval_pre
totval_df = (
    pre.groupby(["region_buyer", "year_month"])["monto_utm"].mean()
    .groupby(level="region_buyer").mean()
    .reset_index(name="totval_pre")
)

# Merge all moderators
mod = (
    q_pre_df
    .merge(nonlocal_df[["region_buyer", "nonlocal_share_pre"]], on="region_buyer", how="outer")
    .merge(n_pot_local_df,  on="region_buyer", how="outer")
    .merge(totval_df,       on="region_buyer", how="outer")
    .merge(total_tenders,   on="region_buyer", how="outer")
)
mod = mod[mod["total_tenders"] >= MIN_PRE_TENDERS].copy()

# ── Merge distance from Santiago ──────────────────────────────────────────────
mod["region_canonical"] = mod["region_buyer"].apply(_normalize_region)
mod["dist_from_santiago_km"] = mod["region_canonical"].map(dist_from_santiago)

# Short labels
_REGION_PREFIXES = (
    "Región de los ", "Región de las ", "Región del ",
    "Región de la ", "Región de ", "Región ",
)
_SHORT_NAMES = {
    "Libertador General Bernardo O\u2019Higgins": "O\u2019Higgins",
    "Libertador General Bernardo O'Higgins":      "O'Higgins",
    "Ays\u00e9n del General Carlos Ib\u00e1\u00f1ez del Campo": "Ays\u00e9n",
    "Aysén del General Carlos Ibáñez del Campo":  "Aysén",
    "Magallanes y de la Antártica Chilena":       "Magallanes",
    "Magallanes y de la Ant\u00e1rtica Chilena":  "Magallanes",
    "Metropolitana de Santiago":                  "RM",
    "La Araucanía":                               "Araucanía",
    "La Araucan\u00eda":                          "Araucanía",
}
def _short(name: str) -> str:
    s = name
    for pfx in _REGION_PREFIXES:
        if name.startswith(pfx):
            s = name[len(pfx):]; break
    return _SHORT_NAMES.get(s, s)

mod["region_short"] = mod["region_canonical"].apply(_short)
mod["is_santiago"]  = (mod["region_canonical"] == SANTIAGO_KEY).astype(int)

# Exclude Santiago from analysis (distance = 0 → log(dist) = 0; outlier in all plots)
mod_nosantiago = mod[mod["is_santiago"] == 0].copy()
print(f"\n  Excluding Santiago from OLS and binscatter plots "
      f"({len(mod_nosantiago)} regions kept).")

# Save (all regions, including Santiago — used by 05_heterogeneity_region.py)
mod.to_csv(OUT_TABLES / f"moderator_with_distance_{SUBSAMPLE}.csv", index=False)
print(f"\n  Saved moderator_with_distance_{SUBSAMPLE}.csv")
print("\n  Moderators + distance:")
print(
    mod[["region_short", "dist_from_santiago_km", "q_pre",
         "nonlocal_share_pre", "n_pot_local", "totval_pre"]]
    .sort_values("dist_from_santiago_km")
    .to_string(index=False)
)

# Flag unmatched regions
unmatched = mod[mod["dist_from_santiago_km"].isna()]["region_buyer"].tolist()
if unmatched:
    print(f"\n  WARNING: {len(unmatched)} region(s) not matched to centroid:")
    for u in unmatched:
        print(f"    '{u}'")
    print("  Add them to REGION_CENTROIDS or _REGION_ALIASES above.")


# ════════════════════════════════════════════════════════════════════════════
# STEP 3: First-stage OLS — nonlocal_share ~ log(dist) + log(q_pre)
# ════════════════════════════════════════════════════════════════════════════
print("\n" + "="*70)
print("STEP 3: First-stage OLS: nonlocal_share_pre ~ log(dist) + log(q_pre)")
print("="*70)

fs = mod_nosantiago[["nonlocal_share_pre", "dist_from_santiago_km", "q_pre",
                     "n_pot_local", "totval_pre"]].dropna().copy()

def _ols(y_arr, X_arr, labels):
    """Minimal OLS with HC1 SEs."""
    n, k = X_arr.shape
    b = np.linalg.lstsq(X_arr, y_arr, rcond=None)[0]
    e = y_arr - X_arr @ b
    XtX_inv = np.linalg.inv(X_arr.T @ X_arr)
    hc1 = (n / (n - k)) * (XtX_inv @ (X_arr.T * e**2 @ X_arr) @ XtX_inv)
    se  = np.sqrt(np.diag(hc1))
    t   = b / se
    from scipy.special import ndtr
    p   = 2 * (1 - ndtr(np.abs(t)))
    r2  = 1 - np.sum(e**2) / np.sum((y_arr - y_arr.mean())**2)
    print(f"\n  R² = {r2:.3f}   n = {n}")
    for lab, bi, si, ti, pi in zip(labels, b, se, t, p):
        stars = "***" if pi < .01 else "**" if pi < .05 else "*" if pi < .10 else ""
        print(f"  {lab:<30} {bi:+.4f}  ({si:.4f})  t={ti:.2f}  {stars}")
    return b, se, r2

# Model A: nonlocal_share ~ const + log(dist+1)
log_dist = np.log(fs["dist_from_santiago_km"].clip(lower=1))
Xa = np.column_stack([np.ones(len(fs)), log_dist])
print("\n  Model A: nonlocal_share ~ const + log(dist_from_santiago+1)")
_ols(fs["nonlocal_share_pre"].values, Xa, ["Intercept", "log(dist_from_santiago)"])

# Model B: add log(q_pre)
log_q = np.log(fs["q_pre"].clip(lower=0.01))
Xb = np.column_stack([np.ones(len(fs)), log_dist, log_q])
print("\n  Model B: nonlocal_share ~ const + log(dist) + log(q_pre)")
_ols(fs["nonlocal_share_pre"].values, Xb, ["Intercept", "log(dist_from_santiago)", "log(q_pre)"])

# Model C: add log(n_pot_local)
log_npl = np.log(fs["n_pot_local"].clip(lower=0.01))
Xc = np.column_stack([np.ones(len(fs)), log_dist, log_q, log_npl])
print("\n  Model C: nonlocal_share ~ const + log(dist) + log(q_pre) + log(n_pot_local)")
_ols(fs["nonlocal_share_pre"].values, Xc,
     ["Intercept", "log(dist_from_santiago)", "log(q_pre)", "log(n_pot_local)"])


# ════════════════════════════════════════════════════════════════════════════
# STEP 4: Binscatter — distance from Santiago vs. key outcomes
# ════════════════════════════════════════════════════════════════════════════
print("\n" + "="*70)
print("STEP 4: Binscatter — dist_from_santiago vs. moderators")
print("="*70)

N_BINS  = 8   # fewer bins since only 16 obs
COL_HI  = "#d62728"   # RM / near
COL_LO  = "#1f77b4"   # far


def binscatter_dist(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    *,
    ax: plt.Axes,
    xlabel: str,
    ylabel: str,
    title: str,
    log_x: bool = True,
    log_y: bool = False,
    n_bins: int = N_BINS,
) -> None:
    sub = df[[x_col, y_col, "region_short", "is_santiago"]].dropna()
    x_raw = np.log(sub[x_col].clip(lower=1)) if log_x else sub[x_col].to_numpy(float)
    y_raw = np.log(sub[y_col].clip(lower=1)) if log_y else sub[y_col].to_numpy(float)

    slope, intercept, r, pval, _ = stats.linregress(x_raw, y_raw)

    try:
        bin_labels = pd.qcut(x_raw, q=min(n_bins, len(sub) - 1),
                              labels=False, duplicates="drop")
    except ValueError:
        bin_labels = pd.cut(x_raw, bins=min(n_bins, len(sub) - 1),
                            labels=False, include_lowest=True)

    bin_df    = pd.DataFrame({"x": x_raw, "y": y_raw, "bin": bin_labels})
    bin_means = bin_df.groupby("bin")[["x", "y"]].mean()

    # Scatter: RM highlighted
    is_hl = sub["is_santiago"].astype(bool)
    ax.scatter(x_raw[~is_hl.values], y_raw[~is_hl.values],
               color=COL_LO, alpha=0.5, s=35, zorder=3, label="Other regions")
    ax.scatter(x_raw[is_hl.values], y_raw[is_hl.values],
               color=COL_HI, alpha=0.9, s=70, marker="*", zorder=5, label="RM/Santiago")

    # Bin means
    ax.scatter(bin_means["x"], bin_means["y"],
               color="black", s=60, marker="D", zorder=6, label="Bin mean")

    # Regression line
    x_line = np.linspace(x_raw.min(), x_raw.max(), 200)
    ax.plot(x_line, intercept + slope * x_line,
            color="#ff7f0e", linewidth=1.6, zorder=4,
            label=f"OLS: r={r:.2f}, p={pval:.3f}")

    # Region labels
    for xi, yi, lab in zip(x_raw, y_raw, sub["region_short"]):
        ax.annotate(lab, (xi, yi), fontsize=6.5, ha="left", va="bottom",
                    xytext=(3, 2), textcoords="offset points", color="gray")

    ax.set_xlabel(("log " if log_x else "") + xlabel, fontsize=9)
    ax.set_ylabel(("log " if log_y else "") + ylabel, fontsize=9)
    ax.set_title(title, fontsize=9)
    ax.legend(fontsize=7)
    ax.grid(color="#e8e8e8", linewidth=0.5)
    ax.tick_params(labelsize=8)


OUT_FIGURES.mkdir(parents=True, exist_ok=True)

# ── Panel 1: dist × nonlocal_share ───────────────────────────────────────────
fig1, axes1 = plt.subplots(1, 2, figsize=(13, 5))
fig1.suptitle(
    "Distance from Santiago vs. non-local bidder share\n"
    "Municipalidades, treated band (30–100 UTM), pre-reform",
    fontsize=11, y=1.01,
)
binscatter_dist(
    mod_nosantiago, "dist_from_santiago_km", "nonlocal_share_pre",
    ax=axes1[0],
    xlabel="Distance from Santiago (km)",
    ylabel="Share non-local bidders",
    title="Log distance (linear fit)",
    log_x=True, log_y=False,
)
# Also show raw distance (to see whether relationship is monotone)
binscatter_dist(
    mod_nosantiago, "dist_from_santiago_km", "nonlocal_share_pre",
    ax=axes1[1],
    xlabel="Distance from Santiago (km)",
    ylabel="Share non-local bidders",
    title="Raw distance",
    log_x=False, log_y=False,
)
plt.tight_layout()
p1 = OUT_FIGURES / f"binscatter_dist_nonlocal_{SUBSAMPLE}.png"
fig1.savefig(p1, dpi=150, bbox_inches="tight")
plt.close(fig1)
print(f"  Saved: {p1.name}")

# ── Panel 2: 2×3 grid — dist vs. all moderators ──────────────────────────────
fig2, axes2 = plt.subplots(2, 3, figsize=(16, 9))
fig2.suptitle(
    "Distance from Santiago vs. regional moderators\n"
    "Municipalidades, treated band (30–100 UTM), pre-reform",
    fontsize=12, y=1.01,
)
DIST_PAIRS = [
    ("dist_from_santiago_km", "nonlocal_share_pre", True,  False,
     "Distance from Santiago (km)", "Share non-local bidders"),
    ("dist_from_santiago_km", "q_pre",              True,  True,
     "Distance from Santiago (km)", "Avg monthly tenders (q_pre)"),
    ("dist_from_santiago_km", "n_pot_local",        True,  True,
     "Distance from Santiago (km)", "N potential local firms"),
    ("dist_from_santiago_km", "totval_pre",         True,  False,
     "Distance from Santiago (km)", "Avg monthly value (UTM)"),
    ("q_pre",                 "nonlocal_share_pre", True,  False,
     "Avg monthly tenders (q_pre)", "Share non-local bidders"),
    ("n_pot_local",           "nonlocal_share_pre", True,  False,
     "N potential local firms",     "Share non-local bidders"),
]

for ax, (xc, yc, lx, ly, xl, yl) in zip(axes2.flat, DIST_PAIRS):
    # Exclude Santiago only from distance plots (dist=0 is degenerate on log scale)
    df_plot = mod_nosantiago if xc == "dist_from_santiago_km" else mod
    sub = df_plot[[xc, yc, "region_short", "is_santiago"]].dropna()
    if len(sub) < 4:
        ax.text(0.5, 0.5, "insuff. data", transform=ax.transAxes, ha="center")
        continue
    binscatter_dist(sub, xc, yc, ax=ax, xlabel=xl, ylabel=yl,
                    title=f"{xc} × {yc}", log_x=lx, log_y=ly)

plt.tight_layout()
p2 = OUT_FIGURES / f"binscatter_dist_moderators_{SUBSAMPLE}.png"
fig2.savefig(p2, dpi=150, bbox_inches="tight")
plt.close(fig2)
print(f"  Saved: {p2.name}")


# ════════════════════════════════════════════════════════════════════════════
# STEP 5: Prepare dist_from_santiago as a DiD moderator
# ════════════════════════════════════════════════════════════════════════════
print("\n" + "="*70)
print("STEP 5: Save standardized distance moderator for 05_heterogeneity")
print("="*70)

# Standardize and save
mu_d  = mod["dist_from_santiago_km"].mean()
sd_d  = mod["dist_from_santiago_km"].std()
mod["dist_from_santiago_std"] = (mod["dist_from_santiago_km"] - mu_d) / max(sd_d, 1e-8)
# Also save log-standardized version
log_d = np.log(mod["dist_from_santiago_km"].clip(lower=1))
mod["log_dist_from_santiago_std"] = (log_d - log_d.mean()) / max(log_d.std(), 1e-8)

dist_mod = mod[["region_buyer", "region_canonical",
                 "dist_from_santiago_km",
                 "dist_from_santiago_std",
                 "log_dist_from_santiago_std"]].copy()
dist_mod.to_csv(OUT_TABLES / "dist_from_santiago_moderator.csv", index=False)
print("  Saved: dist_from_santiago_moderator.csv")
print("  → Use this in 05_heterogeneity_region.py by adding 'dist_from_santiago_std'")
print("    or 'log_dist_from_santiago_std' to the MODERATORS list after merging on")
print("    region_buyer from the pre-reform combined data.")
print()
print("  Expected sign in interacted DiD:")
print("  Farther from Santiago → LESS non-local penetration pre-reform →")
print("  SMALLER reform effect on local entry (fewer non-locals to exclude).")
print("  Prediction: β₂(T×Post×dist_from_santiago_std) < 0 for n_local,")
print("              β₂ > 0 for nonlocal_share (the excluded firms were from nearby).")


# ════════════════════════════════════════════════════════════════════════════
# STEP 6 (optional): Commune-level distance if CSV is available
# ════════════════════════════════════════════════════════════════════════════
if COMMUNE_CENTROID_CSV is not None:
    print("\n" + "="*70)
    print("STEP 6: Commune-level distances")
    print("="*70)
    try:
        comm = pd.read_csv(COMMUNE_CENTROID_CSV)
        # Standardise column names
        comm.columns = [c.lower().strip().replace(" ", "_") for c in comm.columns]
        lat_col = next(c for c in comm.columns if "lat" in c)
        lon_col = next(c for c in comm.columns if "lon" in c or "lng" in c)
        name_col = next(c for c in comm.columns
                        if any(k in c for k in ["nombre", "name", "comuna", "commune"]))
        comm = comm[[name_col, lat_col, lon_col]].rename(
            columns={name_col: "nombre_comuna", lat_col: "lat", lon_col: "lon"}
        )
        stgo_lat, stgo_lon = REGION_CENTROIDS[SANTIAGO_KEY]
        comm["dist_from_santiago_km"] = comm.apply(
            lambda r: haversine_km(r["lat"], r["lon"], stgo_lat, stgo_lon), axis=1
        )
        out_c = OUT_TABLES / "commune_distances.csv"
        comm.to_csv(out_c, index=False)
        print(f"  {len(comm)} communes processed → {out_c.name}")
        print("  Merge on ComunaUnidad (after normalising commune name) to get")
        print("  a commune-level distance for each procuring entity.")
    except Exception as e:
        print(f"  Could not load commune centroid CSV: {e}")
else:
    print("\n" + "="*70)
    print("STEP 6: Commune-level distances — SKIPPED (COMMUNE_CENTROID_CSV = None)")
    print("="*70)
    print("""
  To enable commune-level distances:
  1. Download the INE commune centroid CSV from one of:
       https://www.ine.gob.cl/herramientas/portal-de-mapas/geodatos-abiertos
       https://github.com/robsalasco/geocl  (comunas shapefile with centroids)
       https://github.com/juanbrujo/listado-ruts-empresas-chile  (not ideal)
     The file needs columns: nombre_comuna, lat, lon (or lng).

  2. Set COMMUNE_CENTROID_CSV at the top of this script, e.g.:
       COMMUNE_CENTROID_CSV = Path("data/raw/other/comunas_centroids.csv")

  3. Re-run this script. It will:
       - Compute dist_from_santiago_km for each of Chile's ~346 communes
       - Save to output/did/tables/commune_distances.csv
       - You can then merge on ComunaUnidad in the licitaciones data

  4. In 05_heterogeneity_region.py, replace the region-level dist variable
     with the entity-level (rut_unidad → ComunaUnidad → dist) variable.
     This gives variation WITHIN regions and much more power.
""")


print("\n" + "="*70)
print("DONE — distance moderator outputs saved.")
print("="*70)
