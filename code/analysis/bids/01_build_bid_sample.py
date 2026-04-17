"""
01_build_bid_sample.py
──────────────────────────────────────────────────────────────────────────────
Build the bid-level analysis sample for the bid-markup regressions.

Primary outcome: log(bid / estimated cost) = log_sub_price_ratio

Key variables added
  bidder_region_norm  canonical region name for bidder (mapped via aliases)
  dist_km             Haversine distance (km) between bidder region centroid
                      and buyer region centroid (0 if same region)
  log_dist_km         log(1 + dist_km)  — 0 for same-region bids

  -- Commune-level distance (added 2026-03) --
  comuna_bidder       bidder's home commune (from SII; raw name normalised)
  comuna_buyer        buyer's commune (from ComunaUnidad; raw name normalised)
  dist_km_com         Haversine distance (km) between bidder & buyer commune
                      centroids; 0 for same-commune pairs
  log_dist_km_com     log(1 + dist_km_com)
  dist_bin_com        categorical distance bin at commune level:
                        "0"        same commune (dist_km_com == 0)
                        "1-50"     0 < dist_km_com <= 50
                        "50-150"   50 < dist_km_com <= 150
                        "150-300"  150 < dist_km_com <= 300
                        "300-600"  300 < dist_km_com <= 600
                        "600+"     dist_km_com > 600
  -- End commune-level additions --

  local               same_region flag (already present, renamed for clarity)
  sme                 sme_sii flag (Int8 → int8)
  large               1 - sme
  k_rel               months relative to reform (Dec 2024 = 0); used for
                      event-study regressions in Part 3
  year_month_str      string version of year_month period for pyfixest FEs

Inputs
  output/did/samples/did_bid_sample.parquet
  data/clean/combined_sii_merged_filtered.parquet  (for bidder region + commune)
  data/clean/comunas_centroids.csv                 (commune lat/lon lookup)

Output
  output/bids/bid_analysis_sample.parquet
  output/bids/bid_analysis_sample_munic.parquet
  output/bids/bid_analysis_sample_obras.parquet
"""

from __future__ import annotations

import argparse
import sys
from math import asin, cos, radians, sin, sqrt
from pathlib import Path

import numpy as np
import pandas as pd

# ── Paths ────────────────────────────────────────────────────────────────────
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1]))
from config import CODE_ROOT, DROPBOX_ROOT as ROOT, OUTPUT_ROOT  # noqa: E402
sys.path.insert(0, str(CODE_ROOT / "analysis" / "did"))

from did_utils import (          # noqa: E402
    COMBINED,
    OUT_SAMPLES,
    REFORM_PERIOD,
)

SAMPLE_SUFFIX = {"all": "", "municipalidades": "_munic", "obras": "_obras"}
SAMPLE_KEYWORD = {"municipalidades": "municipal", "obras": "obras"}

OUT_BIDS        = OUTPUT_ROOT / "bids"
OUT_BIDS_TBL    = OUT_BIDS / "tables"
OUT_BIDS_FIG    = OUT_BIDS / "figures"
for _d in [OUT_BIDS, OUT_BIDS_TBL, OUT_BIDS_FIG]:
    _d.mkdir(parents=True, exist_ok=True)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build bid-level markup analysis sample.")
    parser.add_argument(
        "--sample",
        choices=["all", "municipalidades", "obras"],
        default="all",
        help="Sample restriction on buyer sector. Uses the same keywords as the DiD scripts.",
    )
    return parser.parse_args()


ARGS = _parse_args()
SAMPLE = ARGS.sample
SAMPLE_SUFFIX_STR = SAMPLE_SUFFIX[SAMPLE]

# ── Region centroid coordinates ───────────────────────────────────────────────
# Identical to 08_distance_moderator.py; canonical region keys used in data
REGION_CENTROIDS: dict[str, tuple[float, float]] = {
    "Arica y Parinacota":                           (-18.478, -70.322),
    "Tarapacá":                                     (-20.213, -70.152),
    "Antofagasta":                                  (-23.652, -70.396),
    "Atacama":                                      (-27.366, -70.329),
    "Coquimbo":                                     (-29.909, -71.254),
    "Valparaíso":                                   (-33.047, -71.619),
    "Metropolitana de Santiago":                    (-33.459, -70.648),
    "Libertador General Bernardo O'Higgins":        (-34.170, -70.744),
    "Maule":                                        (-35.426, -71.672),
    "Ñuble":                                        (-36.607, -72.103),
    "Biobío":                                       (-36.827, -73.049),
    "La Araucanía":                                 (-38.739, -72.590),
    "Los Ríos":                                     (-39.814, -73.245),
    "Los Lagos":                                    (-41.472, -72.936),
    "Aysén del General Carlos Ibáñez del Campo":    (-45.571, -72.066),
    "Magallanes y de la Antártica Chilena":         (-53.164, -70.911),
}

_REGION_ALIASES: dict[str, str] = {
    # ── Modern "Región de …" format (region_buyer column) ──────────────────
    "Región de Arica y Parinacota":                     "Arica y Parinacota",
    "Región de Tarapacá":                               "Tarapacá",
    "Región de Antofagasta":                            "Antofagasta",
    "Región de Atacama":                                "Atacama",
    "Región de Coquimbo":                               "Coquimbo",
    "Región de Valparaíso":                             "Valparaíso",
    "Región Metropolitana de Santiago":                 "Metropolitana de Santiago",
    "Región del Libertador General Bernardo O'Higgins": "Libertador General Bernardo O'Higgins",
    "Región del Libertador General Bernardo O\u2019Higgins": "Libertador General Bernardo O'Higgins",
    # acute accent variant (´ vs ')
    "Región del Libertador General Bernardo O\u00b4Higgins": "Libertador General Bernardo O'Higgins",
    "Región del Libertador General Bernardo O´Higgins": "Libertador General Bernardo O'Higgins",
    "Región del Maule":                                 "Maule",
    "Región de Ñuble":                                  "Ñuble",
    "Región de los Lagos":                              "Los Lagos",
    "Región del Biobío":                                "Biobío",
    "Región del Biobio":                                "Biobío",
    "Región de La Araucanía":                           "La Araucanía",
    "Región de la Araucanía":                           "La Araucanía",
    "Región de Los Ríos":                               "Los Ríos",
    "Región de los Ríos":                               "Los Ríos",
    "Región de Los Lagos":                              "Los Lagos",
    "Región de Aysén del General Carlos Ibáñez del Campo":
        "Aysén del General Carlos Ibáñez del Campo",
    "Región de Magallanes y de la Antártica Chilena":
        "Magallanes y de la Antártica Chilena",
    # ── Unicode variants ───────────────────────────────────────────────────
    "Ñuble":                                            "Ñuble",
    "N\u00f1uble":                                      "Ñuble",
    "Biob\u00edo":                                      "Biobío",
    "La Araucan\u00eda":                                "La Araucanía",
    "Araucan\u00eda":                                   "La Araucanía",
    "Los R\u00edos":                                    "Los Ríos",
    "Ays\u00e9n del General Carlos Ib\u00e1\u00f1ez del Campo":
        "Aysén del General Carlos Ibáñez del Campo",
    "Aysen del General Carlos Ibanez del Campo":
        "Aysén del General Carlos Ibáñez del Campo",
    "Magallanes y de la Ant\u00e1rtica Chilena":
        "Magallanes y de la Antártica Chilena",
    "Magallanes y de la Antartica Chilena":
        "Magallanes y de la Antártica Chilena",
    # ── Roman numeral format (region column for bidders) ───────────────────
    "I REGION DE TARAPACA":                             "Tarapacá",
    "II REGION DE ANTOFAGASTA":                         "Antofagasta",
    "III REGION DE ATACAMA":                            "Atacama",
    "IV REGION COQUIMBO":                               "Coquimbo",
    "V REGION VALPARAISO":                              "Valparaíso",
    "VI REGION DEL LIBERTADOR GENERAL BERNARDO O'HIGGINS": "Libertador General Bernardo O'Higgins",
    "VII REGION DEL MAULE":                             "Maule",
    "VIII REGION DEL BIO BIO":                          "Biobío",
    "IX REGION DE LA ARAUCANIA":                        "La Araucanía",
    "X REGION LOS LAGOS":                               "Los Lagos",
    "XI REGION AYSEN DEL GENERAL CARLOS IBANEZ DEL CAMPO":
        "Aysén del General Carlos Ibáñez del Campo",
    "XII REGION DE MAGALLANES Y LA ANTARTICA CHILENA":
        "Magallanes y de la Antártica Chilena",
    "XIII REGION METROPOLITANA":                        "Metropolitana de Santiago",
    "XIV REGION DE LOS RIOS":                           "Los Ríos",
    "XV REGION ARICA Y PARINACOTA":                     "Arica y Parinacota",
    "XVI REGION DE NUBLE":                              "Ñuble",
    # encoding-mangled variants of roman numeral format
    "XVI REGION DE \u00c3UBLE":                         "Ñuble",
    "XI REGION AYSEN DEL GENERAL CARLOS IBA\u00c3EZ DEL CAMPO":
        "Aysén del General Carlos Ibáñez del Campo",
}


def _norm_region(s: str) -> str:
    if not isinstance(s, str):
        return ""
    s = s.strip()
    return _REGION_ALIASES.get(s, s)


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6_371.0
    phi1, phi2 = radians(lat1), radians(lat2)
    a = (sin(radians(lat2 - lat1) / 2) ** 2
         + cos(phi1) * cos(phi2) * sin(radians(lon2 - lon1) / 2) ** 2)
    return 2.0 * R * asin(sqrt(min(a, 1.0)))


# ── Pre-compute all 16×16 region-pair distances ───────────────────────────────
_DIST_LOOKUP: dict[tuple[str, str], float] = {
    (r1, r2): (0.0 if r1 == r2 else haversine_km(*c1, *c2))
    for r1, c1 in REGION_CENTROIDS.items()
    for r2, c2 in REGION_CENTROIDS.items()
}

_DIST_TABLE = pd.DataFrame(
    [{"bidder_region_norm": r1, "buyer_region_norm": r2, "dist_km": d}
     for (r1, r2), d in _DIST_LOOKUP.items()]
)

# ── Commune-level centroid lookup ─────────────────────────────────────────────
import unicodedata as _ud
import re as _re

def _normalise_commune_name(s: object) -> str:
    """Lower-case, strip accents, collapse whitespace, remove punctuation."""
    if not isinstance(s, str):
        return ""
    s = s.strip()
    # NFD decomposition → drop combining characters (accents)
    s = _ud.normalize("NFD", s)
    s = "".join(c for c in s if _ud.category(c) != "Mn")
    s = s.lower()
    s = _re.sub(r"[^a-z0-9 ]", " ", s)
    s = _re.sub(r"\s+", " ", s).strip()
    return s

from config import DATA_CLEAN as _DATA_CLEAN  # noqa: E402
_CENTROIDS_CSV = _DATA_CLEAN / "comunas_centroids.csv"

if _CENTROIDS_CSV.exists():
    _centroids_df = pd.read_csv(_CENTROIDS_CSV, dtype={"cut": int})
    _centroids_df["_name_norm"] = _centroids_df["nombre_comuna"].map(_normalise_commune_name)
    # Build fast lookup: normalised_name → (lat, lon)
    _COMMUNE_COORDS: dict[str, tuple[float, float]] = dict(
        zip(_centroids_df["_name_norm"],
            zip(_centroids_df["lat"], _centroids_df["lon"]))
    )
    # Also build cut → (lat, lon) for exact matching where cut codes exist
    _COMMUNE_COORDS_CUT: dict[int, tuple[float, float]] = dict(
        zip(_centroids_df["cut"],
            zip(_centroids_df["lat"], _centroids_df["lon"]))
    )
    print(f"  Loaded commune centroids: {len(_COMMUNE_COORDS):,} communes")
else:
    _COMMUNE_COORDS = {}
    _COMMUNE_COORDS_CUT = {}
    print("  [WARN] comunas_centroids.csv not found — commune distances will be NA")

def _commune_to_latlon(name: object) -> tuple[float | None, float | None]:
    """Return (lat, lon) for a commune name string, or (None, None)."""
    norm = _normalise_commune_name(name)
    coords = _COMMUNE_COORDS.get(norm)
    if coords is None:
        return None, None
    return coords

# Distance bins for commune-level analysis
_COM_BINS = [0, 50, 150, 300, 600, float("inf")]
_COM_BIN_LABELS = ["1-50", "50-150", "150-300", "300-600", "600+"]

def _dist_bin_com(d: float | None) -> str | None:
    """Assign commune-level distance bin label."""
    if d is None or (isinstance(d, float) and (d != d)):  # NaN check
        return None
    if d == 0.0:
        return "0"
    for lo, hi, lab in zip(_COM_BINS[:-1], _COM_BINS[1:], ["1-50"] + _COM_BIN_LABELS[1:]):
        # use _COM_BIN_LABELS but prepend 1-50 guard
        pass
    # use pd.cut equivalent
    if d <= 50:
        return "1-50"
    elif d <= 150:
        return "50-150"
    elif d <= 300:
        return "150-300"
    elif d <= 600:
        return "300-600"
    else:
        return "600+"


# ═════════════════════════════════════════════════════════════════════════════
print("=" * 70)
print("STEP 1: Load DiD bid sample")
print("=" * 70)

bid = pd.read_parquet(OUT_SAMPLES / "did_bid_sample.parquet")
print(f"  Rows: {len(bid):,}   Cols: {bid.shape[1]}")
print(f"  Sample option: {SAMPLE}")

if SAMPLE != "all":
    if "sector" not in bid.columns:
        raise RuntimeError(
            "'sector' column not found in did_bid_sample.parquet. "
            "Re-run 01_build_did_sample.py first."
        )
    kw = SAMPLE_KEYWORD[SAMPLE]
    bid = bid[bid["sector"].astype(str).str.lower().str.contains(kw, na=False)].copy()
    print(f"  After sector filter ({SAMPLE}): {len(bid):,} rows")


# ═════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("STEP 2: Add bidder region from combined file")
print("=" * 70)

# The combined file has 'region' (bidder's region), 'rut_bidder_raw', 'tender_id'
# The bid sample has 'rut_bidder_raw' and 'tender_id'
print("  Loading bidder region + commune from combined file …")
# Pull region (bidder's region), and also commune for both bidder and buyer
# so that commune-level Haversine distances can be computed in Step 3b.
_combined_pull = ["tender_id", "rut_bidder_raw", "region"]
for _c in ["comuna", "comuna_buyer"]:
    try:
        pd.read_parquet(COMBINED, columns=[_c])   # test availability
        _combined_pull.append(_c)
    except Exception:
        pass
_has_comuna = ("comuna" in _combined_pull)

combined_region = pd.read_parquet(
    COMBINED, columns=_combined_pull
).drop_duplicates(subset=["tender_id", "rut_bidder_raw"])
print(f"  Combined region rows: {len(combined_region):,}")
if not _has_comuna:
    print("  [WARN] 'comuna' column missing from combined file — "
          "commune-level distances will be NA")

bid = bid.merge(combined_region, on=["tender_id", "rut_bidder_raw"], how="left")
missing_region = bid["region"].isna().sum()
print(f"  Bids with missing bidder region: {missing_region:,} "
      f"({missing_region / len(bid):.1%})")


# ═════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("STEP 3: Normalize regions and compute inter-regional distances")
print("=" * 70)

bid["bidder_region_norm"] = bid["region"].map(_norm_region)
bid["buyer_region_norm"]  = bid["region_buyer"].map(_norm_region)

# Flag regions that can't be mapped to centroids (unknown spelling)
bid.loc[~bid["bidder_region_norm"].isin(REGION_CENTROIDS), "bidder_region_norm"] = np.nan
bid.loc[~bid["buyer_region_norm"].isin(REGION_CENTROIDS),  "buyer_region_norm"]  = np.nan

# Merge distance via 256-row lookup (efficient: avoids row-by-row apply)
bid = bid.merge(_DIST_TABLE, on=["bidder_region_norm", "buyer_region_norm"], how="left")

n_dist_ok = bid["dist_km"].notna().sum()
print(f"  Bids with valid distance: {n_dist_ok:,} ({n_dist_ok / len(bid):.1%})")
print(f"  Mean dist (non-local bids): "
      f"{bid.loc[bid['same_region'] == 0, 'dist_km'].mean():.0f} km")


# ═════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("STEP 3b: Commune-level distance")
print("=" * 70)

if _has_comuna and _COMMUNE_COORDS:
    # Normalise commune name strings for both bidder and buyer
    bid["_comuna_bidder_norm"] = bid["comuna"].map(_normalise_commune_name)
    bid["_comuna_buyer_norm"]  = bid["comuna_buyer"].map(_normalise_commune_name)

    # Look up lat/lon for each side
    bid["_lat_bidder"] = bid["_comuna_bidder_norm"].map(
        lambda n: _COMMUNE_COORDS.get(n, (None, None))[0])
    bid["_lon_bidder"] = bid["_comuna_bidder_norm"].map(
        lambda n: _COMMUNE_COORDS.get(n, (None, None))[1])
    bid["_lat_buyer"]  = bid["_comuna_buyer_norm"].map(
        lambda n: _COMMUNE_COORDS.get(n, (None, None))[0])
    bid["_lon_buyer"]  = bid["_comuna_buyer_norm"].map(
        lambda n: _COMMUNE_COORDS.get(n, (None, None))[1])

    # Vectorised Haversine (numpy) for commune-level distance
    import math as _math
    _R = 6_371.0

    def _vec_haversine(lat1, lon1, lat2, lon2):
        lat1 = np.radians(lat1.astype(float))
        lon1 = np.radians(lon1.astype(float))
        lat2 = np.radians(lat2.astype(float))
        lon2 = np.radians(lon2.astype(float))
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
        return 2 * _R * np.arcsin(np.sqrt(np.clip(a, 0, 1)))

    _mask = (
        bid["_lat_bidder"].notna() & bid["_lon_bidder"].notna() &
        bid["_lat_buyer"].notna()  & bid["_lon_buyer"].notna()
    )
    bid["dist_km_com"] = np.nan
    bid.loc[_mask, "dist_km_com"] = _vec_haversine(
        bid.loc[_mask, "_lat_bidder"].to_numpy(),
        bid.loc[_mask, "_lon_bidder"].to_numpy(),
        bid.loc[_mask, "_lat_buyer"].to_numpy(),
        bid.loc[_mask, "_lon_buyer"].to_numpy(),
    )

    # Expose clean commune name columns (normalised originals)
    bid["comuna_bidder"] = bid["_comuna_bidder_norm"]
    # Drop temp columns
    bid.drop(columns=["_lat_bidder", "_lon_bidder", "_lat_buyer", "_lon_buyer",
                       "_comuna_bidder_norm", "_comuna_buyer_norm"],
             inplace=True, errors="ignore")

    # Binned distance variable
    bid["dist_bin_com"] = bid["dist_km_com"].apply(_dist_bin_com)

    n_com_ok  = bid["dist_km_com"].notna().sum()
    n_com_0   = (bid["dist_km_com"] == 0).sum()
    n_nonloc  = (bid["same_region"] == 0).sum()
    print(f"  Commune distance computed: {n_com_ok:,} ({n_com_ok/len(bid):.1%})")
    print(f"  Same-commune bids (dist=0): {n_com_0:,}")
    print(f"  Non-local (region) bids: {n_nonloc:,}")
    print(f"  Mean commune dist (non-local): "
          f"{bid.loc[bid['same_region'] == 0, 'dist_km_com'].mean():.0f} km")
    print(f"  Mean commune dist (local/same-region): "
          f"{bid.loc[bid['same_region'] == 1, 'dist_km_com'].mean():.0f} km "
          f"  ← within-region variation")
    print("\n  Distance bin distribution:")
    print(bid["dist_bin_com"].value_counts().sort_index().to_string())
else:
    bid["dist_km_com"]  = np.nan
    bid["log_dist_km_com"] = np.nan
    bid["dist_bin_com"] = np.nan
    bid["comuna_bidder"] = np.nan
    print("  Skipped (commune centroid data unavailable)")


# ═════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("STEP 4: Build analysis variables")
print("=" * 70)

# Distance: log(1 + dist_km); same-region → 0
bid["log_dist_km"]     = np.log1p(bid["dist_km"].fillna(0))
# Commune-level log distance (non-null only where commune coords matched)
bid["log_dist_km_com"] = np.log1p(bid["dist_km_com"].fillna(0))

# Local / non-local (rename for clarity; keep original same_region)
bid["local"] = bid["same_region"].astype("Int8")

# SME and large (from SII classification)
bid["sme"]   = bid["sme_sii"].astype("Int8")
bid["large"]  = (bid["sme_sii"] == 0).astype("Int8")
# Firms not in SII: sme=NA → keep as NA

# k_rel: months relative to reform (Dec 2024 = 0)
# year_month is Period[M]; compute as integer offset from REFORM_PERIOD
reform_yr, reform_mo = 2024, 12
bid["k_rel"] = (
    (bid["year_month"].dt.year  - reform_yr) * 12 +
    (bid["year_month"].dt.month - reform_mo)
).astype("Int16")

# String versions of FE variables for pyfixest
bid["year_month_str"] = bid["year_month"].dt.strftime("%Y-%m")
bid["bidder_id_str"]  = bid["bidder_id"].astype(str)
bid["tender_id_str"]  = bid["tender_id"].astype(str)
bid["entity_str"]     = bid["rut_unidad"].astype(str)

# Rename outcome for clarity (already computed by 01_build_did_sample.py)
bid["log_bid_ratio"] = bid["log_sub_price_ratio"]

print(f"  log_bid_ratio non-null: {bid['log_bid_ratio'].notna().sum():,}")
print(f"  local==1: {(bid['local'] == 1).sum():,}  "
      f"local==0: {(bid['local'] == 0).sum():,}  "
      f"local==NA: {bid['local'].isna().sum():,}")
print(f"  sme==1:  {(bid['sme'] == 1).sum():,}  "
      f"sme==0: {(bid['sme'] == 0).sum():,}  "
      f"sme==NA: {bid['sme'].isna().sum():,}")
print(f"  k_rel range: {bid['k_rel'].min()} to {bid['k_rel'].max()}")


# ═════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("STEP 5: Sample restrictions and summary statistics")
print("=" * 70)

n_raw = len(bid)

# Drop rows where outcome is missing or infinite
bid = bid[bid["log_bid_ratio"].notna() & np.isfinite(bid["log_bid_ratio"])]
print(f"  After dropping null/inf log_bid_ratio: {len(bid):,}  "
      f"(dropped {n_raw - len(bid):,})")

# Must have valid auction FE identifier
bid = bid[bid["tender_id_str"].notna() & (bid["tender_id_str"] != "None")]
print(f"  After tender_id filter: {len(bid):,}")

# Value band is assigned (band in [control_low, treated, control_high])
bid = bid[bid["band"].notna()]
print(f"  After band filter: {len(bid):,}")

print(f"\n  Final sample: {len(bid):,} bids")
print(f"  Unique auctions (tender_id):   {bid['tender_id_str'].nunique():,}")
print(f"  Unique bidders (bidder_id):    {bid['bidder_id_str'].nunique():,}")
print(f"  Unique entities (rut_unidad):  {bid['entity_str'].nunique():,}")
print(f"  Date range: {bid['fecha_pub'].min().date()} — {bid['fecha_pub'].max().date()}")

# Summary of log_bid_ratio by group
print("\n  Outcome summary (log_bid_ratio = log(bid / estimated cost)):")
for grp, lab in [(None, "All"), (bid["local"] == 1, "Local"), (bid["local"] == 0, "Non-local"),
                  (bid["sme"] == 1, "SME"), (bid["sme"] == 0, "Large")]:
    sub = bid if grp is None else bid[grp]
    sub_valid = sub["log_bid_ratio"].dropna()
    print(f"    {lab:12s}: N={len(sub_valid):>10,}  mean={sub_valid.mean():.3f}  "
          f"sd={sub_valid.std():.3f}  p50={sub_valid.median():.3f}")


# ═════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("STEP 6: Save")
print("=" * 70)

out_path = OUT_BIDS / f"bid_analysis_sample{SAMPLE_SUFFIX_STR}.parquet"
bid.to_parquet(out_path, index=False)
print(f"  Saved: {out_path.relative_to(ROOT)}")
print(f"  Shape: {bid.shape}")

print("\nDone.")
