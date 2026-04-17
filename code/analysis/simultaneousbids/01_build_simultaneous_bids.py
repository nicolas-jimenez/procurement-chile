"""
01_build_simultaneous_bids.py
─────────────────────────────────────────────────────────────────────────────
Build the core dataset for the simultaneous-bidding analysis.
Uses DuckDB for aggregation, then pandas for enrichment.

Inputs
  data/clean/licitaciones_sii_merged.parquet

Outputs
  output/simultaneousbids/firm_month_panel.parquet
  output/simultaneousbids/bid_level_simult.parquet
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config import DATA_CLEAN, OUTPUT_ROOT  # noqa: E402

OUT = OUTPUT_ROOT / "simultaneousbids"
OUT.mkdir(parents=True, exist_ok=True)

PARQUET = str(DATA_CLEAN / "licitaciones_sii_merged.parquet")
REFORM_MONTH = "2024-12"

# ── Region centroid lookup ────────────────────────────────────────────────────
REGION_CENTROIDS = {
    "Arica y Parinacota": (-18.478, -70.322),
    "Tarapacá":           (-20.213, -70.152),
    "Antofagasta":        (-23.652, -70.396),
    "Atacama":            (-27.366, -70.329),
    "Coquimbo":           (-29.909, -71.254),
    "Valparaíso":         (-33.036, -71.625),
    "Metropolitana":      (-33.457, -70.648),
    "OHiggins":           (-34.583, -71.003),   # apostrophe stripped for SQL safety
    "Maule":              (-35.426, -71.655),
    "Ñuble":              (-36.727, -72.111),
    "Biobío":             (-37.469, -72.354),
    "Araucanía":          (-38.948, -72.662),
    "Los Ríos":           (-40.231, -72.334),
    "Los Lagos":          (-41.469, -72.937),
    "Aysén":              (-45.572, -72.068),
    "Magallanes":         (-53.163, -70.907),
}

# Python-side normalisation — covers both ChileCompra buyer region names (with trailing
# spaces, accents, ´ apostrophe variants) and SII Roman-numeral bidder region names.
REGION_NORM: dict[str, str] = {}

# ── Buyer region variants (region_buyer column) ───────────────────────────────
_BUYER_PAIRS = [
    ("Arica y Parinacota",                                     "Arica y Parinacota"),
    ("Región de Tarapacá",                                     "Tarapacá"),
    ("Región de Antofagasta",                                  "Antofagasta"),
    ("Región de Atacama",                                      "Atacama"),
    ("Región de Coquimbo",                                     "Coquimbo"),
    ("Región de Valparaíso",                                   "Valparaíso"),
    ("Región Metropolitana de Santiago",                       "Metropolitana"),
    ("Región Metropolitana",                                   "Metropolitana"),
    ("Metropolitana",                                          "Metropolitana"),
    ("Los Ríos",                                               "Los Ríos"),
    ("Región del Libertador General Bernardo O'Higgins",       "OHiggins"),
    ("Región del Libertador General Bernardo O´Higgins",       "OHiggins"),
    ("Región del Maule",                                       "Maule"),
    ("Región del Ñuble",                                       "Ñuble"),
    ("Región del Biobío",                                      "Biobío"),
    ("Región de La Araucanía",                                 "Araucanía"),
    ("Región de la Araucanía",                                 "Araucanía"),
    ("Región de Los Ríos",                                     "Los Ríos"),
    ("Región de los Lagos",                                    "Los Lagos"),
    ("Región de Los Lagos",                                    "Los Lagos"),
    ("Región Aysén del General Carlos Ibáñez del Campo",       "Aysén"),
    ("Región de Aysén del General Carlos Ibáñez del Campo",    "Aysén"),
    ("Región de Magallanes y de la Antártica Chilena",         "Magallanes"),
    ("Región de Magallanes y de la Antártica",                 "Magallanes"),
    # short canonical forms
    ("Tarapacá", "Tarapacá"), ("Antofagasta", "Antofagasta"),
    ("Atacama", "Atacama"), ("Coquimbo", "Coquimbo"),
    ("Valparaíso", "Valparaíso"), ("OHiggins", "OHiggins"),
    ("Maule", "Maule"), ("Ñuble", "Ñuble"), ("Biobío", "Biobío"),
    ("Araucanía", "Araucanía"), ("Los Ríos", "Los Ríos"),
    ("Los Lagos", "Los Lagos"), ("Aysén", "Aysén"), ("Magallanes", "Magallanes"),
]
for raw, canon in _BUYER_PAIRS:
    REGION_NORM[raw] = canon
    REGION_NORM[raw.strip()] = canon   # also strip trailing spaces

# ── SII Roman numeral bidder region variants ──────────────────────────────────
_SII_PAIRS = [
    ("I REGION DE TARAPACA",                                   "Tarapacá"),
    ("II REGION DE ANTOFAGASTA",                               "Antofagasta"),
    ("III REGION DE ATACAMA",                                  "Atacama"),
    ("IV REGION COQUIMBO",                                     "Coquimbo"),
    ("V REGION VALPARAISO",                                    "Valparaíso"),
    ("VI REGION DEL LIBERTADOR GENERAL BERNARDO O'HIGGINS",    "OHiggins"),
    ("VII REGION DEL MAULE",                                   "Maule"),
    ("VIII REGION DEL BIO BIO",                                "Biobío"),
    ("IX REGION DE LA ARAUCANIA",                              "Araucanía"),
    ("X REGION LOS LAGOS",                                     "Los Lagos"),
    ("XI REGION AYSEN DEL GENERAL CARLOS IBA\x91EZ DEL CAMPO","Aysén"),
    ("XI REGION AYSEN DEL GENERAL CARLOS IBAÃEZ DEL CAMPO",   "Aysén"),
    ("XII REGION DE MAGALLANES Y LA ANTARTICA CHILENA",        "Magallanes"),
    ("XIII REGION METROPOLITANA",                              "Metropolitana"),
    ("XIV REGION DE LOS RIOS",                                 "Los Ríos"),
    ("XV REGION ARICA Y PARINACOTA",                           "Arica y Parinacota"),
    ("XVI REGION DE \x91UBLE",                                 "Ñuble"),
    ("XVI REGION DE Ã\x91UBLE",                                "Ñuble"),
]
for raw, canon in _SII_PAIRS:
    REGION_NORM[raw] = canon

def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    phi1, phi2 = np.radians(lat1), np.radians(lat2)
    dphi = np.radians(lat2 - lat1)
    dlam = np.radians(lon2 - lon1)
    a = np.sin(dphi / 2)**2 + np.cos(phi1) * np.cos(phi2) * np.sin(dlam / 2)**2
    return 2 * R * np.arcsin(np.sqrt(np.clip(a, 0, 1)))


# ── Step 1: DuckDB aggregation to tender–firm level ───────────────────────────
print("Step 1: DuckDB dedup to tender–firm level…")
con = duckdb.connect(database=":memory:")
con.execute("PRAGMA threads=2")
con.execute("PRAGMA memory_limit='3.5GB'")

# Select only needed columns first to minimise memory
dedup_sql = f"""
SELECT
    CAST(tender_id AS VARCHAR) AS tender_id,
    CAST(rut_bidder AS BIGINT) AS rut_bidder,
    fecha_pub,
    strftime(fecha_pub, '%Y-%m') AS ym,
    region_buyer,
    region AS region_bidder,
    SUM(monto_oferta)    AS monto_oferta,
    SUM(monto_estimado)  AS monto_estimado,
    MAX(CAST(is_selected AS INT))    AS is_selected,
    FIRST(sector)       AS sector,
    FIRST(tipo)         AS tipo,
    FIRST(n_oferentes)  AS n_oferentes,
    FIRST(tramoventas)  AS tramoventas,
    FIRST(ntrabajadores) AS ntrabajadores,
    FIRST(same_region)  AS same_region,
    CASE WHEN strftime(fecha_pub, '%Y-%m') > '{REFORM_MONTH}' THEN 1 ELSE 0 END AS post,
    MAX(CASE WHEN sector = 'Municipalidades' THEN 1 ELSE 0 END) AS is_munic,
    MAX(CASE WHEN sector = 'Obras Públicas'  THEN 1 ELSE 0 END) AS is_obras
FROM read_parquet('{PARQUET}')
WHERE rut_bidder IS NOT NULL
  AND fecha_pub IS NOT NULL
  AND (is_key_dup IS NULL OR is_key_dup = false)
GROUP BY tender_id, rut_bidder, fecha_pub, ym,
         region_buyer, region_bidder, post
"""
t_df = con.execute(dedup_sql).df()
con.close()
print(f"  Tender–firm rows: {len(t_df):,}  |  tenders: {t_df['tender_id'].nunique():,}  |  firms: {t_df['rut_bidder'].nunique():,}")

# ── Step 1b: Add size_group in Python ────────────────────────────────────────
tv = t_df["tramoventas"]
t_df["size_group"] = pd.cut(tv, bins=[0,4,7,10,13],
                             labels=["micro","small","medium","large"],
                             right=True).astype(str)
t_df.loc[t_df["size_group"] == "nan", "size_group"] = np.nan

# ── Step 2: Python-side region normalisation ──────────────────────────────────
print("Step 2: Normalise region names…")
# Strip whitespace before mapping so trailing-space variants resolve correctly
def norm_region(s):
    if pd.isna(s):
        return None
    s2 = str(s).strip()
    return REGION_NORM.get(s2, REGION_NORM.get(s, s2))

t_df["region_buyer_n"]  = t_df["region_buyer"].apply(norm_region)
t_df["region_bidder_n"] = t_df["region_bidder"].apply(norm_region)

# ── Step 3: Geographic distance ───────────────────────────────────────────────
print("Step 3: Computing geographic distances…")
lat_buyer  = t_df["region_buyer_n"].map(lambda r: REGION_CENTROIDS.get(r, (np.nan, np.nan))[0])
lon_buyer  = t_df["region_buyer_n"].map(lambda r: REGION_CENTROIDS.get(r, (np.nan, np.nan))[1])
lat_bidder = t_df["region_bidder_n"].map(lambda r: REGION_CENTROIDS.get(r, (np.nan, np.nan))[0])
lon_bidder = t_df["region_bidder_n"].map(lambda r: REGION_CENTROIDS.get(r, (np.nan, np.nan))[1])
t_df["dist_km"] = haversine_km(
    lat_bidder.values, lon_bidder.values,
    lat_buyer.values,  lon_buyer.values
)

# ── Step 4: Firm–month panel ──────────────────────────────────────────────────
print("Step 4: Building firm–month panel…")
fm_df = (
    t_df.groupby(["rut_bidder", "ym"], sort=False)
    .agg(
        n_sim          = ("tender_id", "nunique"),
        n_sim_local    = ("same_region", lambda x: (x == 1.0).sum()),
        n_sim_nonlocal = ("same_region", lambda x: (x == 0.0).sum()),
        n_regions_bid  = ("region_buyer_n", "nunique"),
        n_won          = ("is_selected", "sum"),
        tramoventas    = ("tramoventas", "first"),
        ntrabajadores  = ("ntrabajadores", "first"),
        size_group     = ("size_group", "first"),
        region_bidder_n= ("region_bidder_n", "first"),
        post           = ("post", "first"),
        n_sim_munic    = ("is_munic", "sum"),
        n_sim_obras    = ("is_obras", "sum"),
        avg_dist_km    = ("dist_km", "mean"),
        max_dist_km    = ("dist_km", "max"),
    )
    .reset_index()
)
fm_df["share_nonlocal"] = (fm_df["n_sim_nonlocal"] / fm_df["n_sim"]).fillna(0)
print(f"  Firm–month rows: {len(fm_df):,}")

# ── Step 5: Lagged variables ──────────────────────────────────────────────────
print("Step 5: Lagged variables…")
fm_df = fm_df.sort_values(["rut_bidder", "ym"])
fm_df["n_sim_lag1"]    = fm_df.groupby("rut_bidder")["n_sim"].shift(1)
fm_df["n_sim_lag2"]    = fm_df.groupby("rut_bidder")["n_sim"].shift(2)
fm_df["n_sim_nl_lag1"] = fm_df.groupby("rut_bidder")["n_sim_nonlocal"].shift(1)

# ── Step 6: Merge lags back to bid level ─────────────────────────────────────
print("Step 6: Merging lags to bid level…")
t_df = t_df.merge(
    fm_df[["rut_bidder", "ym", "n_sim", "n_sim_local", "n_sim_nonlocal",
           "n_sim_lag1", "n_sim_nl_lag1", "n_sim_munic", "n_sim_obras",
           "avg_dist_km"]],
    on=["rut_bidder", "ym"], how="left"
)

# ── Step 7: Bid ratio ─────────────────────────────────────────────────────────
t_df["bid_ratio"] = np.where(
    t_df["monto_estimado"].gt(0), t_df["monto_oferta"] / t_df["monto_estimado"], np.nan
)
t_df["log_bid_ratio"] = np.log(t_df["bid_ratio"].replace(0, np.nan))

# ── Step 8: First-bid-in-region entry indicator ───────────────────────────────
print("Step 8: Entry indicators…")
t_df["fecha_pub"] = pd.to_datetime(t_df["fecha_pub"], errors="coerce")
t_df = t_df.sort_values(["rut_bidder", "region_buyer_n", "fecha_pub"])
t_df["first_bid_in_region"] = (
    ~t_df.duplicated(subset=["rut_bidder", "region_buyer_n"], keep="first")
).astype(int)

t_df = t_df.sort_values(["rut_bidder", "sector", "region_buyer_n", "fecha_pub"])
t_df["first_bid_sector_region"] = (
    ~t_df.duplicated(subset=["rut_bidder", "sector", "region_buyer_n"], keep="first")
).astype(int)

# ── Save ──────────────────────────────────────────────────────────────────────
print("Saving outputs…")
fm_df.to_parquet(OUT / "firm_month_panel.parquet", index=False)
t_df.to_parquet(OUT / "bid_level_simult.parquet", index=False)
print(f"  firm_month_panel: {fm_df.shape}  →  {OUT}/firm_month_panel.parquet")
print(f"  bid_level_simult: {t_df.shape}  →  {OUT}/bid_level_simult.parquet")
print("\nDone.")
