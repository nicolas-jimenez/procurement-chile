"""
05_heterogeneity_region.py
─────────────────────────────────────────────────────────────────────────────
Heterogeneity by region for the Compra Ágil DiD.

Steps
  1. Build four region-level pre-reform moderators and standardize to Z-scores:
       nonlocal_share_pre, q_pre, totval_pre, n_pot_local
  2. For each moderator × outcome, run interacted OLS-DiD and IV-DiD.
     Save summary table per moderator ({csv,tex}).
  3. Run a fully-interacted IV-DiD (one δ per eligible region) and produce a
     horizontal coefficient plot per outcome.
  4. Sanity checks printed to console before and after regressions.

Configuration
  Change SUBSAMPLE / CONTROL at the top to rerun for another group.

Outputs
  output/did/tables/hetero_interacted_{moderator}_{SUBSAMPLE}.{csv,tex}
  output/did/figures/hetero_coefplot_{outcome}_{SUBSAMPLE}.png
"""

from __future__ import annotations

import sys
from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from scipy.special import ndtr

matplotlib.use("Agg")

# ── Path setup ────────────────────────────────────────────────────────────────
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from did_utils import (
    COMBINED,
    OUT_SAMPLES,
    OUT_TABLES,
    OUT_FIGURES,
    _twoway_demean,
    _cluster_se,
)

# ════════════════════════════════════════════════════════════════════════════
# ── Top-level configuration — change SUBSAMPLE to rerun for another sector ───
# ════════════════════════════════════════════════════════════════════════════
SUBSAMPLE = "munic"   # options: "munic", "obras", "all"
CONTROL   = "high"    # use Control 2 (100–200 UTM) throughout

# ── Fixed column names (matched to existing DiD pipeline) ────────────────────
ENTITY_COL  = "rut_unidad"
TIME_COL    = "year_month"
DID_COL     = "did"
TREAT_COL   = "treated"
CLUSTER_COL = "rut_unidad"
REGION_COL  = "region_buyer"     # tender's region (RegionUnidad)

MIN_PRE_TENDERS = 30             # exclude regions with fewer pre-reform tenders
_PRE_CUTOFF     = pd.Timestamp("2024-12-01")   # strictly before December 2024

# ── Outcome catalogue (skip silently if absent from sample) ──────────────────
# Bid-level outcomes use df_bid; all others use df_tender.
OUTCOMES = [
    ("n_bidders",                 "N bidders"),
    ("n_local",                   "N local bidders"),
    ("n_nonlocal",                "N non-local bidders"),
    ("n_sme_bidders",             "N SME bidders"),
    ("n_large_bidders",           "N large bidders"),
    ("single_bidder",             "Pr(single-bidder)"),
    ("sme_share_sii",             "% bidders: SME"),
    ("share_large_bidders",       "% bidders: large"),
    ("share_sme_local_bidders",   "% bidders: SME×local"),
    ("share_large_local_bidders", "% bidders: large×local"),
    ("winner_is_sme_sii",         "Pr(winner: SME)"),
    ("winner_is_large",           "Pr(winner: large)"),
    ("winner_is_sme_local",       r"Pr(winner: SME \& local)"),
    ("winner_is_large_local",     r"Pr(winner: large \& local)"),
    ("log_win_price_ratio",       "log win bid / ref price"),
    ("log_sub_price_ratio",       "log bid / ref price"),
]

_BID_OUTCOMES = {"log_sub_price_ratio"}

MODERATORS = ["nonlocal_share_pre", "q_pre", "totval_pre", "n_pot_local",
              "dist_from_santiago"]   # added by 08_distance_moderator.py

# ── Subsample keyword map ─────────────────────────────────────────────────────
_SUBSAMPLE_KW = {"munic": "municipal", "obras": "obras", "all": None}

# ── Region label prefixes to strip for plot readability ──────────────────────
_REGION_PREFIXES = (
    "Región de los ",
    "Región de las ",
    "Región del ",
    "Región de la ",
    "Región de ",
    "Región ",
)

# Override long names that remain verbose even after prefix stripping
_SHORT_REGION_NAMES: dict[str, str] = {
    "Libertador General Bernardo O\u2019Higgins": "O\u2019Higgins",
    "Libertador General Bernardo O'Higgins":      "O'Higgins",
    "Ays\u00e9n del General Carlos Ib\u00e1\u00f1ez del Campo": "Ays\u00e9n",
    "Aysen del General Carlos Ibanez del Campo":  "Ays\u00e9n",
}


# ── Small helpers ─────────────────────────────────────────────────────────────
def _stars(p: float) -> str:
    if not np.isfinite(p):
        return ""
    return "***" if p < 0.01 else ("**" if p < 0.05 else ("*" if p < 0.1 else ""))


def _pval(coef: float, se: float) -> float:
    if se <= 0 or not np.isfinite(se):
        return np.nan
    return float(2 * (1 - ndtr(abs(coef / se))))


def _apply_subsample(df: pd.DataFrame) -> pd.DataFrame:
    """Filter rows to the configured SUBSAMPLE by sector keyword."""
    kw = _SUBSAMPLE_KW[SUBSAMPLE]
    if kw is None or "sector" not in df.columns:
        return df.copy()
    return df[
        df["sector"].astype(str).str.lower().str.contains(kw, na=False)
    ].copy()


def _strip_region_prefix(name: str) -> str:
    """Remove standard Spanish region prefixes and apply short-name overrides."""
    short = name
    for pfx in _REGION_PREFIXES:
        if name.startswith(pfx):
            short = name[len(pfx):]
            break
    return _SHORT_REGION_NAMES.get(short, short)


# ── Data loaders ──────────────────────────────────────────────────────────────
def _load_did_parquet(name: str) -> pd.DataFrame:
    path = OUT_SAMPLES / name
    print(f"  Loading {path.name} …")
    df = pd.read_parquet(path)
    df["year_month"] = df["year_month"].apply(
        lambda x: pd.Period(x, freq="M") if not isinstance(x, pd.Period) else x
    )
    df["fecha_pub"] = pd.to_datetime(df["fecha_pub"], errors="coerce")
    for col in df.select_dtypes(include=["Int64", "Int8"]).columns:
        df[col] = df[col].astype("float64")
    return df


def load_tender() -> pd.DataFrame:
    return _load_did_parquet("did_tender_sample.parquet")


def load_bid() -> pd.DataFrame:
    return _load_did_parquet("did_bid_sample.parquet")


def load_combined_raw() -> pd.DataFrame:
    """
    Load the full COMBINED bid-level parquet from 2022 onwards.
    Used for nonlocal_share_pre and n_pot_local which require pre-2023 data.
    Mirrors the loading logic in 01_build_did_sample.py::load_combined().
    """
    print("  Loading combined_sii_merged_filtered (raw, 2022+) …")
    avail = pq.read_schema(COMBINED).names
    want  = [
        "tender_id", "rut_bidder", ENTITY_COL, REGION_COL,
        "fecha_pub", "source_year", "source_month",
        "monto_estimado", "same_region", "sector", "is_key_dup",
    ]
    cols = [c for c in want if c in avail]
    df   = pd.read_parquet(COMBINED, columns=cols)
    df   = df[~df["is_key_dup"].fillna(False)].copy()
    df["fecha_pub"] = pd.to_datetime(df["fecha_pub"], errors="coerce")
    df   = df[df["fecha_pub"].notna()].copy()
    # From 2022 onwards
    if "source_year" in df.columns:
        df["source_year"] = pd.to_numeric(df["source_year"], errors="coerce")
        df = df[df["source_year"] >= 2022].copy()
    else:
        df = df[df["fecha_pub"].dt.year >= 2022].copy()
    print(f"    {len(df):,} rows (2022+)")
    return df


# ── Tender augmentation (mirrors 02_run_did._augment_tender_from_bid) ────────
def _augment_tender(df_tender: pd.DataFrame, df_bid: pd.DataFrame) -> pd.DataFrame:
    """
    Compute bid-derived tender-level columns not stored in the parquet and
    merge them into df_tender.  Adds: n_sme_bidders, n_large_bidders,
    share_large_bidders, share_large_local_bidders, winner_is_large,
    winner_is_sme_local, winner_is_large_local.
    """
    print("  Augmenting tender sample from bid sample …")

    sme = pd.to_numeric(df_bid["sme_sii"], errors="coerce")
    loc = pd.to_numeric(
        df_bid.get("same_region", pd.Series(np.nan, index=df_bid.index)),
        errors="coerce",
    )
    tid = df_bid["tender_id"]

    sme_flag  = (sme == 1).astype("float64")
    large_flag = pd.Series(
        np.where(sme.notna(), (sme == 0).astype("float64"), np.nan),
        index=df_bid.index,
    )
    large_local = pd.Series(
        np.where((sme == 0) & (loc == 1), 1.0,
                 np.where(sme.notna() & loc.notna(), 0.0, np.nan)),
        index=df_bid.index,
    )
    sme_local = pd.Series(
        np.where((sme == 1) & (loc == 1), 1.0,
                 np.where(sme.notna() & loc.notna(), 0.0, np.nan)),
        index=df_bid.index,
    )

    n_tot = df_bid.groupby(tid, sort=False)["bidder_id"].nunique()

    agg = pd.concat([
        sme_flag.groupby(tid, sort=False).sum().rename("n_sme_bidders"),
        large_flag.groupby(tid, sort=False).sum().rename("n_large_bidders"),
        (large_flag.groupby(tid, sort=False).sum() / n_tot).rename("share_large_bidders"),
        large_local.groupby(tid, sort=False).mean().rename("share_large_local_bidders"),
    ], axis=1)

    # Winner flags
    sel = (df_bid["is_selected"].astype("float64")
           if "is_selected" in df_bid.columns
           else pd.Series(0.0, index=df_bid.index))

    def _winner_flag(flag_s: pd.Series, name: str) -> pd.Series:
        w   = flag_s.where(sel == 1)
        n_w = w.groupby(tid, sort=False).count()
        return pd.Series(
            np.where(n_w > 0, w.groupby(tid, sort=False).max(), np.nan),
            index=n_w.index, name=name,
        )

    agg = pd.concat([
        agg,
        _winner_flag(large_flag,  "winner_is_large"),
        _winner_flag(sme_local,   "winner_is_sme_local"),
        _winner_flag(large_local, "winner_is_large_local"),
    ], axis=1).reset_index()

    new_cols = [
        c for c in agg.columns if c != "tender_id" and c not in df_tender.columns
    ]
    if not new_cols:
        print("    All augmented columns already present.")
        return df_tender
    df_tender = df_tender.merge(agg[["tender_id"] + new_cols], on="tender_id", how="left")
    print(f"    Added {len(new_cols)} columns: {new_cols}")
    return df_tender


# ════════════════════════════════════════════════════════════════════════════
# ── Step 1: Region-level pre-reform moderators ───────────────────────────────
# ════════════════════════════════════════════════════════════════════════════
def build_region_moderators(
    df_tender: pd.DataFrame,
    df_bid:    pd.DataFrame,
) -> pd.DataFrame:
    """
    Construct four region-level moderators using pre-reform data, standardize
    them to mean 0 / SD 1, and return a DataFrame indexed by REGION_COL.

    Columns returned
      nonlocal_share_pre, q_pre, totval_pre, n_pot_local   (standardized)
      *_raw versions of each                                (unstandardized)
      n_pre_tenders                                         (for threshold check)
    """
    # ── Load full COMBINED for nonlocal_share_pre and n_pot_local ─────────────
    raw      = load_combined_raw()
    raw_sub  = _apply_subsample(raw)
    raw_pre  = raw_sub[raw_sub["fecha_pub"] < _PRE_CUTOFF].copy()

    if REGION_COL not in raw_pre.columns:
        raise ValueError(
            f"Column '{REGION_COL}' not found in COMBINED. "
            "Cannot construct region-level moderators."
        )

    # 1. nonlocal_share_pre
    #    Pool all bidder-tender rows in pre-period; fraction where same_region == 0.
    #    Weighted by bidder row (not averaged per tender).
    sr_num = pd.to_numeric(raw_pre["same_region"], errors="coerce")
    valid  = raw_pre[sr_num.notna()].copy()
    valid["_nonlocal"] = (pd.to_numeric(valid["same_region"], errors="coerce") == 0).astype("float64")
    nonlocal_share_pre = (
        valid.groupby(REGION_COL)["_nonlocal"].mean()
        .rename("nonlocal_share_pre")
    )

    # 4. n_pot_local
    #    Unique local bidder RUTs (same_region == 1) from 2022+ pre-reform data.
    if "rut_bidder" in raw_pre.columns:
        local_rows  = raw_pre[pd.to_numeric(raw_pre["same_region"], errors="coerce") == 1]
        n_pot_local = (
            local_rows.groupby(REGION_COL)["rut_bidder"]
            .nunique()
            .rename("n_pot_local")
        )
    else:
        print("  [WARN] rut_bidder absent — n_pot_local set to NaN for all regions.")
        n_pot_local = pd.Series(dtype="float64", name="n_pot_local")

    # ── DiD tender sample for q_pre and totval_pre ────────────────────────────
    if REGION_COL not in df_tender.columns:
        raise ValueError(
            f"Column '{REGION_COL}' not found in tender sample. "
            "Cannot construct region-level moderators."
        )

    tender_pre = df_tender[df_tender["fecha_pub"] < _PRE_CUTOFF].copy()
    tender_pre["_ym_str"] = tender_pre[TIME_COL].astype(str)

    # 2. q_pre — average monthly tender count per region
    rmon_q = (
        tender_pre.groupby([REGION_COL, "_ym_str"])["tender_id"]
        .nunique()
        .reset_index(name="_n_tenders")
    )
    q_pre = rmon_q.groupby(REGION_COL)["_n_tenders"].mean().rename("q_pre")

    # 3. totval_pre — average monthly total estimated value per region
    #    Use monto_utm (UTM) when available, fall back to monto_estimado (CLP).
    val_col = "monto_utm" if "monto_utm" in tender_pre.columns else "monto_estimado"
    rmon_v  = (
        tender_pre.groupby([REGION_COL, "_ym_str"])[val_col]
        .sum()
        .reset_index(name="_totval")
    )
    totval_pre = rmon_v.groupby(REGION_COL)["_totval"].mean().rename("totval_pre")

    # ── Pre-reform tender count per region (for sanity check + threshold) ─────
    n_pre = (
        tender_pre.groupby(REGION_COL)["tender_id"].nunique()
        .rename("n_pre_tenders")
    )

    # ── Assemble ──────────────────────────────────────────────────────────────
    mods = (
        nonlocal_share_pre.to_frame()
        .join(q_pre,         how="outer")
        .join(totval_pre,    how="outer")
        .join(n_pot_local,   how="outer")
        .join(n_pre,         how="left")
    )
    mods.index.name = REGION_COL

    # ── Distance from Santiago (loaded from 08_distance_moderator.py output) ─
    _dist_path = OUT_TABLES / "dist_from_santiago_moderator.csv"
    if _dist_path.exists():
        _dist = pd.read_csv(_dist_path)[["region_buyer", "log_dist_from_santiago_std"]]
        _dist = _dist.rename(columns={"log_dist_from_santiago_std": "dist_from_santiago"})
        _dist = _dist.set_index("region_buyer")
        mods  = mods.join(_dist, how="left")
        print(f"  [dist] Merged log_dist_from_santiago_std for "
              f"{mods['dist_from_santiago'].notna().sum()} regions")
    else:
        print(f"  [dist] WARNING: {_dist_path.name} not found — "
              "run 08_distance_moderator.py first. "
              "Setting dist_from_santiago = NaN.")
        mods["dist_from_santiago"] = np.nan

    # Store raw values before standardizing (dist is already standardized)
    for col in MODERATORS:
        mods[f"{col}_raw"] = mods[col].copy()

    # Standardize to mean 0, SD 1 across regions
    # (dist_from_santiago is already standardized; skip re-standardization)
    _skip_std = {"dist_from_santiago"}
    for col in MODERATORS:
        if col in _skip_std:
            continue
        s       = mods[col]
        mu, sd  = s.mean(), s.std(ddof=1)
        mods[col] = (s - mu) / sd if sd > 0 else s - mu

    return mods.reset_index()


# ════════════════════════════════════════════════════════════════════════════
# ── Step 4a: Sanity checks (printed before regressions) ─────────────────────
# ════════════════════════════════════════════════════════════════════════════
def print_sanity_checks(mods: pd.DataFrame) -> None:
    regions = mods[REGION_COL].dropna().tolist()
    print("\n" + "=" * 75)
    print(f"SANITY CHECKS — Region moderators  [SUBSAMPLE={SUBSAMPLE!r}]")
    print("=" * 75)
    print(f"\n  N regions in subsample: {len(regions)}")
    print(f"  Regions: {regions}\n")

    hdr = "{:<34} {:>13} {:>8} {:>14} {:>12} {:>10}"
    print(hdr.format(
        "Region", "nonlocal_shr", "q_pre",
        "totval_pre", "n_pot_local", "n_pre_tndr",
    ))
    print("-" * 75)
    for _, row in mods.sort_values(REGION_COL).iterrows():
        print(hdr.format(
            str(row[REGION_COL])[:34],
            f"{row.get('nonlocal_share_pre_raw', np.nan):.3f}",
            f"{row.get('q_pre_raw', np.nan):.1f}",
            f"{row.get('totval_pre_raw', np.nan):.0f}",
            f"{row.get('n_pot_local_raw', np.nan):.0f}",
            f"{row.get('n_pre_tenders', np.nan):.0f}",
        ))
    print("=" * 75)

    below = mods[mods["n_pre_tenders"].fillna(0) < MIN_PRE_TENDERS][REGION_COL].tolist()
    if below:
        print(f"\n  [INFO] Regions excluded from coefplot (<{MIN_PRE_TENDERS} tenders): {below}")


# ════════════════════════════════════════════════════════════════════════════
# ── Step 2: Interacted DiD regressions ───────────────────────────────────────
# ════════════════════════════════════════════════════════════════════════════
def _prep_interacted(
    df: pd.DataFrame,
    outcome_col: str,
    moderator_col: str,
    extra_endog: str | None = None,
) -> pd.DataFrame | None:
    """
    Prepare the sub-sample for an interacted DiD regression.

    Adds columns:
      did_z       = did × Z_r          (instrument for IV; regressor for OLS)
      ca_post_z   = ca_post × Z_r      (endogenous for IV — only if extra_endog set)

    Returns None if sample too small.
    """
    did_z     = f"did_{moderator_col}"
    base_cols = [outcome_col, ENTITY_COL, TIME_COL, DID_COL, TREAT_COL,
                 moderator_col]
    if CLUSTER_COL not in base_cols:
        base_cols.append(CLUSTER_COL)
    if extra_endog and extra_endog not in base_cols:
        base_cols.append(extra_endog)

    sub = df[list(dict.fromkeys(c for c in base_cols if c in df.columns))].copy()
    sub[did_z] = sub[DID_COL] * sub[moderator_col]

    if extra_endog and extra_endog in sub.columns:
        sub[f"{extra_endog}_{moderator_col}"] = sub[extra_endog] * sub[moderator_col]

    drop_on = [outcome_col, DID_COL, did_z, ENTITY_COL, TIME_COL]
    if extra_endog and extra_endog in sub.columns:
        drop_on += [extra_endog, f"{extra_endog}_{moderator_col}"]

    sub = sub.replace([np.inf, -np.inf], np.nan).dropna(subset=drop_on)

    # Drop singleton entities
    ent_sz = sub.groupby(ENTITY_COL)[outcome_col].transform("count")
    sub    = sub[ent_sz > 1].copy()

    return sub if len(sub) >= 50 else None


def run_interacted_ols(
    df: pd.DataFrame,
    outcome_col: str,
    moderator_col: str,
    label: str = "",
) -> dict | None:
    """
    OLS interacted DiD:
      ỹ = β1·(T×Post)̃ + β2·(T×Post×Z)̃ + ε
    after entity + year-month two-way demeaning.
    SEs clustered by procuring entity.
    """
    did_z = f"did_{moderator_col}"
    sub   = _prep_interacted(df, outcome_col, moderator_col)
    if sub is None:
        return None

    dm    = _twoway_demean(sub, ENTITY_COL, TIME_COL, [outcome_col, DID_COL, did_z])
    X     = dm[[DID_COL, did_z]].to_numpy(dtype=float)
    y     = dm[outcome_col].to_numpy(dtype=float)
    clu   = sub[CLUSTER_COL].to_numpy()

    valid = np.isfinite(X).all(axis=1) & np.isfinite(y)
    X, y, clu = X[valid], y[valid], clu[valid]
    if len(y) < 50:
        return None

    coefs, _, _, _ = np.linalg.lstsq(X, y, rcond=None)
    resid = y - X @ coefs
    se    = _cluster_se(X, resid, clu)

    b1, s1 = float(coefs[0]), float(se[0])
    b2, s2 = float(coefs[1]), float(se[1])

    return {
        "outcome"    : outcome_col,
        "label"      : label,
        "moderator"  : moderator_col,
        "estimator"  : "OLS",
        "beta1"      : b1,
        "se1"        : s1,
        "pval1"      : _pval(b1, s1),
        "beta2"      : b2,
        "se2"        : s2,
        "pval2"      : _pval(b2, s2),
        "N"          : int(len(y)),
        "n_entities" : int(len(np.unique(clu))),
    }


def run_interacted_iv(
    df: pd.DataFrame,
    outcome_col: str,
    moderator_col: str,
    label: str = "",
) -> dict | None:
    """
    IV-DiD with interacted moderator (Wooldridge approach).

    Endogenous:  [ca_post,       ca_post × Z_r]
    Instruments: [did (T×Post),  did × Z_r    ]

    2SLS with k=2, exactly identified.
    Cluster-robust SE via sandwich: V = (Z'X)^{-1} B (X'Z)^{-1}.
    """
    if "ca_post" not in df.columns:
        return None

    did_z      = f"did_{moderator_col}"
    ca_post_z  = f"ca_post_{moderator_col}"

    sub = _prep_interacted(df, outcome_col, moderator_col, extra_endog="ca_post")
    if sub is None:
        return None
    if sub["ca_post"].nunique() < 2:
        return None

    dm_cols = [outcome_col, DID_COL, did_z, "ca_post", ca_post_z]
    dm_cols = [c for c in dm_cols if c in sub.columns]
    dm  = _twoway_demean(sub, ENTITY_COL, TIME_COL, dm_cols)

    X   = dm[["ca_post", ca_post_z]].to_numpy(dtype=float)
    Z   = dm[[DID_COL, did_z]].to_numpy(dtype=float)
    y   = dm[outcome_col].to_numpy(dtype=float)
    clu = sub[CLUSTER_COL].to_numpy()

    valid = np.isfinite(X).all(axis=1) & np.isfinite(Z).all(axis=1) & np.isfinite(y)
    X, Z, y, clu = X[valid], Z[valid], y[valid], clu[valid]
    if len(y) < 50:
        return None

    n, k = X.shape

    # 2SLS: β̂ = (Z'X)^{-1} Z'y
    ZtX = Z.T @ X
    Zty = Z.T @ y
    try:
        ZtX_inv = np.linalg.inv(ZtX)
    except np.linalg.LinAlgError:
        ZtX_inv = np.linalg.pinv(ZtX)

    coefs = ZtX_inv @ Zty
    resid = y - X @ coefs   # structural residuals (actual X)

    # Cluster-robust sandwich SE: V = (Z'X)^{-1} B (X'Z)^{-1}
    cluster_codes, _ = pd.factorize(clu, sort=False)
    G = int(np.max(cluster_codes)) + 1 if cluster_codes.size else 0
    if G == 0:
        se = np.full(k, np.nan)
    else:
        scores = np.zeros((G, k), dtype=float)
        for j in range(k):
            np.add.at(scores[:, j], cluster_codes, Z[:, j] * resid)
        B    = scores.T @ scores
        hc1  = (G / max(G - 1, 1)) * (n / max(n - k, 1))
        V    = hc1 * (ZtX_inv @ B @ ZtX_inv.T)
        se   = np.sqrt(np.clip(np.diag(V), 0.0, None))

    b1, s1 = float(coefs[0]), float(se[0])
    b2, s2 = float(coefs[1]), float(se[1])

    return {
        "outcome"    : outcome_col,
        "label"      : label,
        "moderator"  : moderator_col,
        "estimator"  : "IV",
        "beta1"      : b1,
        "se1"        : s1,
        "pval1"      : _pval(b1, s1),
        "beta2"      : b2,
        "se2"        : s2,
        "pval2"      : _pval(b2, s2),
        "N"          : int(len(y)),
        "n_entities" : int(len(np.unique(clu))),
    }


def _save_interacted_table(results: list[dict], moderator: str) -> None:
    """Save OLS + IV interacted results for one moderator to CSV and LaTeX."""
    if not results:
        return

    rows = []
    for r in results:
        rows.append({
            "outcome"      : r["outcome"],
            "label"        : r["label"],
            "type"         : r["estimator"],
            "beta1"        : r["beta1"],
            "se1"          : r["se1"],
            "stars_beta1"  : _stars(r["pval1"]),
            "beta2"        : r["beta2"],
            "se2"          : r["se2"],
            "stars_beta2"  : _stars(r["pval2"]),
            "N"            : r["N"],
            "n_entities"   : r["n_entities"],
        })

    df  = pd.DataFrame(rows)
    stem = f"hetero_interacted_{moderator}_{SUBSAMPLE}"

    # CSV
    df.to_csv(OUT_TABLES / f"{stem}.csv", index=False)

    # LaTeX — simple tabular
    def _fmt(val, decimals=4):
        if not np.isfinite(float(val)):
            return "—"
        return f"{val:.{decimals}f}"

    df_iv = df[df["type"] == "IV"]
    lines = [
        r"\begin{tabular}{lrrrrrr}",
        r"\toprule",
        r"Outcome & $\hat\beta_1$ & SE$_1$ & $\hat\beta_2$ & SE$_2$ & $N$ & $N_{\rm ent}$ \\",
        r"\midrule",
    ]
    for _, row in df_iv.iterrows():
        label = row['label'].replace(" & ", r" \& ")
        s1 = row['stars_beta1'].replace("*", r"^*").replace("^*^*", "^{**}").replace("^*^*^*", "^{***}")
        s2 = row['stars_beta2'].replace("*", r"^*").replace("^*^*", "^{**}").replace("^*^*^*", "^{***}")
        # Reformat stars: *** -> ^{***}, ** -> ^{**}, * -> ^{*}
        def _fmt_stars(s: str) -> str:
            if s == "***":   return "^{***}"
            if s == "**":    return "^{**}"
            if s == "*":     return "^{*}"
            return ""
        c1 = f"{_fmt(row['beta1'])}{_fmt_stars(row['stars_beta1'])}"
        c2 = f"{_fmt(row['beta2'])}{_fmt_stars(row['stars_beta2'])}"
        lines.append(
            f"{label} & "
            f"${c1}$ & $({_fmt(row['se1'])})$ & "
            f"${c2}$ & $({_fmt(row['se2'])})$ & "
            f"{int(row['N']):,} & {int(row['n_entities']):,} \\\\"
        )
    lines += [
        r"\bottomrule",
        r"\end{tabular}",
    ]
    (OUT_TABLES / f"{stem}.tex").write_text("\n".join(lines), encoding="utf-8")
    print(f"  Saved: {stem}.{{csv,tex}}")


# ════════════════════════════════════════════════════════════════════════════
# ── Step 3: Region-specific IV-DiD coefficient plot ──────────────────────────
# ════════════════════════════════════════════════════════════════════════════
def run_region_iv(
    df: pd.DataFrame,
    outcome_col: str,
    eligible_regions: list,
) -> pd.DataFrame:
    """
    Fully-interacted IV-DiD — one treatment effect δ_r per eligible region.

    Endogenous: ca_post × 1[region == r]   for each r
    Instruments: did     × 1[region == r]  for each r

    Returns DataFrame with columns:
      REGION_COL, coef, se, ci95_low, ci95_high, pval
    """
    if "ca_post" not in df.columns or REGION_COL not in df.columns:
        return pd.DataFrame()
    if not eligible_regions:
        return pd.DataFrame()

    n_reg = len(eligible_regions)
    sub   = df.copy()

    endog_cols = []
    instr_cols = []
    for i, reg in enumerate(eligible_regions):
        flag = (sub[REGION_COL] == reg).astype("float64")
        ec   = f"_endog_{i}"
        ic   = f"_instr_{i}"
        sub[ec] = sub["ca_post"] * flag
        sub[ic] = sub[DID_COL]   * flag
        endog_cols.append(ec)
        instr_cols.append(ic)

    needed = list(dict.fromkeys(
        [outcome_col, ENTITY_COL, TIME_COL, CLUSTER_COL]
        + endog_cols + instr_cols
    ))
    sub = (sub[needed]
           .replace([np.inf, -np.inf], np.nan)
           .dropna(subset=[outcome_col, ENTITY_COL, TIME_COL])
           .copy())

    ent_sz = sub.groupby(ENTITY_COL)[outcome_col].transform("count")
    sub    = sub[ent_sz > 1].copy()
    if len(sub) < 50:
        return pd.DataFrame()

    # Two-way demean
    dm_cols = [outcome_col] + endog_cols + instr_cols
    dm      = _twoway_demean(sub, ENTITY_COL, TIME_COL, dm_cols)

    X   = dm[endog_cols].to_numpy(dtype=float)
    Z   = dm[instr_cols].to_numpy(dtype=float)
    y   = dm[outcome_col].to_numpy(dtype=float)
    clu = sub[CLUSTER_COL].to_numpy()

    valid = np.isfinite(X).all(axis=1) & np.isfinite(Z).all(axis=1) & np.isfinite(y)
    X, Z, y, clu = X[valid], Z[valid], y[valid], clu[valid]
    if len(y) < 50:
        return pd.DataFrame()

    n, k = X.shape

    # 2SLS
    ZtX = Z.T @ X
    Zty = Z.T @ y
    try:
        ZtX_inv = np.linalg.inv(ZtX)
    except np.linalg.LinAlgError:
        ZtX_inv = np.linalg.pinv(ZtX)

    coefs = ZtX_inv @ Zty
    resid = y - X @ coefs

    # Cluster-robust SE
    cluster_codes, _ = pd.factorize(clu, sort=False)
    G = int(np.max(cluster_codes)) + 1 if cluster_codes.size else 0
    if G == 0:
        se = np.full(k, np.nan)
    else:
        scores = np.zeros((G, k), dtype=float)
        for j in range(k):
            np.add.at(scores[:, j], cluster_codes, Z[:, j] * resid)
        B   = scores.T @ scores
        hc1 = (G / max(G - 1, 1)) * (n / max(n - k, 1))
        V   = hc1 * (ZtX_inv @ B @ ZtX_inv.T)
        se  = np.sqrt(np.clip(np.diag(V), 0.0, None))

    rows = []
    for i, reg in enumerate(eligible_regions):
        c, s = float(coefs[i]), float(se[i])
        rows.append({
            REGION_COL  : reg,
            "coef"      : c,
            "se"        : s,
            "ci95_low"  : c - 1.96 * s,
            "ci95_high" : c + 1.96 * s,
            "pval"      : _pval(c, s),
        })

    return pd.DataFrame(rows)


def _mod_raw_by_region(mods: pd.DataFrame, mod: str) -> pd.Series:
    """
    Return a Series of raw (unstandardized) moderator values indexed by REGION_COL.
    Used to order regions vertically in the coefficient plot.
    """
    raw_col = f"{mod}_raw"
    col = raw_col if raw_col in mods.columns else mod
    return mods.set_index(REGION_COL)[col].dropna()


def _pre_means_by_region(df: pd.DataFrame, outcome_col: str) -> pd.Series:
    """
    Compute the pre-reform mean of outcome_col per region.
    Used to order regions in the outcome-ordered coefficient plot.
    """
    pre = df[df["fecha_pub"] < _PRE_CUTOFF].copy()
    if outcome_col not in pre.columns or REGION_COL not in pre.columns:
        return pd.Series(dtype="float64", name=outcome_col)
    return (
        pre.groupby(REGION_COL)[outcome_col]
        .mean()
        .dropna()
        .rename(outcome_col)
    )


def _fmt_mod_val(val: float, series: pd.Series) -> str:
    """Format a moderator value for the region label, adapting to scale."""
    max_val = float(series.max())
    if max_val < 10:
        return f"{val:.2f}"
    elif max_val < 1000:
        return f"{val:.1f}"
    else:
        return f"{val:,.0f}"


def plot_region_coef(
    coef_df:       pd.DataFrame,
    outcome_label: str,
    mod_raw:       pd.Series,   # raw moderator values indexed by REGION_COL
    mod_label:     str,         # human-readable moderator name for subtitle
    out_path:      Path,
) -> None:
    """
    Horizontal coefficient plot — one row per region, one plot per outcome×moderator.

    Ordering: highest raw moderator value at top.
    Labels:   "Short Name (value)" where value is the raw moderator value.
    Colors:   blue  if δ̂ > 0 and p < 0.10
              red   if δ̂ < 0 and p < 0.10
              gray  otherwise
    """
    df = coef_df.copy()

    # Attach raw moderator values for ordering and labelling
    df = df.join(mod_raw.rename("_mod_val"), on=REGION_COL, how="left")

    # Build label: "Short Name (formatted value)"
    def _make_label(row: pd.Series) -> str:
        short = _strip_region_prefix(str(row[REGION_COL]))
        if pd.notna(row["_mod_val"]):
            return f"{short} ({_fmt_mod_val(row['_mod_val'], mod_raw)})"
        return short

    df["_label"] = df.apply(_make_label, axis=1)

    # Highest on top → ascending sort puts smallest at index 0 (bottom of plot)
    df = df.sort_values("_mod_val", ascending=True).reset_index(drop=True)

    def _color(row: pd.Series) -> str:
        if np.isfinite(row["pval"]) and row["pval"] < 0.1:
            return "#2166AC" if row["coef"] > 0 else "#D6604D"
        return "#AAAAAA"

    df["_color"] = df.apply(_color, axis=1)

    n     = len(df)
    y_pos = np.arange(n)

    fig, ax = plt.subplots(figsize=(7, 9))
    fig.patch.set_facecolor("white")
    ax.set_facecolor("white")

    # Light gray horizontal gridlines, drawn behind estimates
    ax.set_axisbelow(True)
    ax.yaxis.grid(True, color="#DDDDDD", linewidth=0.6, linestyle="-")
    ax.xaxis.grid(False)

    # Zero reference (dotted, zorder=1 → behind estimates)
    ax.axvline(0, lw=1, ls=":", color="#555555", zorder=1)

    # CI lines then point estimates (zorder 2 & 3)
    for i, row in df.iterrows():
        col = row["_color"]
        ax.plot([row["ci95_low"], row["ci95_high"]], [i, i],
                lw=1.2, color=col, zorder=2)
        ax.plot(row["coef"], i, "o", ms=5, color=col,
                markerfacecolor=col, zorder=3)

    ax.set_yticks(y_pos)
    ax.set_yticklabels(df["_label"].tolist(), fontsize=10)
    ax.set_xlabel(outcome_label, fontsize=10)
    ax.set_title(f"Ordered by: {mod_label}", fontsize=9, color="#555555", pad=4)
    ax.tick_params(axis="x", labelsize=10)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_visible(False)

    fig.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print(f"  Saved: {out_path.name}")


# ════════════════════════════════════════════════════════════════════════════
# ── Step 4b: Post-regression summary of significant β̂₂ ─────────────────────
# ════════════════════════════════════════════════════════════════════════════
def print_beta2_summary(all_results: list[dict]) -> None:
    sig = [r for r in all_results if np.isfinite(r.get("pval2", np.nan)) and r["pval2"] < 0.1]
    print("\n" + "=" * 75)
    print(f"SUMMARY: Significant β̂₂ coefficients (p < 0.10) — {len(sig)} found")
    print("=" * 75)
    if not sig:
        print("  None.")
        return
    fmt = "{:<8} {:<30} {:<24} {:>9} {:>9} {:>5}"
    print(fmt.format("Type", "Outcome", "Moderator", "β̂₂", "SE₂", "sig"))
    print("-" * 75)
    for r in sig:
        print(fmt.format(
            r["estimator"],
            r["label"][:30],
            r["moderator"][:24],
            f"{r['beta2']:+.4f}",
            f"({r['se2']:.4f})",
            _stars(r["pval2"]),
        ))
    print("=" * 75)
    print("  *** p<0.01  ** p<0.05  * p<0.10  |  SE clustered by procuring entity")


# ════════════════════════════════════════════════════════════════════════════
# ── Main ─────────────────────────────────────────────────────────────────────
# ════════════════════════════════════════════════════════════════════════════
def main() -> None:
    print("=" * 75)
    print("05_heterogeneity_region.py — Compra Ágil Regional Heterogeneity")
    print(f"  SUBSAMPLE={SUBSAMPLE!r}   CONTROL={CONTROL!r}")
    print("=" * 75)

    # ── Load DiD samples ───────────────────────────────────────────────────────
    print("\nLoading DiD samples …")
    df_tender = load_tender()
    df_bid    = load_bid()
    df_tender = _augment_tender(df_tender, df_bid)

    # Endogenous CA indicator for IV (= 1 if processed via Compra Ágil)
    df_tender["ca_post"] = (df_tender["dataset"] == "compra_agil").astype("float64")
    df_bid["ca_post"]    = (df_bid["dataset"]    == "compra_agil").astype("float64")

    # Band restriction: Control 2 (100–200 UTM) + treated (30–100 UTM)
    band_keep = ["control_high", "treated"]
    df_tender = df_tender[df_tender["band"].isin(band_keep)].copy()
    df_bid    = df_bid[df_bid["band"].isin(band_keep)].copy()

    # Subsample restriction by sector
    df_tender = _apply_subsample(df_tender)
    df_bid    = _apply_subsample(df_bid)
    print(f"  After filters: tender={len(df_tender):,}, bid={len(df_bid):,}")

    # ── Step 1: Build region-level moderators ─────────────────────────────────
    print("\nStep 1: Building region-level moderators …")
    mods = build_region_moderators(df_tender, df_bid)

    # ── Step 4a: Sanity checks ────────────────────────────────────────────────
    print_sanity_checks(mods)

    # Merge standardized moderators into DiD samples
    mod_cols = [REGION_COL] + MODERATORS + [f"{m}_raw" for m in MODERATORS] + ["n_pre_tenders"]
    mod_cols = [c for c in mod_cols if c in mods.columns]
    df_tender = df_tender.merge(mods[mod_cols], on=REGION_COL, how="left")

    if REGION_COL in df_bid.columns:
        bid_mod_cols = [REGION_COL] + MODERATORS
        df_bid = df_bid.merge(mods[bid_mod_cols], on=REGION_COL, how="left")

    # Eligible regions for coefplot
    eligible_regions = (
        mods.loc[mods["n_pre_tenders"].fillna(0) >= MIN_PRE_TENDERS, REGION_COL]
        .dropna().tolist()
    )
    print(
        f"\n  Eligible regions for coefplot "
        f"(>={MIN_PRE_TENDERS} pre-reform tenders): {len(eligible_regions)}"
    )

    # ── Step 2: Interacted DiD regressions ───────────────────────────────────
    print("\n" + "=" * 75)
    print("Step 2: Interacted DiD regressions")
    print("=" * 75)

    all_results: list[dict] = []

    for mod in MODERATORS:
        print(f"\n  ── Moderator: {mod} ──")
        mod_results: list[dict] = []

        for outcome_col, outcome_label in OUTCOMES:
            src = df_bid if outcome_col in _BID_OUTCOMES else df_tender
            if outcome_col not in src.columns:
                continue
            if src[outcome_col].notna().sum() < 50:
                continue
            if mod not in src.columns or src[mod].notna().sum() < 10:
                continue

            # OLS
            r_ols = run_interacted_ols(src, outcome_col, mod, label=outcome_label)
            if r_ols:
                print(
                    f"  OLS  {outcome_label:<30} β₁={r_ols['beta1']:+.4f}"
                    f" β₂={r_ols['beta2']:+.4f}{_stars(r_ols['pval2'])}  n={r_ols['N']:,}"
                )
                mod_results.append(r_ols)
                all_results.append(r_ols)

            # IV
            r_iv = run_interacted_iv(src, outcome_col, mod, label=outcome_label)
            if r_iv:
                print(
                    f"  IV   {outcome_label:<30} β₁={r_iv['beta1']:+.4f}"
                    f" β₂={r_iv['beta2']:+.4f}{_stars(r_iv['pval2'])}  n={r_iv['N']:,}"
                )
                mod_results.append(r_iv)
                all_results.append(r_iv)

        _save_interacted_table(mod_results, mod)

    # ── Step 4b: Significant β̂₂ summary ──────────────────────────────────────
    print_beta2_summary(all_results)

    # ── Step 3: Region-specific coefficient plots ─────────────────────────────
    # One plot per outcome × moderator: regions ordered by raw moderator value.
    print("\n" + "=" * 75)
    print("Step 3: Region-specific coefficient plots")
    print("=" * 75)

    # Human-readable labels for plot subtitles
    _MOD_LABELS = {
        "nonlocal_share_pre" : "Pre-reform non-local bidder share",
        "q_pre"              : "Pre-reform avg. monthly tender count",
        "totval_pre"         : "Pre-reform avg. monthly tender value",
        "n_pot_local"        : "Pre-reform potential local firms",
        "outcome"            : "Pre-reform outcome mean",
    }

    for outcome_col, outcome_label in OUTCOMES:
        src = df_bid if outcome_col in _BID_OUTCOMES else df_tender
        if outcome_col not in src.columns:
            continue
        if REGION_COL not in src.columns:
            continue
        if src[outcome_col].notna().sum() < 50:
            continue

        print(f"\n  [{outcome_label}]")
        # Run the region IV once — same regression for all orderings
        coef_df = run_region_iv(src, outcome_col, eligible_regions)
        if coef_df.empty:
            print(f"    — no results (sample too small).")
            continue

        # 4 moderator orderings
        for mod in MODERATORS:
            mod_raw  = _mod_raw_by_region(mods, mod)
            mod_lbl  = _MOD_LABELS.get(mod, mod)
            out_path = OUT_FIGURES / f"hetero_coefplot_{outcome_col}_{mod}_{SUBSAMPLE}.png"
            plot_region_coef(coef_df, outcome_label, mod_raw, mod_lbl, out_path)

        # 5th ordering: pre-reform outcome mean
        pre_means = _pre_means_by_region(src, outcome_col)
        out_path  = OUT_FIGURES / f"hetero_coefplot_{outcome_col}_outcome_{SUBSAMPLE}.png"
        plot_region_coef(coef_df, outcome_label, pre_means, _MOD_LABELS["outcome"], out_path)

    print("\nDone.")


if __name__ == "__main__":
    main()
