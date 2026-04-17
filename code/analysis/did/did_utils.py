"""
did_utils.py
─────────────────────────────────────────────────────────────────────────────
Shared utilities for the Compra Ágil DiD analysis.

Contents
  · Path constants
  · UTM loading & value conversion
  · Value-band assignment and DiD indicator construction
  · TWFE regression with cluster-robust SEs (entity + time FE)
  · Event-study TWFE variant (monthly interaction coefficients)
  · Results formatting and coefficient plot helper
"""

from __future__ import annotations

import math
from pathlib import Path
from typing import Sequence

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy.special import ndtr

matplotlib.use("Agg")

# ── Paths ─────────────────────────────────────────────────────────────────────
ROOT        = Path(__file__).resolve().parents[3]
DATA_CLEAN  = ROOT / "data" / "clean"
DATA_OTHER  = ROOT / "data" / "raw" / "other"
LIC_PANEL   = DATA_CLEAN / "chilecompra_panel.parquet"
CA_PANEL    = DATA_CLEAN / "compra_agil_panel.parquet"
COMBINED    = DATA_CLEAN / "combined_sii_merged_filtered.parquet"
RUT_SECTOR_CROSSWALK = DATA_CLEAN / "rut_unidad_sector_crosswalk.parquet"

OUT_DIR     = ROOT / "output" / "did"
OUT_TABLES  = OUT_DIR / "tables"
OUT_FIGURES = OUT_DIR / "figures"
OUT_SAMPLES = OUT_DIR / "samples"

for _d in [OUT_TABLES, OUT_FIGURES, OUT_SAMPLES]:
    _d.mkdir(parents=True, exist_ok=True)

# ── Reform constants ──────────────────────────────────────────────────────────
REFORM_DATE   = pd.Timestamp("2024-12-12")
REFORM_PERIOD = pd.Period("2024-12", freq="M")   # month the reform took effect
OMIT_PERIOD   = pd.Period("2024-11", freq="M")   # omitted (baseline) month in event study

# Band boundaries (UTM)
BAND_LOW_MIN   =   1.0
BAND_LOW_MAX   =  30.0   # old CA ceiling (exclusive upper bound for control_low)
BAND_TREAT_MAX = 100.0   # new CA ceiling (inclusive upper bound for treated)
BAND_HIGH_MAX  = 200.0   # upper bound for control_high

# ── UTM loading & conversion ──────────────────────────────────────────────────
def load_utm_table() -> pd.DataFrame:
    """Load monthly UTM→CLP conversion table."""
    utm = pd.read_csv(DATA_OTHER / "utm_clp_2022_2025.csv")
    utm.columns = [c.strip() for c in utm.columns]
    rename = {}
    for c in utm.columns:
        cl = c.lower().strip()
        if cl in ("year", "anio", "año"):
            rename[c] = "source_year"
        elif cl in ("month_num", "month", "mes", "month_num"):
            rename[c] = "source_month"
        elif "utm" in cl:
            rename[c] = "utm_clp"
    # Deduplicate: if multiple columns map to the same target name (e.g. both
    # "month" and "month_num" → "source_month"), keep only the last mapping so
    # month_num (numeric) wins over month (text label).
    seen: dict[str, str] = {}
    for src, tgt in rename.items():
        seen[tgt] = src
    rename = {src: tgt for tgt, src in seen.items()}
    utm = utm.rename(columns=rename)
    utm["source_year"]  = pd.to_numeric(utm["source_year"],  errors="coerce").astype("Int64")
    utm["source_month"] = pd.to_numeric(utm["source_month"], errors="coerce").astype("Int64")
    utm["utm_clp"]      = pd.to_numeric(utm["utm_clp"],      errors="coerce")
    return utm[["source_year", "source_month", "utm_clp"]].dropna()


def add_utm_value(df: pd.DataFrame, utm: pd.DataFrame) -> pd.DataFrame:
    """Merge monthly UTM rate and compute monto_utm (estimated value in UTM)."""
    out = df.merge(utm, on=["source_year", "source_month"], how="left")
    out["monto_utm"] = out["monto_estimado"] / out["utm_clp"]
    return out


# ── Band assignment & DiD indicators ─────────────────────────────────────────
def assign_band(df: pd.DataFrame) -> pd.DataFrame:
    """
    Assign value-band and DiD indicators.

    Bands (estimated value in UTM):
      control_low  : [1, 30)    — always Compra Ágil under both regimes
      treated      : [30, 100]  — shifted from licitación to Compra Ágil by reform
      control_high : (100, 200] — always licitación under both regimes

    Adds columns
      band    : string label above
      treated : 1 iff band == 'treated'
      post    : 1 iff fecha_pub >= REFORM_DATE
      did     : treated * post   (the DiD interaction)
      year_month : monthly Period for FE / event-study
    """
    v  = df["monto_utm"]
    conditions = [
        (v >= BAND_LOW_MIN)   & (v <  BAND_LOW_MAX),
        (v >= BAND_LOW_MAX)   & (v <= BAND_TREAT_MAX),
        (v >  BAND_TREAT_MAX) & (v <= BAND_HIGH_MAX),
    ]
    choices = ["control_low", "treated", "control_high"]
    df = df.copy()
    df["band"]    = np.select(conditions, choices, default=pd.NA)
    df            = df[df["band"].notna()].copy()
    df["treated"] = (df["band"] == "treated").astype("int8")
    df["post"]    = (df["fecha_pub"] >= REFORM_DATE).astype("int8")
    df["did"]     = (df["treated"] * df["post"]).astype("int8")
    df["year_month"] = df["fecha_pub"].dt.to_period("M")
    return df


# ── Cluster-robust sandwich SE ────────────────────────────────────────────────
def _cluster_se(
    X: np.ndarray,
    resid: np.ndarray,
    cluster_ids: np.ndarray,
) -> np.ndarray:
    """
    HC1-scaled cluster-robust SEs.

    Var(β) = (X'X)^{-1} · B · (X'X)^{-1}
    B = [G/(G-1)] · [n/(n-k)] · Σ_g (X_g' e_g)(X_g' e_g)'
    """
    n, k  = X.shape
    if n == 0:
        return np.full(k, np.nan, dtype=float)

    # Factorize once to avoid O(n * G) boolean-mask loops over clusters.
    # This is critical for large samples (millions of bids).
    cluster_codes, uniques = pd.factorize(cluster_ids, sort=False)
    valid = cluster_codes >= 0
    if not np.all(valid):
        X = X[valid]
        resid = resid[valid]
        cluster_codes = cluster_codes[valid]
        n = X.shape[0]
    if n == 0:
        return np.full(k, np.nan, dtype=float)

    XtX   = X.T @ X
    try:
        XtX_inv = np.linalg.inv(XtX)
    except np.linalg.LinAlgError:
        XtX_inv = np.linalg.pinv(XtX)

    G = int(np.max(cluster_codes)) + 1 if cluster_codes.size else 0
    if G == 0:
        return np.full(k, np.nan, dtype=float)

    # score_g = X_g' e_g for each cluster g (shape: G x k)
    scores = np.zeros((G, k), dtype=float)
    for j in range(k):
        np.add.at(scores[:, j], cluster_codes, X[:, j] * resid)
    B = scores.T @ scores

    hc1_scale = (G / max(G - 1, 1)) * (n / max(n - k, 1))
    V   = hc1_scale * (XtX_inv @ B @ XtX_inv)
    return np.sqrt(np.clip(np.diag(V), 0.0, None))


# ── Full cluster-robust covariance matrix (for joint Wald tests) ─────────────
def _cluster_cov(
    X: np.ndarray,
    resid: np.ndarray,
    cluster_ids: np.ndarray,
) -> np.ndarray:
    """
    HC1-scaled cluster-robust covariance matrix (k × k full matrix).

    Same sandwich formula as _cluster_se but returns V instead of sqrt(diag(V)).
    Use for joint Wald tests (e.g. pre-trend F-tests on event-study coefficients).
    """
    n, k = X.shape
    if n == 0:
        return np.full((k, k), np.nan, dtype=float)

    cluster_codes, _ = pd.factorize(cluster_ids, sort=False)
    valid = cluster_codes >= 0
    if not np.all(valid):
        X, resid, cluster_codes = X[valid], resid[valid], cluster_codes[valid]
        n = X.shape[0]
    if n == 0:
        return np.full((k, k), np.nan, dtype=float)

    XtX = X.T @ X
    try:
        XtX_inv = np.linalg.inv(XtX)
    except np.linalg.LinAlgError:
        XtX_inv = np.linalg.pinv(XtX)

    G = int(np.max(cluster_codes)) + 1 if cluster_codes.size else 0
    if G == 0:
        return np.full((k, k), np.nan, dtype=float)

    scores = np.zeros((G, k), dtype=float)
    for j in range(k):
        np.add.at(scores[:, j], cluster_codes, X[:, j] * resid)
    B = scores.T @ scores

    hc1_scale = (G / max(G - 1, 1)) * (n / max(n - k, 1))
    return hc1_scale * (XtX_inv @ B @ XtX_inv)


# ── Two-way FE demeaning (entity + time) via alternating projections ──────────
def _twoway_demean(
    df: pd.DataFrame,
    entity_col: str,
    time_col: str,
    cols: list[str],
    n_iter: int = 15,
) -> pd.DataFrame:
    """
    Partial out entity and time FEs from `cols` via alternating projections.

    Memory: O(n × |cols|) — no dummy matrix required.
    Mathematically equivalent to the within-estimator with explicit dummies;
    converges in 3–5 iterations for typical unbalanced panels.
    """
    result   = df[cols].astype("float64").copy()
    ent_ids  = df[entity_col]
    time_ids = df[time_col]
    for _ in range(n_iter):
        ent_mean  = result.groupby(ent_ids,  sort=False).transform("mean")
        result   -= ent_mean
        time_mean = result.groupby(time_ids, sort=False).transform("mean")
        result   -= time_mean
    return result


# ── Pooled TWFE DiD ───────────────────────────────────────────────────────────
def run_twfe_did(
    df: pd.DataFrame,
    *,
    outcome_col: str,
    entity_col:  str = "rut_unidad",
    time_col:    str = "year_month",
    did_col:     str = "did",
    treat_col:   str = "treated",
    cluster_col: str = "rut_unidad",
    extra_controls: Sequence[str] = (),
    min_obs: int = 50,
    label:   str = "",
) -> dict:
    """
    Two-way FE DiD with cluster-robust SEs.

    Model (FWL representation after entity demeaning):
      ỹ_it = β_did · d̃_it + β_treat · treat̃_it
             + Σ_t γ_t · M̃_it,t + ε_it

    where tilde denotes entity-demeaned variables and M̃ are entity-demeaned
    year-month dummies (partialling out time FE).

    Returns a dict with coefficient, SE, t-stat, p-value, and diagnostics.
    """
    extra_controls = list(extra_controls)
    needed = [outcome_col, entity_col, time_col, did_col, treat_col] + extra_controls
    if cluster_col not in needed:
        needed.append(cluster_col)
    sub = df[needed].replace([np.inf, -np.inf], np.nan).dropna().copy()

    if len(sub) < min_obs:
        print(f"  [{label}] skip — {len(sub)} obs after dropna (need {min_obs}).")
        return {}

    # Drop singleton entities (FE not identified)
    entity_sizes = sub.groupby(entity_col)[outcome_col].transform("count")
    sub = sub[entity_sizes > 1].copy()
    if len(sub) < min_obs:
        print(f"  [{label}] skip — {len(sub)} obs after singleton drop.")
        return {}

    n_months = int(sub[time_col].nunique())

    # ── Two-way FE demeaning (entity + time; no dummy matrix) ─────────────
    # treat_col is time-invariant → fully absorbed by entity FE (treat̃ = 0).
    dm_cols = list(dict.fromkeys([outcome_col, did_col] + extra_controls))
    sub_dm  = _twoway_demean(sub, entity_col, time_col, dm_cols)

    # ── Assemble design matrix ─────────────────────────────────────────────
    regressor_cols = [did_col] + extra_controls
    X   = sub_dm[regressor_cols].to_numpy(dtype=float)
    y   = sub_dm[outcome_col].to_numpy(dtype=float)
    clu = sub[cluster_col].to_numpy()

    valid = np.isfinite(X).all(axis=1) & np.isfinite(y)
    X, y, clu = X[valid], y[valid], clu[valid]
    if len(y) < min_obs:
        print(f"  [{label}] skip — {len(y)} finite obs.")
        return {}

    # ── OLS ───────────────────────────────────────────────────────────────
    coefs, _, _, _ = np.linalg.lstsq(X, y, rcond=None)
    resid          = y - X @ coefs
    se             = _cluster_se(X, resid, clu)

    coef_did = float(coefs[0])
    se_did   = float(se[0])
    t_did    = coef_did / se_did if se_did > 0 else np.nan
    p_did    = float(2 * (1 - ndtr(abs(t_did)))) if np.isfinite(t_did) else np.nan

    return {
        "outcome"    : outcome_col,
        "label"      : label,
        "coef_did"   : coef_did,
        "se_did"     : se_did,
        "tstat_did"  : t_did,
        "pval_did"   : p_did,
        "ci95_low"   : coef_did - 1.96 * se_did,
        "ci95_high"  : coef_did + 1.96 * se_did,
        "coef_treat" : np.nan,   # time-invariant → absorbed by entity FE
        "se_treat"   : np.nan,
        "n_obs"      : int(len(y)),
        "n_entities" : int(len(np.unique(clu))),
        "n_months"   : n_months,
    }


# ── 2SLS IV-TWFE ─────────────────────────────────────────────────────────────
def run_twfe_iv(
    df: pd.DataFrame,
    *,
    outcome_col: str,
    endog_col:   str,   # endogenous regressor (e.g. "ca_post")
    instr_col:   str,   # instrument (e.g. "did" = treated × post)
    entity_col:  str = "rut_unidad",
    time_col:    str = "year_month",
    treat_col:   str = "treated",
    cluster_col: str = "rut_unidad",
    min_obs: int = 50,
    label:   str = "",
) -> dict:
    """
    Two-way FE IV-DiD (2SLS) with cluster-robust SEs.

    Endogenous: endog_col (e.g. whether the tender used Compra Ágil)
    Instrument:  instr_col (e.g. treated × post — the eligibility indicator)
    Exogenous in both X and Z: treat_col + year-month dummies.

    Model (after entity demeaning):
      First stage:  ẽndog = π·ẑ + δ·treat̃ + Σγ_t·M̃ + ν
      Second stage: ỹ = β_IV·ẽndog_hat + δ·treat̃ + Σγ_t·M̃ + ε

    Cluster-robust SE via sandwich formula:
      V(β̂) = (Z'X)^{-1} · B · (X'Z)^{-1}
      B = [G/(G-1)][n/(n-k)] · Σ_g (Z_g'ẽ_g)(ẽ_g'Z_g)
    where ẽ are the 2SLS structural residuals using actual X.

    Returns dict with same keys as run_twfe_did, plus first_stage_coef and
    first_stage_f (first-stage F-stat for the excluded instrument).
    """
    needed = [
        outcome_col, endog_col, instr_col, entity_col,
        time_col, treat_col, cluster_col, "post",
    ]
    # Deduplicate while preserving order (entity_col == cluster_col in typical usage)
    seen: set = set()
    needed_present = []
    for c in needed:
        if c in df.columns and c not in seen:
            needed_present.append(c)
            seen.add(c)
    sub = df[needed_present].replace([np.inf, -np.inf], np.nan).dropna().copy()

    if len(sub) < min_obs:
        return {}

    entity_sizes = sub.groupby(entity_col)[outcome_col].transform("count")
    sub = sub[entity_sizes > 1].copy()
    if len(sub) < min_obs:
        return {}

    n_months = int(sub[time_col].nunique())

    # ── Two-way FE demeaning (entity + time; no dummy matrix) ────────────────
    # treat_col absorbed by entity FE; time FE absorbed by time demeaning.
    dm_cols = list(dict.fromkeys([outcome_col, endog_col, instr_col]))
    sub_dm  = _twoway_demean(sub, entity_col, time_col, dm_cols)

    # ── Build X, Z, y ─────────────────────────────────────────────────────────
    # After two-way demeaning: k=1 each (entity+time FEs already absorbed).
    X   = sub_dm[[endog_col]].to_numpy(dtype=float)
    Z   = sub_dm[[instr_col]].to_numpy(dtype=float)
    y   = sub_dm[outcome_col].to_numpy(dtype=float)
    clu = sub[cluster_col].to_numpy()

    valid = (
        np.isfinite(X).all(axis=1) & np.isfinite(Z).all(axis=1) & np.isfinite(y)
    )
    X, Z, y, clu = X[valid], Z[valid], y[valid], clu[valid]
    if len(y) < min_obs:
        return {}

    n, k = X.shape

    # ── 2SLS: β̂ = (Z'X)^{-1} Z'y ────────────────────────────────────────────
    ZtX = Z.T @ X
    Zty = Z.T @ y
    try:
        ZtX_inv = np.linalg.inv(ZtX)
    except np.linalg.LinAlgError:
        ZtX_inv = np.linalg.pinv(ZtX)
    coefs = ZtX_inv @ Zty                  # 2SLS coefficients
    resid = y - X @ coefs                  # structural residuals (actual X)

    # ── Cluster-robust SE: V = (Z'X)^{-1} B (X'Z)^{-1} ─────────────────────
    cluster_codes, _ = pd.factorize(clu, sort=False)
    valid_cl = cluster_codes >= 0
    if not np.all(valid_cl):
        Z_cl = Z[valid_cl]; resid_cl = resid[valid_cl]
        cluster_codes = cluster_codes[valid_cl]
    else:
        Z_cl, resid_cl = Z, resid

    G = int(np.max(cluster_codes)) + 1 if cluster_codes.size else 0
    if G == 0:
        se = np.full(k, np.nan)
    else:
        scores = np.zeros((G, k), dtype=float)
        for j in range(k):
            np.add.at(scores[:, j], cluster_codes, Z_cl[:, j] * resid_cl)
        B = scores.T @ scores
        hc1  = (G / max(G - 1, 1)) * (n / max(n - k, 1))
        XtZ_inv = ZtX_inv.T   # (X'Z)^{-1} = ((Z'X)^{-1})^T for square matrices
        V    = hc1 * (ZtX_inv @ B @ XtZ_inv)
        se   = np.sqrt(np.clip(np.diag(V), 0.0, None))

    # ── First-stage F-stat for excluded instrument ─────────────────────────
    # Regress endog_tilde on instr_tilde + exog (same as Z) and test coef on instr
    y_fs = X[:, 0]                                       # endog_tilde
    coefs_fs, _, _, _ = np.linalg.lstsq(Z, y_fs, rcond=None)
    resid_fs  = y_fs - Z @ coefs_fs
    se_fs     = _cluster_se(Z, resid_fs, clu)
    t_fs      = coefs_fs[0] / se_fs[0] if (se_fs.size > 0 and se_fs[0] > 0) else np.nan
    f_stat    = float(t_fs ** 2) if np.isfinite(t_fs) else np.nan

    coef_iv = float(coefs[0])
    se_iv   = float(se[0])
    t_iv    = coef_iv / se_iv if se_iv > 0 else np.nan
    p_iv    = float(2 * (1 - ndtr(abs(t_iv)))) if np.isfinite(t_iv) else np.nan

    return {
        "outcome"          : outcome_col,
        "label"            : label,
        "coef_did"         : coef_iv,
        "se_did"           : se_iv,
        "tstat_did"        : t_iv,
        "pval_did"         : p_iv,
        "ci95_low"         : coef_iv - 1.96 * se_iv,
        "ci95_high"        : coef_iv + 1.96 * se_iv,
        "coef_treat"       : np.nan,   # absorbed by entity FE
        "se_treat"         : np.nan,
        "first_stage_coef" : float(coefs_fs.flat[0]),
        "first_stage_f"    : f_stat,
        "n_obs"            : int(len(y)),
        "n_entities"       : int(len(np.unique(clu))),
        "n_months"         : n_months,
    }


# ── Event-study TWFE ──────────────────────────────────────────────────────────
def run_twfe_event_study(
    df: pd.DataFrame,
    *,
    outcome_col: str,
    entity_col:  str = "rut_unidad",
    time_col:    str = "year_month",
    treat_col:   str = "treated",
    cluster_col: str = "rut_unidad",
    pre_periods:  int = 8,
    post_periods: int = 6,
    drop_k0:      bool = False,
    min_obs: int = 50,
    label:   str = "",
) -> pd.DataFrame:
    """
    Event-study TWFE: replace single DiD term with monthly interactions.

    Model:
      ỹ_it = Σ_{k≠-1} β_k · treat_i · 1[t = reform+k]
             + β_treat · treat̃_it + Σ_t γ_t · M̃_it + ε

    Returns a DataFrame with columns: k, period, coef, se, ci95_low, ci95_high.
    β_{-1} = 0 by construction (omitted period = OMIT_PERIOD).
    """
    df = df.copy()
    df["_ym_int"] = df[time_col].apply(lambda p: p.ordinal if hasattr(p, "ordinal") else int(p))
    reform_ord    = REFORM_PERIOD.ordinal
    df["k"]       = df["_ym_int"] - reform_ord

    k_range = list(range(-pre_periods, post_periods + 1))
    _omit_ks = {-1, 0} if drop_k0 else {-1}
    k_range_no_omit = [k for k in k_range if k not in _omit_ks]

    # Build interaction columns  treat * 1[k = k0]
    for k in k_range_no_omit:
        df[f"int_k{k:+d}"] = (df[treat_col] * (df["k"] == k)).astype("int8")

    int_cols = [f"int_k{k:+d}" for k in k_range_no_omit]

    needed = [outcome_col, entity_col, time_col, "k", treat_col] + int_cols
    if cluster_col not in needed:
        needed.append(cluster_col)
    sub = df[needed].replace([np.inf, -np.inf], np.nan).dropna().copy()
    sub = sub[sub["k"].between(-pre_periods, post_periods)].copy()
    if drop_k0:
        sub = sub[sub["k"] != 0].copy()

    if len(sub) < min_obs:
        print(f"  [{label}] event study skip — {len(sub)} obs.")
        return pd.DataFrame()

    entity_sizes = sub.groupby(entity_col)[outcome_col].transform("count")
    sub = sub[entity_sizes > 1].copy()

    # ── Two-way FE demeaning (entity + time; no dummy matrix) ────────────────
    # treat_col is time-invariant → absorbed by entity FE.
    # int_cols vary across both entities and time → survive demeaning.
    dm_cols = list(dict.fromkeys([outcome_col] + int_cols))
    sub_dm  = _twoway_demean(sub, entity_col, time_col, dm_cols)

    regressor_cols = int_cols
    X   = sub_dm[regressor_cols].to_numpy(dtype=float)
    y   = sub_dm[outcome_col].to_numpy(dtype=float)
    clu = sub[cluster_col].to_numpy()

    valid = np.isfinite(X).all(axis=1) & np.isfinite(y)
    X, y, clu = X[valid], y[valid], clu[valid]

    if len(y) < min_obs:
        return pd.DataFrame()

    coefs, _, _, _ = np.linalg.lstsq(X, y, rcond=None)
    resid          = y - X @ coefs
    se             = _cluster_se(X, resid, clu)

    rows = []
    for i, k in enumerate(k_range_no_omit):
        rows.append({
            "k"        : k,
            "period"   : str(REFORM_PERIOD + k),
            "coef"     : float(coefs[i]),
            "se"       : float(se[i]),
            "ci95_low" : float(coefs[i] - 1.96 * se[i]),
            "ci95_high": float(coefs[i] + 1.96 * se[i]),
        })
    # Insert omitted period with zeros
    rows.append({"k": -1, "period": str(OMIT_PERIOD),
                 "coef": 0.0, "se": 0.0, "ci95_low": 0.0, "ci95_high": 0.0})
    es = pd.DataFrame(rows).sort_values("k").reset_index(drop=True)
    es["outcome"] = outcome_col
    es["label"]   = label
    es["n_obs"]   = int(len(y))
    return es


# ── Results formatting ────────────────────────────────────────────────────────
def results_to_df(results: list[dict]) -> pd.DataFrame:
    rows = [r for r in results if r]
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["stars"] = df["pval_did"].apply(
        lambda p: "***" if p < 0.01 else ("**" if p < 0.05 else ("*" if p < 0.1 else ""))
        if pd.notna(p) else ""
    )
    col_order = [
        "label", "outcome", "coef_did", "se_did", "tstat_did", "pval_did",
        "stars", "ci95_low", "ci95_high", "n_obs", "n_entities", "n_months",
        "first_stage_coef", "first_stage_f",
    ]
    col_order = [c for c in col_order if c in df.columns]
    return df[col_order].copy()


# ── Coefficient plot ───────────────────────────────────────────────────────────
def plot_event_study(
    es: pd.DataFrame,
    *,
    title:    str,
    out_path: Path,
    color:    str = "#1B9E77",
    y_label:  str = "Coefficient (vs Nov 2024)",
) -> None:
    """Plot event-study coefficients with 95% CI."""
    if es.empty:
        return
    fig, ax = plt.subplots(figsize=(9, 4))
    k   = es["k"].to_numpy()
    y   = es["coef"].to_numpy()
    lo  = es["ci95_low"].to_numpy()
    hi  = es["ci95_high"].to_numpy()
    yerr = np.vstack([y - lo, hi - y])

    ax.errorbar(k, y, yerr=yerr, fmt="o-", ms=4, lw=1.6,
                capsize=3, color=color, ecolor="#888", elinewidth=1.2)
    ax.axhline(0, lw=1, ls=":", color="#555")
    ax.axvline(0, lw=1.2, ls="--", color="#333",
               label="Reform (Dec 2024)")
    ax.axvline(-1, lw=1, ls=":", color="#aaa",
               label="Omitted period (Nov 2024)")
    ax.set_xlabel("Months relative to reform")
    ax.set_ylabel(y_label)
    ax.set_title(title, fontsize=11, fontweight="bold")
    ax.legend(fontsize=8)
    ax.grid(axis="y", alpha=0.25)
    ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {out_path.name}")


def plot_did_coef_summary(
    results_df: pd.DataFrame,
    *,
    title:    str,
    out_path: Path,
    color:    str = "#1B9E77",
) -> None:
    """Horizontal coefficient plot for multiple outcomes."""
    if results_df.empty:
        return
    df  = results_df.copy().reset_index(drop=True)
    n   = len(df)
    fig, ax = plt.subplots(figsize=(7, max(3, 0.45 * n + 1.5)))
    y_pos = np.arange(n)

    ax.errorbar(
        df["coef_did"], y_pos,
        xerr=1.96 * df["se_did"],
        fmt="o", ms=5, lw=0, capsize=3,
        color=color, ecolor="#888", elinewidth=1.5,
    )
    ax.axvline(0, lw=1, ls="--", color="#444")

    labels = (df.get("label", df.get("outcome", df.index.astype(str)))).tolist()
    stars  = df.get("stars", pd.Series([""] * n)).tolist()
    ax.set_yticks(y_pos)
    ax.set_yticklabels([f"{l}  {s}" for l, s in zip(labels, stars)], fontsize=8)
    ax.set_xlabel("DiD coefficient (β̂) with 95% CI")
    ax.set_title(title, fontsize=11, fontweight="bold")
    ax.grid(axis="x", alpha=0.25)
    ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {out_path.name}")
