"""
02_estimate_choice_functions.py
──────────────────────────────────────────────────────────────────────────────
Estimate the choice function for each of four subsamples:
  licitaciones × {pre, post}   and   compra_agil × {pre, post}.

Two flavours:
  1. Pooled LPM with buyer + year_month FE, cluster-robust SEs.
  2. Buyer-level same_region coefficients via an interaction model
     (one same_region × buyer dummy per buyer with >= N tenders; dropped if
     not identified). Extracted and saved as buyer_level_coefficients.parquet.

Also a plain pooled conditional logit (no FE) as a robustness specification.

Outputs (under {OUTPUT_ROOT}/choice_function/):
  estimates/
    pooled_results.csv
    pooled_logit_results.csv
    buyer_level_coefficients.parquet
"""

from __future__ import annotations

import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

# ── Paths ────────────────────────────────────────────────────────────────────
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1]))
from config import OUTPUT_ROOT  # noqa: E402

OUT_DIR      = OUTPUT_ROOT / "choice_function"
OUT_SAMPLES  = OUT_DIR / "samples"
OUT_EST      = OUT_DIR / "estimates"
OUT_EST.mkdir(parents=True, exist_ok=True)

MIN_BUYER_TENDERS = 20     # minimum awarded tenders for a buyer-level estimate


# ── Helpers ──────────────────────────────────────────────────────────────────
def load_sample(label: str) -> pd.DataFrame:
    path = OUT_SAMPLES / f"choice_sample_{label}.parquet"
    df = pd.read_parquet(path)
    # drop rows missing core regressors
    core = ["is_selected", "is_lowest_bid", "same_region", "log_bid_ratio", "rut_unidad", "tender_id"]
    return df.dropna(subset=core).copy()


def pooled_lpm_with_fe(df: pd.DataFrame, label: str) -> dict:
    """
    LPM: is_selected ~ is_lowest_bid + same_region + log_bid_ratio + sme
                    + same_region:is_lowest_bid
                    + C(rut_unidad) + C(year_month)
    Cluster-robust SEs at buyer level via pyfixest.
    """
    import pyfixest as pf
    # sme has NaNs — drop for the regression
    cols = ["is_selected", "is_lowest_bid", "same_region", "log_bid_ratio", "sme",
            "rut_unidad", "year_month"]
    sub = df[cols].dropna().copy()
    sub["sr_x_low"] = sub["same_region"] * sub["is_lowest_bid"]
    fml = ("is_selected ~ is_lowest_bid + same_region + log_bid_ratio "
           "+ sme + sr_x_low | rut_unidad + year_month")
    try:
        m = pf.feols(fml, data=sub, vcov={"CRV1": "rut_unidad"})
        coefs = m.coef()
        ses   = m.se()
        pvals = m.pvalue()
        out = {
            "label": label,
            "n_obs": int(len(sub)),
            "n_buyers": int(sub["rut_unidad"].nunique()),
            "n_tenders": int(df["tender_id"].nunique()),
        }
        for name in coefs.index:
            out[f"coef_{name}"] = float(coefs[name])
            out[f"se_{name}"]   = float(ses[name])
            out[f"p_{name}"]    = float(pvals[name])
        return out
    except Exception as exc:
        print(f"  [{label}] pooled LPM failed: {exc}")
        return {"label": label, "error": str(exc)}


def pooled_logit_no_fe(df: pd.DataFrame, label: str, max_rows: int = 500_000) -> dict:
    """
    Plain logit: is_selected ~ is_lowest_bid + same_region + log_bid_ratio + sme
                             + sr_x_low.
    No FE (too many buyer dummies) — robustness specification for coefficient
    signs. SEs clustered by tender_id. Downsamples by tender to keep it tractable.
    """
    import statsmodels.api as sm
    cols = ["is_selected", "is_lowest_bid", "same_region", "log_bid_ratio", "sme", "tender_id"]
    sub = df[cols].dropna().copy()
    if len(sub) > max_rows:
        # Sample whole tenders to keep the clustering structure intact
        tenders = sub["tender_id"].drop_duplicates()
        # frac chosen to target max_rows
        frac = min(1.0, max_rows / len(sub))
        keep = tenders.sample(frac=frac, random_state=42)
        sub = sub[sub["tender_id"].isin(keep)].copy()
    sub["sr_x_low"] = sub["same_region"] * sub["is_lowest_bid"]
    X = sub[["is_lowest_bid", "same_region", "log_bid_ratio", "sme", "sr_x_low"]].astype(float)
    X = sm.add_constant(X)
    y = sub["is_selected"].astype(int)
    try:
        model = sm.Logit(y, X)
        res = model.fit(disp=False, maxiter=200,
                        cov_type="cluster",
                        cov_kwds={"groups": sub["tender_id"].values})
        out = {
            "label": label,
            "n_obs": int(len(sub)),
            "pseudo_r2": float(res.prsquared),
        }
        for name in res.params.index:
            out[f"coef_{name}"] = float(res.params[name])
            out[f"se_{name}"]   = float(res.bse[name])
            out[f"p_{name}"]    = float(res.pvalues[name])
        return out
    except Exception as exc:
        print(f"  [{label}] pooled logit failed: {exc}")
        return {"label": label, "error": str(exc)}


def _cluster_se(X: np.ndarray, resid: np.ndarray, cluster_ids: np.ndarray) -> np.ndarray:
    """HC1 cluster-robust SE (sandwich). Cluster by tender_id."""
    n, k = X.shape
    if n == 0 or k == 0:
        return np.full(k, np.nan)
    codes, _ = pd.factorize(cluster_ids, sort=False)
    XtX = X.T @ X
    try:
        XtX_inv = np.linalg.inv(XtX)
    except np.linalg.LinAlgError:
        XtX_inv = np.linalg.pinv(XtX)
    G = int(codes.max()) + 1 if codes.size else 0
    if G == 0:
        return np.full(k, np.nan)
    scores = np.zeros((G, k))
    for j in range(k):
        np.add.at(scores[:, j], codes, X[:, j] * resid)
    B = scores.T @ scores
    hc1 = (G / max(G - 1, 1)) * (n / max(n - k, 1))
    V = hc1 * (XtX_inv @ B @ XtX_inv)
    return np.sqrt(np.clip(np.diag(V), 0.0, None))


def _fit_buyer_ols(sub: pd.DataFrame) -> dict | None:
    """Fast OLS for a single buyer. Returns coef/se on same_region, etc."""
    if sub["same_region"].nunique() < 2 or len(sub) < 40:
        return None
    X = np.column_stack([
        np.ones(len(sub)),
        sub["is_lowest_bid"].to_numpy(dtype=float),
        sub["same_region"].to_numpy(dtype=float),
        sub["log_bid_ratio"].to_numpy(dtype=float),
    ])
    y = sub["is_selected"].to_numpy(dtype=float)
    valid = np.isfinite(X).all(axis=1) & np.isfinite(y)
    if valid.sum() < 40:
        return None
    X, y = X[valid], y[valid]
    clu = sub["tender_id"].to_numpy()[valid]
    try:
        coefs, *_ = np.linalg.lstsq(X, y, rcond=None)
    except np.linalg.LinAlgError:
        return None
    resid = y - X @ coefs
    se = _cluster_se(X, resid, clu)
    names = ["const", "is_lowest_bid", "same_region", "log_bid_ratio"]
    out = {"n_obs": int(len(y))}
    for i, n in enumerate(names):
        out[f"coef_{n}"] = float(coefs[i])
        out[f"se_{n}"]   = float(se[i])
    return out


def buyer_level_coefs_via_interactions(df: pd.DataFrame, label: str,
                                       min_tenders: int = MIN_BUYER_TENDERS) -> pd.DataFrame:
    """
    Buyer-specific same_region coefficients via one OLS per qualifying buyer.

    Per buyer: is_selected ~ 1 + is_lowest_bid + same_region + log_bid_ratio
               with tender-level cluster-robust SEs.

    (Omits year_month FE for speed; shifts in selection propensity over time are
    a small concern relative to the cross-sectional identification coming from
    variation across bidders within the same tender.)
    """
    core = ["is_selected", "is_lowest_bid", "same_region", "log_bid_ratio",
            "rut_unidad", "tender_id"]
    sub = df[core].dropna().copy()

    tcounts = sub.groupby("rut_unidad")["tender_id"].nunique()
    big_buyers = tcounts[tcounts >= min_tenders].index.tolist()
    print(f"  [{label}] {len(big_buyers)} buyers qualify (>= {min_tenders} tenders).")

    from scipy.special import ndtr
    results = []
    # Preindex for faster per-buyer slicing
    sub_indexed = sub.set_index("rut_unidad").sort_index()
    for i, rut in enumerate(big_buyers):
        try:
            d = sub_indexed.loc[[rut]]
        except KeyError:
            continue
        fit = _fit_buyer_ols(d)
        if fit is None:
            continue
        fit["rut_unidad"]       = rut
        fit["n_tenders"]        = int(d["tender_id"].nunique())
        fit["share_local_bids"] = float(d["same_region"].mean())
        fit["share_selected"]   = float(d["is_selected"].mean())
        results.append(fit)
        if (i + 1) % 500 == 0:
            print(f"    [{label}] processed {i + 1}/{len(big_buyers)}")

    out = pd.DataFrame(results)
    if len(out):
        out["z_same_region"] = out["coef_same_region"] / out["se_same_region"]
        out["p_same_region"] = 2 * (1 - ndtr(np.abs(out["z_same_region"])))
    out["label"] = label
    return out


def main() -> None:
    pooled_rows = []
    logit_rows = []
    buyer_frames = []

    for label in [
        "licitaciones_pre", "licitaciones_post",
        "compra_agil_pre",  "compra_agil_post",
    ]:
        print(f"\n=== {label} ===")
        df = load_sample(label)
        print(f"  n_obs={len(df):,}  n_tenders={df['tender_id'].nunique():,}  "
              f"n_buyers={df['rut_unidad'].nunique():,}")
        if len(df) < 500:
            print(f"  [{label}] too few rows, skipping.")
            continue

        print("  Running pooled LPM with buyer + year-month FE ...")
        pooled_rows.append(pooled_lpm_with_fe(df, label))

        print("  Running pooled logit (no FE) ...")
        logit_rows.append(pooled_logit_no_fe(df, label))

        print(f"  Estimating buyer-level coefficients (min {MIN_BUYER_TENDERS} tenders) ...")
        bcoef = buyer_level_coefs_via_interactions(df, label, min_tenders=MIN_BUYER_TENDERS)
        print(f"    {len(bcoef)} buyer-level estimates.")
        buyer_frames.append(bcoef)

    if pooled_rows:
        pd.DataFrame(pooled_rows).to_csv(OUT_EST / "pooled_results.csv", index=False)
    if logit_rows:
        pd.DataFrame(logit_rows).to_csv(OUT_EST / "pooled_logit_results.csv", index=False)
    if buyer_frames:
        out = pd.concat(buyer_frames, ignore_index=True)
        out.to_parquet(OUT_EST / "buyer_level_coefficients.parquet", index=False)
        out.to_csv(OUT_EST / "buyer_level_coefficients.csv", index=False)
        print(f"\nBuyer-level coefficients: {len(out):,} rows saved.")
    print(f"\nAll estimates written to {OUT_EST}")


if __name__ == "__main__":
    main()
