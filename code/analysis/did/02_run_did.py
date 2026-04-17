"""
02_run_did.py
─────────────────────────────────────────────────────────────────────────────
Run all workhorse DiD regressions for the Compra Ágil reform.

Reads the pre-built tender-level and bid-level samples from
  output/did/samples/did_tender_sample.parquet
  output/did/samples/did_bid_sample.parquet

Outcomes estimated
  ── Entry & bidding ──────────────────────────────────────────────────────────
  n_bidders               Total number of bidders per tender
  n_local                 Local (same-region) bidders per tender
  n_nonlocal              Non-local bidders per tender
  share_local_bidders     Share of bidders that are local
  single_bidder           Indicator: only one bidder
  any_new_entrant         Indicator: at least one first-time bidder
  new_entrant_winner      Indicator: winning bidder is a first-time entrant

  ── Composition ──────────────────────────────────────────────────────────────
  any_sme_sii             Indicator: at least one SME bidder (SII definition)
  sme_share_sii           Share of bidders that are SME (SII)
  winner_is_sme_sii       Indicator: winner is SME (SII)
  share_bidders_not_in_sii  Share of bidders not linked to SII
  winner_not_in_sii       Indicator: winner not linked to SII
  share_sme_local_bidders Share of bidders that are SME and local
  share_sme_nonlocal_bidders Share of bidders that are SME and non-local
  any_sme_local_bidder    Indicator: at least one bidder is SME and local
  any_sme_tamano          (CA only) any SME bidder by platform tamano
  sme_share_tamano        (CA only) share SME bidders by tamano
  winner_is_sme_tamano    (CA only) winner is SME by tamano
  winner_is_local         Indicator: winner is in the same region as buyer

  ── Costs ─────────────────────────────────────────────────────────────────
  log_win_price_ratio     log(winning bid / reference price)
  bid_cv                  Coefficient of variation of submitted bids

  ── Process ──────────────────────────────────────────────────────────────────
  single_bidder           (also under entry)
  is_desierto             Indicator: tender declared desert
  days_to_award           Days from publication to award (licitaciones)

  ── Bid-level ────────────────────────────────────────────────────────────────
  log_sub_price_ratio     log(submitted bid / reference price) — all firms

For each outcome group the script produces:
  · A pooled DiD coefficient table (CSV)
  · An event-study coefficient table (CSV)
  · A coefficient summary plot (PNG)
  · An event-study plot for key outcomes (PNG)

Outputs
  output/did/tables/did_results_{group}.csv
  output/did/tables/event_study_{outcome}.csv
  output/did/figures/did_coef_{group}.png
  output/did/figures/event_study_{outcome}.png
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import matplotlib
import numpy as np
import pandas as pd

matplotlib.use("Agg")

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from did_utils import (
    OUT_SAMPLES,
    OUT_TABLES,
    OUT_FIGURES,
    REFORM_PERIOD,
    run_twfe_did,
    run_twfe_iv,
    run_twfe_event_study,
    results_to_df,
    plot_event_study,
    plot_did_coef_summary,
)

# ── Config ────────────────────────────────────────────────────────────────────
ENTITY_COL  = "rut_unidad"
TIME_COL    = "year_month"
DID_COL     = "did"
TREAT_COL   = "treated"
CLUSTER_COL = "rut_unidad"
MIN_NONDEGENERATE_SHARE = 0.50

# Event study: months relative to reform
PRE_PERIODS  = 8
POST_PERIODS = 6

# Outcomes for which we run an event study
EVENT_STUDY_OUTCOMES = [
    "n_bidders",
    "sme_share_sii",
    "winner_is_sme_sii",
    "winner_is_large",
    "log_win_price_ratio",
    "log_sub_price_ratio",
    "single_bidder",
]

# ── Outcome catalogue ─────────────────────────────────────────────────────────
# (outcome_col, human label, dataset filter or None for both)
OUTCOMES_TENDER = [
    # Panel A — Entry at Bidding Stage
    ("n_bidders",            "N bidders",                None),
    ("n_local",              "N local bidders",          None),
    ("n_nonlocal",           "N non-local bidders",      None),
    ("n_sme_bidders",        "N SME bidders",            None),
    ("n_large_bidders",      "N large firm bidders",     None),
    ("n_nonsii_bidders",     "N non-SII bidders",        None),
    ("single_bidder",        "Pr(single-bidder tender)", None),
    # Panel B — Bidder Composition
    ("any_sme_sii",          "Any SME bidder",           None),
    ("sme_share_sii",        "% bidders: SME",           None),
    ("share_large_bidders",  "% bidders: large firm",    None),
    ("share_bidders_not_in_sii", "% bidders: not in SII", None),
    ("share_sme_local_bidders", "% bidders: SME×local",  None),
    ("share_large_local_bidders", "% bidders: large×local", None),
    ("share_nonsii_local_bidders", "% bidders: non-SII×local", None),
    # Panel C — Winner Characteristics
    ("winner_is_sme_sii",    "Pr(winner: SME)",          None),
    ("winner_is_large",      "Pr(winner: large firm)",   None),
    ("winner_not_in_sii",    "Pr(winner: non-SII)",      None),
    ("winner_is_sme_local",  "Pr(winner: SME & local)",  None),
    ("winner_is_large_local", "Pr(winner: large & local)", None),
    ("winner_is_nonsii_local", "Pr(winner: non-SII & local)", None),
    # Panel D — Bid Outcomes
    ("log_win_price_ratio",  "log(win bid/ref)",         None),
    ("log_min_price_ratio",  "log(min bid/ref)",         None),
    ("bid_cv",               "Bid CV",                   None),
]

OUTCOMES_BID = [
    ("log_sub_price_ratio",  "log(bid/ref)",             None),
]


# ── Pre-run non-degeneracy checks ────────────────────────────────────────────
def _series_check_stats(s: pd.Series) -> tuple[float, int]:
    """Return (non-missing share, number of unique non-missing values)."""
    nonnull_share = float(s.notna().mean()) if len(s) else 0.0
    nunique_nonnull = int(s.dropna().nunique())
    return nonnull_share, nunique_nonnull


def _check_design_non_degenerate(
    sub: pd.DataFrame,
    *,
    outcome_col: str,
    label: str,
    group: str,
    ds_filter: str,
) -> tuple[bool, list[dict]]:
    """
    Validate outcome + core regressors before running DiD.
    Pass criteria for each variable:
      1) non-missing share >= MIN_NONDEGENERATE_SHARE
      2) at least 2 unique non-missing values
    """
    vars_to_check = [outcome_col, DID_COL, TREAT_COL, ENTITY_COL, TIME_COL]
    rows = []
    ok = True
    for var in vars_to_check:
        if var not in sub.columns:
            rows.append({
                "group": group,
                "label": label,
                "outcome": outcome_col,
                "dataset_filter": ds_filter,
                "variable": var,
                "n_obs": int(len(sub)),
                "nonmissing_share": 0.0,
                "nunique_nonmissing": 0,
                "passes": False,
                "fail_reason": "column_absent",
            })
            ok = False
            continue

        nonnull_share, nunique_nonnull = _series_check_stats(sub[var])
        passes = (nonnull_share >= MIN_NONDEGENERATE_SHARE) and (nunique_nonnull >= 2)
        rows.append({
            "group": group,
            "label": label,
            "outcome": outcome_col,
            "dataset_filter": ds_filter,
            "variable": var,
            "n_obs": int(len(sub)),
            "nonmissing_share": round(nonnull_share, 6),
            "nunique_nonmissing": nunique_nonnull,
            "passes": bool(passes),
            "fail_reason": "" if passes else "low_coverage_or_no_variation",
        })
        ok = ok and bool(passes)
    return ok, rows


# ── Loaders ───────────────────────────────────────────────────────────────────
def load_tender() -> pd.DataFrame:
    path = OUT_SAMPLES / "did_tender_sample.parquet"
    print(f"  Loading {path.name} …")
    df = pd.read_parquet(path)
    df["year_month"] = df["year_month"].apply(
        lambda x: pd.Period(x, freq="M") if not isinstance(x, pd.Period) else x
    )
    df["fecha_pub"] = pd.to_datetime(df["fecha_pub"], errors="coerce")
    # Coerce numeric outcome columns that might have Int8/Int64 nullable types
    for col in df.select_dtypes(include="Int64").columns:
        df[col] = df[col].astype("float64")
    for col in df.select_dtypes(include="Int8").columns:
        df[col] = df[col].astype("float64")
    print(f"    {len(df):,} rows, {df['band'].value_counts().to_dict()}")
    return df


def load_bid() -> pd.DataFrame:
    path = OUT_SAMPLES / "did_bid_sample.parquet"
    print(f"  Loading {path.name} …")
    df = pd.read_parquet(path)
    df["year_month"] = df["year_month"].apply(
        lambda x: pd.Period(x, freq="M") if not isinstance(x, pd.Period) else x
    )
    df["fecha_pub"] = pd.to_datetime(df["fecha_pub"], errors="coerce")
    for col in df.select_dtypes(include=["Int64", "Int8"]).columns:
        df[col] = df[col].astype("float64")
    print(f"    {len(df):,} rows")
    return df


# ── Augmentation: compute new tender-level variables from bid sample ──────────
def _augment_tender_from_bid(
    df_tender: pd.DataFrame,
    df_bid: pd.DataFrame,
) -> pd.DataFrame:
    """
    Compute new tender-level aggregates from df_bid and merge them into
    df_tender.  Used to add variables that are not in the stored parquet
    without re-running the slow full sample build.

    New columns computed (if not already present in df_tender):
      n_sme_bidders, n_large_bidders, n_nonsii_bidders
      share_large_bidders, share_large_local_bidders, share_nonsii_local_bidders
      winner_is_large, winner_is_sme_local, winner_is_large_local,
      winner_is_nonsii_local, log_min_price_ratio
    """
    print("  Augmenting tender sample from bid sample …")
    # Work directly on df_bid with local Series — avoids a ~2 GB full copy.

    # ── SME / large / non-SII flags ──────────────────────────────────────────
    sme = pd.to_numeric(df_bid["sme_sii"], errors="coerce")
    sme_flag    = (sme == 1).astype("float64")
    large_flag  = (sme == 0).astype("float64")
    nonsii_flag = sme.isna().astype("float64")

    # ── Locality ─────────────────────────────────────────────────────────────
    loc_raw = df_bid.get("same_region", pd.Series(np.nan, index=df_bid.index))
    loc = pd.to_numeric(loc_raw, errors="coerce")

    large_local_flag = pd.Series(
        np.where((sme == 0) & (loc == 1), 1.0,
                 np.where(sme.notna() & loc.notna(), 0.0, np.nan)),
        index=df_bid.index,
    )
    nonsii_local_flag = pd.Series(
        np.where(sme.isna() & (loc == 1), 1.0,
                 np.where(loc.notna(), 0.0, np.nan)),
        index=df_bid.index,
    )

    tid = df_bid["tender_id"]

    # ── Counts ────────────────────────────────────────────────────────────────
    n_sme    = sme_flag.groupby(tid, sort=False).sum().rename("n_sme_bidders")
    n_large  = large_flag.groupby(tid, sort=False).sum().rename("n_large_bidders")
    n_nonsii = nonsii_flag.groupby(tid, sort=False).sum().rename("n_nonsii_bidders")
    n_tot    = df_bid.groupby(tid, sort=False)["bidder_id"].nunique()

    # ── Shares ────────────────────────────────────────────────────────────────
    share_large        = (large_flag.groupby(tid, sort=False).sum() / n_tot).rename("share_large_bidders")
    share_large_local  = large_local_flag.groupby(tid, sort=False).mean().rename("share_large_local_bidders")
    share_nonsii_local = nonsii_local_flag.groupby(tid, sort=False).mean().rename("share_nonsii_local_bidders")

    # ── Minimum bid / reference price ────────────────────────────────────────
    sub_pos = df_bid["submitted_bid"].where(df_bid["submitted_bid"] > 0)
    min_bid = sub_pos.groupby(tid, sort=False).min()
    ref_bid = (
        df_bid[["tender_id", "monto_estimado"]]
        .drop_duplicates("tender_id")
        .set_index("tender_id")["monto_estimado"]
    )
    valid_min = (
        min_bid.notna() & (min_bid > 0)
        & ref_bid.reindex(min_bid.index).notna()
        & (ref_bid.reindex(min_bid.index) > 0)
    )
    log_min = pd.Series(
        np.where(valid_min, np.log(min_bid / ref_bid.reindex(min_bid.index)), np.nan),
        index=min_bid.index,
        name="log_min_price_ratio",
    )

    # ── Winner flags ─────────────────────────────────────────────────────────
    sel = (df_bid["is_selected"].astype("float64")
           if "is_selected" in df_bid.columns
           else pd.Series(0.0, index=df_bid.index))

    def _winner_flag(flag_s: pd.Series, name: str) -> pd.Series:
        w   = flag_s.where(sel == 1)
        n_w = w.groupby(tid, sort=False).count()
        return pd.Series(
            np.where(n_w > 0, w.groupby(tid, sort=False).max(), np.nan),
            index=n_w.index,
            name=name,
        )

    large_flag_s = pd.Series(
        np.where(sme.notna(), (sme == 0).astype("float64"), np.nan),
        index=df_bid.index,
    )
    sme_local_flag_s = pd.Series(
        np.where((sme == 1) & (loc == 1), 1.0,
                 np.where(sme.notna() & loc.notna(), 0.0, np.nan)),
        index=df_bid.index,
    )
    large_local_s  = large_local_flag
    nonsii_local_s = nonsii_local_flag

    w_is_large        = _winner_flag(large_flag_s,   "winner_is_large")
    w_is_sme_local    = _winner_flag(sme_local_flag_s, "winner_is_sme_local")
    w_is_large_local  = _winner_flag(large_local_s,  "winner_is_large_local")
    w_is_nonsii_local = _winner_flag(nonsii_local_s, "winner_is_nonsii_local")

    # ── Merge ────────────────────────────────────────────────────────────────
    aug = pd.concat(
        [
            n_sme, n_large, n_nonsii,
            share_large, share_large_local, share_nonsii_local,
            log_min,
            w_is_large, w_is_sme_local, w_is_large_local, w_is_nonsii_local,
        ],
        axis=1,
    ).reset_index()

    new_cols = [c for c in aug.columns if c != "tender_id" and c not in df_tender.columns]
    if not new_cols:
        print("    All augmented columns already present — no merge needed.")
        return df_tender

    df_tender = df_tender.merge(aug[["tender_id"] + new_cols], on="tender_id", how="left")
    print(f"    Added {len(new_cols)} columns: {new_cols}")
    return df_tender


# ── Run all pooled DiD ────────────────────────────────────────────────────────
def run_all_pooled(
    df_tender: pd.DataFrame,
    df_bid:    pd.DataFrame,
    band_include: list[str] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Run pooled TWFE DiD for every outcome. Return (results_df, checks_df).

    band_include : if given, restrict both samples to rows where
                   band is in the list before running any regression.
                   E.g. ["control_low", "treated"] or ["control_high", "treated"].
    """
    all_results = []
    checks = []

    if band_include is not None:
        df_tender = df_tender[df_tender["band"].isin(band_include)].copy()
        df_bid    = df_bid[df_bid["band"].isin(band_include)].copy()

    print("\n  ── Tender-level outcomes ──────────────────────────────────")
    for outcome_col, label, ds_filter in OUTCOMES_TENDER:
        if outcome_col not in df_tender.columns:
            print(f"  [{label}] column absent — skip.")
            continue
        sub = df_tender if ds_filter is None else df_tender[df_tender["dataset"] == ds_filter]
        check_ok, check_rows = _check_design_non_degenerate(
            sub,
            outcome_col=outcome_col,
            label=label,
            group="tender",
            ds_filter=ds_filter or "both",
        )
        checks.extend(check_rows)
        if not check_ok:
            print(
                f"  [{label}] non-degeneracy check failed "
                f"(requires >= {int(100*MIN_NONDEGENERATE_SHARE)}% non-missing and variation) — skip."
            )
            continue
        if sub[outcome_col].notna().sum() < 100:
            print(f"  [{label}] fewer than 100 non-null obs — skip.")
            continue
        r = run_twfe_did(
            sub,
            outcome_col=outcome_col,
            entity_col=ENTITY_COL,
            time_col=TIME_COL,
            did_col=DID_COL,
            treat_col=TREAT_COL,
            cluster_col=CLUSTER_COL,
            label=label,
        )
        if r:
            r["group"] = "tender"
            r["ds_filter"] = ds_filter or "both"
            all_results.append(r)
            sig = r.get("stars", "")
            print(
                f"  {label:<35} β={r['coef_did']:+.4f}  SE={r['se_did']:.4f}"
                f"  t={r['tstat_did']:+.2f}  {sig}  n={r['n_obs']:,}"
            )

    print("\n  ── Bid-level outcomes ─────────────────────────────────────")
    for outcome_col, label, ds_filter in OUTCOMES_BID:
        if outcome_col not in df_bid.columns:
            print(f"  [{label}] column absent — skip.")
            continue
        sub = df_bid if ds_filter is None else df_bid[df_bid["dataset"] == ds_filter]
        check_ok, check_rows = _check_design_non_degenerate(
            sub,
            outcome_col=outcome_col,
            label=label,
            group="bid",
            ds_filter=ds_filter or "both",
        )
        checks.extend(check_rows)
        if not check_ok:
            print(
                f"  [{label}] non-degeneracy check failed "
                f"(requires >= {int(100*MIN_NONDEGENERATE_SHARE)}% non-missing and variation) — skip."
            )
            continue
        if sub[outcome_col].notna().sum() < 100:
            print(f"  [{label}] fewer than 100 non-null obs — skip.")
            continue
        r = run_twfe_did(
            sub,
            outcome_col=outcome_col,
            entity_col=ENTITY_COL,
            time_col=TIME_COL,
            did_col=DID_COL,
            treat_col=TREAT_COL,
            cluster_col=CLUSTER_COL,
            label=label,
        )
        if r:
            r["group"] = "bid"
            r["ds_filter"] = ds_filter or "both"
            all_results.append(r)
            sig = r.get("stars", "")
            print(
                f"  {label:<35} β={r['coef_did']:+.4f}  SE={r['se_did']:.4f}"
                f"  t={r['tstat_did']:+.2f}  {sig}  n={r['n_obs']:,}"
            )

    results_df = results_to_df(all_results)
    checks_df = pd.DataFrame(checks)
    return results_df, checks_df


# ── Run IV-DiD (2SLS) ─────────────────────────────────────────────────────────
def run_all_iv(
    df_tender: pd.DataFrame,
    df_bid:    pd.DataFrame,
    band_include: list[str] | None = None,
) -> pd.DataFrame:
    """
    Run IV-DiD (2SLS) for every outcome.

    Endogenous: ca_post  (= 1 if tender processed via Compra Ágil)
    Instrument:  did     (= treated × post, the reform eligibility indicator)

    Returns results_df (same format as run_all_pooled).
    """
    all_results = []

    if band_include is not None:
        df_tender = df_tender[df_tender["band"].isin(band_include)].copy()
        df_bid    = df_bid[df_bid["band"].isin(band_include)].copy()

    if "ca_post" not in df_tender.columns:
        print("  [WARN] ca_post not found in tender sample — skipping IV.")
        return pd.DataFrame()

    print("\n  ── IV Tender-level outcomes ───────────────────────────────")
    for outcome_col, label, ds_filter in OUTCOMES_TENDER:
        if outcome_col not in df_tender.columns:
            print(f"  [{label}] column absent — skip.")
            continue
        sub = df_tender if ds_filter is None else df_tender[df_tender["dataset"] == ds_filter]
        if sub[outcome_col].notna().sum() < 100:
            print(f"  [{label}] fewer than 100 non-null obs — skip.")
            continue
        if sub["ca_post"].nunique() < 2:
            print(f"  [{label}] ca_post has no variation — skip IV.")
            continue
        r = run_twfe_iv(
            sub,
            outcome_col=outcome_col,
            endog_col="ca_post",
            instr_col=DID_COL,
            entity_col=ENTITY_COL,
            time_col=TIME_COL,
            treat_col=TREAT_COL,
            cluster_col=CLUSTER_COL,
            label=label,
        )
        if r:
            r["group"] = "tender"
            r["ds_filter"] = ds_filter or "both"
            all_results.append(r)
            sig = r.get("stars", "")
            print(
                f"  {label:<35} β_IV={r['coef_did']:+.4f}  SE={r['se_did']:.4f}"
                f"  F1st={r.get('first_stage_f', float('nan')):.1f}  n={r['n_obs']:,}"
            )

    print("\n  ── IV Bid-level outcomes ──────────────────────────────────")
    for outcome_col, label, ds_filter in OUTCOMES_BID:
        if outcome_col not in df_bid.columns or "ca_post" not in df_bid.columns:
            print(f"  [{label}] column absent — skip.")
            continue
        sub = df_bid if ds_filter is None else df_bid[df_bid["dataset"] == ds_filter]
        if sub[outcome_col].notna().sum() < 100:
            print(f"  [{label}] fewer than 100 non-null obs — skip.")
            continue
        if sub["ca_post"].nunique() < 2:
            print(f"  [{label}] ca_post has no variation — skip IV.")
            continue
        r = run_twfe_iv(
            sub,
            outcome_col=outcome_col,
            endog_col="ca_post",
            instr_col=DID_COL,
            entity_col=ENTITY_COL,
            time_col=TIME_COL,
            treat_col=TREAT_COL,
            cluster_col=CLUSTER_COL,
            label=label,
        )
        if r:
            r["group"] = "bid"
            r["ds_filter"] = ds_filter or "both"
            all_results.append(r)
            print(
                f"  {label:<35} β_IV={r['coef_did']:+.4f}  SE={r['se_did']:.4f}"
                f"  F1st={r.get('first_stage_f', float('nan')):.1f}  n={r['n_obs']:,}"
            )

    results_df = results_to_df(all_results)
    return results_df


def save_nondegeneracy_checks(checks_df: pd.DataFrame) -> None:
    if checks_df.empty:
        print("  [WARN] No non-degeneracy diagnostics to save.")
        return
    out = OUT_TABLES / "did_nondegeneracy_checks.csv"
    checks_df.to_csv(out, index=False)
    n_fail = int((~checks_df["passes"]).sum()) if "passes" in checks_df.columns else 0
    print(f"\n  Saved: {out.name} ({len(checks_df):,} rows; fails={n_fail:,})")


# ── Save pooled results ───────────────────────────────────────────────────────
def save_pooled(results_df: pd.DataFrame, suffix: str = "all") -> None:
    if results_df.empty:
        print("  [WARN] No pooled results to save.")
        return
    out = OUT_TABLES / f"did_results_{suffix}.csv"
    results_df.to_csv(out, index=False)
    print(f"\n  Saved: {out.name}")


# ── Coefficient summary plots ─────────────────────────────────────────────────
def save_coef_plots(results_df: pd.DataFrame) -> None:
    if results_df.empty:
        return

    outcome_groups = {
        "entry_bidding": [
            "n_bidders", "n_local", "n_nonlocal",
            "n_sme_bidders", "n_large_bidders", "n_nonsii_bidders",
            "single_bidder",
        ],
        "bidder_composition": [
            "any_sme_sii", "sme_share_sii", "share_large_bidders",
            "share_bidders_not_in_sii",
            "share_sme_local_bidders", "share_large_local_bidders",
            "share_nonsii_local_bidders",
        ],
        "winner_characteristics": [
            "winner_is_sme_sii", "winner_is_large", "winner_not_in_sii",
            "winner_is_sme_local", "winner_is_large_local", "winner_is_nonsii_local",
        ],
        "bid_outcomes": [
            "log_win_price_ratio", "log_min_price_ratio",
            "log_sub_price_ratio", "bid_cv",
        ],
    }
    group_colors = {
        "entry_bidding"        : "#1B9E77",
        "bidder_composition"   : "#D95F02",
        "winner_characteristics": "#E7298A",
        "bid_outcomes"         : "#7570B3",
    }
    group_labels = {
        "entry_bidding"        : "Entry at Bidding Stage",
        "bidder_composition"   : "Bidder Composition",
        "winner_characteristics": "Winner Characteristics",
        "bid_outcomes"         : "Bid Outcomes",
    }

    for grp, outcomes in outcome_groups.items():
        sub = results_df[results_df["outcome"].isin(outcomes)].copy()
        if sub.empty:
            continue
        # Map outcome → human label via OUTCOMES_TENDER + OUTCOMES_BID catalogue
        label_map = {o: l for o, l, _ in OUTCOMES_TENDER + OUTCOMES_BID}
        sub["label"] = sub["outcome"].map(label_map).fillna(sub["outcome"])
        plot_did_coef_summary(
            sub,
            title=f"DiD Coefficients — {group_labels[grp]}\n"
                  f"TWFE, entity + year-month FE, SE clustered by PE",
            out_path=OUT_FIGURES / f"did_coef_{grp}.png",
            color=group_colors[grp],
        )


# ── Event studies ─────────────────────────────────────────────────────────────
def run_event_studies(
    df_tender: pd.DataFrame,
    df_bid:    pd.DataFrame,
    file_suffix: str = "",
    drop_k0:     bool = False,
) -> None:
    """Run event-study TWFE for key outcomes and save results + plots."""
    print("\n  ── Event studies ──────────────────────────────────────────")

    # Map outcome → (DataFrame, ds_filter, human label, color)
    label_map  = {o: l for o, l, _ in OUTCOMES_TENDER + OUTCOMES_BID}
    ds_map     = {o: (ds or "both") for o, _, ds in OUTCOMES_TENDER + OUTCOMES_BID}
    color_map  = {
        "n_bidders"           : "#1B9E77",
        "sme_share_sii"       : "#D95F02",
        "winner_is_sme_sii"   : "#D95F02",
        "log_win_price_ratio" : "#7570B3",
        "log_sub_price_ratio" : "#7570B3",
        "winner_is_local"     : "#E7298A",
        "single_bidder"       : "#66A61E",
    }

    all_es = []
    for outcome_col in EVENT_STUDY_OUTCOMES:
        label = label_map.get(outcome_col, outcome_col)
        ds_f  = ds_map.get(outcome_col, "both")

        # Choose source DataFrame
        if outcome_col in [o for o, _, _ in OUTCOMES_BID]:
            src = df_bid
        else:
            src = df_tender

        if outcome_col not in src.columns:
            print(f"  [{label}] absent — skip event study.")
            continue
        sub = src if ds_f == "both" else src[src["dataset"] == ds_f]
        if sub[outcome_col].notna().sum() < 100:
            print(f"  [{label}] too few obs — skip event study.")
            continue

        es = run_twfe_event_study(
            sub,
            outcome_col=outcome_col,
            entity_col=ENTITY_COL,
            time_col=TIME_COL,
            treat_col=TREAT_COL,
            cluster_col=CLUSTER_COL,
            pre_periods=PRE_PERIODS,
            post_periods=POST_PERIODS,
            drop_k0=drop_k0,
            label=label,
        )
        if es.empty:
            continue

        # Save CSV
        csv_name = f"event_study_{outcome_col}{file_suffix}.csv"
        es.to_csv(OUT_TABLES / csv_name, index=False)
        print(f"  Saved: {csv_name}")
        all_es.append(es)

        # Plot
        k0_note = " — Dec 2024 (k=0) excluded" if drop_k0 else ""
        plot_event_study(
            es,
            title=f"Event study: {label}\nTWFE, entity + year-month FE, SE clustered by PE{k0_note}",
            out_path=OUT_FIGURES / f"event_study_{outcome_col}{file_suffix}.png",
            color=color_map.get(outcome_col, "#1B9E77"),
            y_label=f"β_k (vs Nov 2024)",
        )

    if all_es:
        combined_es = pd.concat(all_es, ignore_index=True)
        out_name = f"event_study_all{file_suffix}.csv"
        combined_es.to_csv(OUT_TABLES / out_name, index=False)
        print(f"  Saved: {out_name}")


# ── Summary table ─────────────────────────────────────────────────────────────
def print_summary_table(results_df: pd.DataFrame) -> None:
    """Pretty-print a compact summary of all DiD coefficients."""
    if results_df.empty:
        return
    label_map = {o: l for o, l, _ in OUTCOMES_TENDER + OUTCOMES_BID}
    print("\n" + "=" * 70)
    print("SUMMARY: DiD coefficients (β̂ with SE and significance)")
    print("=" * 70)
    fmt = "{:<37} {:>9} {:>9} {:>7} {:>5}"
    print(fmt.format("Outcome", "β̂", "SE", "t", "sig"))
    print("-" * 70)
    for _, row in results_df.iterrows():
        lbl = label_map.get(row.get("outcome", ""), row.get("label", ""))
        print(fmt.format(
            lbl[:37],
            f"{row['coef_did']:+.4f}",
            f"({row['se_did']:.4f})",
            f"{row['tstat_did']:+.2f}",
            row.get("stars", ""),
        ))
    print("=" * 70)
    print("  *** p<0.01  ** p<0.05  * p<0.10  |  SE clustered by procuring entity")


# ── Main ──────────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(description="Run pooled DiD and optional event studies.")
    parser.add_argument(
        "--did-only",
        action="store_true",
        help="Run only pooled DiD outputs (skip event-study estimation/plots).",
    )
    parser.add_argument(
        "--drop-k0",
        action="store_true",
        help="Option B: exclude k=0 (Dec 2024) as a partial-treatment transition month. "
             "Post period becomes k>=1. Output files get '_optB' suffix.",
    )
    parser.add_argument(
        "--sample",
        choices=["all", "municipalidades", "obras"],
        default="all",
        help="Sample restriction on buyer sector. 'municipalidades' keeps sector "
             "containing 'municipal'; 'obras' keeps sector containing 'obras'. "
             "Output files get '_munic'/'_obras' suffix.",
    )
    args = parser.parse_args()

    print("=" * 70)
    print("02_run_did.py — Compra Ágil Workhorse DiD")
    print("=" * 70)

    df_tender = load_tender()
    df_bid    = load_bid()
    df_tender = _augment_tender_from_bid(df_tender, df_bid)

    # ── ca_post: indicator of Compra Ágil mechanism (endogenous in IV) ─────
    df_tender["ca_post"] = (df_tender["dataset"] == "compra_agil").astype("float64")
    df_bid["ca_post"]    = (df_bid["dataset"]    == "compra_agil").astype("float64")

    # ── Sample restriction by buyer sector ─────────────────────────────────
    _SAMPLE_SUFFIX = {"all": "", "municipalidades": "_munic", "obras": "_obras"}
    ssuffix = _SAMPLE_SUFFIX[args.sample]
    if args.sample != "all":
        if "sector" not in df_tender.columns or "sector" not in df_bid.columns:
            print("  [ERROR] 'sector' column not found in parquet. "
                  "Re-run 01_build_did_sample.py first.")
            return
        kw = "municipal" if args.sample == "municipalidades" else "obras"
        df_tender = df_tender[
            df_tender["sector"].astype(str).str.lower().str.contains(kw, na=False)
        ].copy()
        df_bid = df_bid[
            df_bid["sector"].astype(str).str.lower().str.contains(kw, na=False)
        ].copy()
        print(f"\n  [Sample: {args.sample}] "
              f"Tender: {len(df_tender):,} rows, Bid: {len(df_bid):,} rows")

    # ── Option B: drop k=0 (Dec 2024) as a partial-treatment month ─────────
    optb_suffix = ""
    if args.drop_k0:
        optb_suffix = "_optB"
        _k_t = df_tender["year_month"].apply(
            lambda p: p.ordinal if hasattr(p, "ordinal") else int(p)
        ) - REFORM_PERIOD.ordinal
        _k_b = df_bid["year_month"].apply(
            lambda p: p.ordinal if hasattr(p, "ordinal") else int(p)
        ) - REFORM_PERIOD.ordinal
        df_tender = df_tender[_k_t != 0].copy()
        df_bid    = df_bid[_k_b != 0].copy()
        print(f"\n  [Option B] Dropped k=0 (Dec 2024): "
              f"tender {len(df_tender):,} rows, bid {len(df_bid):,} rows")

    fsuffix = ssuffix + optb_suffix

    # ── Spec 1: both control groups (original) ─────────────────────────────
    print("\n" + "=" * 70)
    print("SPEC: both control groups (control_low + control_high)")
    print("=" * 70)
    results_all, checks_all = run_all_pooled(df_tender, df_bid)
    save_nondegeneracy_checks(checks_all)
    save_pooled(results_all, suffix="all" + fsuffix)
    save_coef_plots(results_all)
    print_summary_table(results_all)

    # ── Spec 2: control = 0–30 UTM ─────────────────────────────────────────
    print("\n" + "=" * 70)
    print("SPEC: control = 0–30 UTM (control_low + treated)")
    print("=" * 70)
    results_cl, checks_cl = run_all_pooled(
        df_tender, df_bid, band_include=["control_low", "treated"]
    )
    save_pooled(results_cl, suffix="cl" + fsuffix)
    print_summary_table(results_cl)

    # ── Spec 3: control = 100–200 UTM ──────────────────────────────────────
    print("\n" + "=" * 70)
    print("SPEC: control = 100–200 UTM (control_high + treated)")
    print("=" * 70)
    results_ch, checks_ch = run_all_pooled(
        df_tender, df_bid, band_include=["control_high", "treated"]
    )
    save_pooled(results_ch, suffix="ch" + fsuffix)
    print_summary_table(results_ch)

    # ── IV Spec: control = 100–200 UTM ─────────────────────────────────────
    print("\n" + "=" * 70)
    print("IV SPEC: control = 100–200 UTM (control_high + treated)")
    print("=" * 70)
    results_iv_ch = run_all_iv(
        df_tender, df_bid, band_include=["control_high", "treated"]
    )
    if not results_iv_ch.empty:
        save_pooled(results_iv_ch, suffix="iv_ch" + fsuffix)
        print_summary_table(results_iv_ch)

    # ── Event studies (full sample) ────────────────────────────────────────
    if args.did_only:
        print("\nSkipping event studies (--did-only).")
    else:
        run_event_studies(df_tender, df_bid, file_suffix=fsuffix, drop_k0=args.drop_k0)

    print("\nDone.")


if __name__ == "__main__":
    main()
