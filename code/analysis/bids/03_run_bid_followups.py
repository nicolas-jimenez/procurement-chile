"""
03_run_bid_followups.py
──────────────────────────────────────────────────────────────────────────────
Follow-up within-firm bid-markup results for deck slides.

Outputs
  output/bids/tables/bids_part2_size_split.csv
  output/bids/tables/bids_part2_region_split.csv
  output/bids/figures/bids_part2_size_split_local.png
  output/bids/figures/bids_part2_region_split.png
"""

from __future__ import annotations

import argparse
import sys
import warnings
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from linearmodels.iv import AbsorbingLS

warnings.filterwarnings("ignore", category=FutureWarning)

SAMPLE_SUFFIX = {"all": "", "municipalidades": "_munic", "obras": "_obras"}
SAMPLE_KEYWORD = {"municipalidades": "municipal", "obras": "obras"}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run follow-up bid-markup results.")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--size-only", action="store_true", help="Run only the size split.")
    group.add_argument("--region-only", action="store_true", help="Run only the region split.")
    parser.add_argument(
        "--sample",
        choices=["all", "municipalidades", "obras"],
        default="all",
        help="Sample restriction on buyer sector.",
    )
    return parser.parse_args()


ARGS = _parse_args()
RUN_SIZE = not ARGS.region_only
RUN_REGION = not ARGS.size_only
SAMPLE = ARGS.sample
SAMPLE_SUFFIX_STR = SAMPLE_SUFFIX[SAMPLE]

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]

OUT_BIDS = ROOT / "output" / "bids"
OUT_BIDS_TBL = OUT_BIDS / "tables"
OUT_BIDS_FIG = OUT_BIDS / "figures"

CI_Z = 1.96
MIN_BIDS_FIRM = 3

REGION_ORDER = [
    "Arica y Parinacota",
    "Tarapacá",
    "Antofagasta",
    "Atacama",
    "Coquimbo",
    "Valparaíso",
    "Metropolitana de Santiago",
    "Libertador General Bernardo O'Higgins",
    "Maule",
    "Ñuble",
    "Biobío",
    "La Araucanía",
    "Los Ríos",
    "Los Lagos",
    "Aysén del General Carlos Ibáñez del Campo",
    "Magallanes y de la Antártica Chilena",
]

REGION_SHORT = {
    "Arica y Parinacota": "Arica",
    "Tarapacá": "Tarapacá",
    "Antofagasta": "Antofagasta",
    "Atacama": "Atacama",
    "Coquimbo": "Coquimbo",
    "Valparaíso": "Valparaíso",
    "Metropolitana de Santiago": "RM",
    "Libertador General Bernardo O'Higgins": "O'Higgins",
    "Maule": "Maule",
    "Ñuble": "Ñuble",
    "Biobío": "Biobío",
    "La Araucanía": "Araucanía",
    "Los Ríos": "Los Ríos",
    "Los Lagos": "Los Lagos",
    "Aysén del General Carlos Ibáñez del Campo": "Aysén",
    "Magallanes y de la Antártica Chilena": "Magallanes",
}


def _as_category(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for col in cols:
        df[col] = df[col].astype("category")
    return df


def _filter_sector_sample(df: pd.DataFrame, sample: str) -> pd.DataFrame:
    if sample == "all":
        return df
    if "sector" not in df.columns:
        raise RuntimeError(
            "'sector' column not found in bid analysis sample. "
            "Re-run 01_build_bid_sample.py first."
        )
    kw = SAMPLE_KEYWORD[sample]
    return df[df["sector"].astype(str).str.lower().str.contains(kw, na=False)].copy()


def _fit_hdfe(
    data: pd.DataFrame,
    outcome: str,
    regressors: list[str],
    absorb: list[str],
    cluster: str,
    spec: str,
    fe_label: str,
    cluster_label: str,
    **meta: str,
) -> dict:
    cols = list(dict.fromkeys([outcome, *regressors, *absorb, cluster]))
    work = data[cols].copy()
    work = work.dropna(subset=absorb + [cluster])
    work[outcome] = pd.to_numeric(work[outcome], errors="coerce")
    for col in regressors:
        work[col] = pd.to_numeric(work[col], errors="coerce")

    mask = np.isfinite(work[outcome])
    for col in regressors:
        mask &= np.isfinite(work[col])
    work = work.loc[mask].copy()

    absorb_df = _as_category(work[absorb].copy(), absorb)
    clusters = pd.Categorical(work[cluster]).codes.astype(np.int32)
    n_clusters = int(pd.Series(clusters).nunique())

    model = AbsorbingLS(
        dependent=work[outcome].astype(float),
        exog=work[regressors].astype(float),
        absorb=absorb_df,
        drop_absorbed=True,
    )
    result = model.fit(
        cov_type="clustered",
        clusters=clusters,
        method="auto",
        absorb_options={"drop_singletons": True},
        use_cache=True,
    )
    return {
        "result": result,
        "spec": spec,
        "fe_label": fe_label,
        "cluster_label": cluster_label,
        "n_clusters": n_clusters,
        **meta,
    }


def _tidy_result(bundle: dict) -> pd.DataFrame:
    res = bundle["result"]
    out = pd.DataFrame({
        "Coefficient": res.params.index.astype(str),
        "Estimate": res.params.to_numpy(),
        "Std. Error": res.std_errors.reindex(res.params.index).to_numpy(),
        "t value": res.tstats.reindex(res.params.index).to_numpy(),
        "Pr(>|t|)": res.pvalues.reindex(res.params.index).to_numpy(),
    })
    out["CI Low"] = out["Estimate"] - CI_Z * out["Std. Error"]
    out["CI High"] = out["Estimate"] + CI_Z * out["Std. Error"]
    out["spec"] = bundle["spec"]
    out["nobs"] = int(res.nobs)
    out["fe"] = bundle["fe_label"]
    out["cluster"] = bundle["cluster_label"]
    out["n_clusters"] = bundle["n_clusters"]
    for key, val in bundle.items():
        if key not in {"result", "spec", "fe_label", "cluster_label", "n_clusters"}:
            out[key] = val
    return out


def _save_tidy(results: list[dict], path: Path) -> pd.DataFrame:
    out = pd.concat([_tidy_result(bundle) for bundle in results], ignore_index=True)
    out.to_csv(path, index=False)
    print(f"  Saved: {path.relative_to(ROOT)}")
    return out


def _single_coef_plot(
    tidy: pd.DataFrame,
    coef_name: str,
    label_col: str,
    path: Path,
    title: str,
    xlabel: str,
    order: list[str],
    color_col: str | None = None,
    palette: dict[str, str] | None = None,
    figsize: tuple[float, float] = (8, 4.8),
) -> None:
    sub = tidy[tidy["Coefficient"] == coef_name].copy()
    sub[label_col] = pd.Categorical(sub[label_col], categories=order, ordered=True)
    sub = sub.sort_values(label_col)

    fig, ax = plt.subplots(figsize=figsize)
    y = np.arange(len(sub))

    for i, (_, row) in enumerate(sub.iterrows()):
        color = "steelblue"
        if color_col and palette:
            color = palette.get(row[color_col], "steelblue")
        ax.errorbar(
            x=row["Estimate"],
            y=i,
            xerr=CI_Z * row["Std. Error"],
            fmt="o",
            color=color,
            ms=5,
            lw=1.5,
            capsize=3,
        )

    ax.axvline(0, color="black", lw=0.8, linestyle="--")
    ax.set_yticks(y)
    ax.set_yticklabels(sub[label_col], fontsize=9)
    ax.set_xlabel(xlabel, fontsize=9)
    ax.set_title(title, fontsize=10)
    ax.grid(axis="x", color="#e0e0e0", lw=0.5)
    plt.tight_layout()
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {path.relative_to(ROOT)}")


def _region_split_plot(tidy: pd.DataFrame, path: Path) -> None:
    local = tidy[tidy["model"] == "local_interacted"].copy()
    dist = tidy[tidy["model"] == "dist_interacted"].copy()

    keep_regions = [r for r in REGION_ORDER if r in set(local["region"]) or r in set(dist["region"])]
    local = local[local["region"].isin(keep_regions)].copy()
    dist = dist[dist["region"].isin(keep_regions)].copy()

    local["region_short"] = pd.Categorical(
        local["region"].map(REGION_SHORT),
        categories=[REGION_SHORT[r] for r in keep_regions],
        ordered=True,
    )
    dist["region_short"] = pd.Categorical(
        dist["region"].map(REGION_SHORT),
        categories=[REGION_SHORT[r] for r in keep_regions],
        ordered=True,
    )
    local = local.sort_values("region_short")
    dist = dist.sort_values("region_short")

    fig, axes = plt.subplots(1, 2, figsize=(12, 6), sharey=True)
    for ax, sub, coef_label, title in [
        (axes[0], local, "Local", "Within-firm local premium by firm home region"),
        (axes[1], dist, "log(dist km)", "Within-firm distance slope by firm home region"),
    ]:
        y = np.arange(len(sub))
        ax.errorbar(
            x=sub["Estimate"],
            y=y,
            xerr=CI_Z * sub["Std. Error"],
            fmt="o",
            color="steelblue",
            ms=4.5,
            lw=1.4,
            capsize=3,
        )
        ax.axvline(0, color="black", lw=0.8, linestyle="--")
        ax.set_title(title, fontsize=10)
        ax.set_xlabel(f"{coef_label} coefficient", fontsize=9)
        ax.grid(axis="x", color="#e0e0e0", lw=0.5)
        ax.set_yticks(y)
        ax.set_yticklabels(sub["region_short"], fontsize=8)

    plt.tight_layout()
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {path.relative_to(ROOT)}")


print("=" * 70, flush=True)
print("LOAD BID ANALYSIS SAMPLE", flush=True)
print("=" * 70, flush=True)
print(f"  Sample: {SAMPLE}", flush=True)

sample_path = OUT_BIDS / f"bid_analysis_sample{SAMPLE_SUFFIX_STR}.parquet"
filter_in_memory = False
if not sample_path.exists():
    fallback_path = OUT_BIDS / "bid_analysis_sample.parquet"
    if SAMPLE != "all" and fallback_path.exists():
        sample_path = fallback_path
        filter_in_memory = True
        print("  [WARN] Sample-specific bid analysis sample not found; "
              "falling back to full sample and filtering in memory.", flush=True)
    else:
        sys.exit(f"ERROR: {sample_path.name} not found. Run 01 first.")

cols = [
    "log_bid_ratio",
    "local",
    "sme",
    "log_dist_km",
    "sector",
    "bidder_id_str",
    "year_month_str",
    "buyer_region_norm",
    "bidder_region_norm",
]
df = pd.read_parquet(sample_path, columns=cols)
if filter_in_memory:
    df = _filter_sector_sample(df, SAMPLE)
for col in ["log_bid_ratio", "local", "sme", "log_dist_km"]:
    df[col] = pd.to_numeric(df[col], errors="coerce")

region_match = df["bidder_region_norm"].notna() & df["buyer_region_norm"].notna()
df["local"] = pd.Series(pd.NA, index=df.index, dtype="Int8")
df.loc[region_match, "local"] = (
    df.loc[region_match, "bidder_region_norm"]
    == df.loc[region_match, "buyer_region_norm"]
).astype("int8")

bid_counts = df.groupby("bidder_id_str")["log_bid_ratio"].count()
keep_firms = bid_counts[bid_counts >= MIN_BIDS_FIRM].index
df = df[df["bidder_id_str"].isin(keep_firms) & df["local"].notna()].copy()
df = _as_category(df, ["bidder_id_str", "year_month_str", "buyer_region_norm"])

print(f"  Sample: {len(df):,} bids", flush=True)
print(f"  Firms with >= {MIN_BIDS_FIRM} bids: {len(keep_firms):,}", flush=True)


if RUN_SIZE:
    print("\n" + "=" * 70, flush=True)
    print("PART A — Within-firm local premium by SME vs. large", flush=True)
    print("=" * 70, flush=True)

    results_size: list[dict] = []
    size_specs = [
        ("SME", 1),
        ("Large", 0),
    ]
    for group_label, group_val in size_specs:
        sub = df[df["sme"] == group_val].copy()
        print(f"  {group_label}: {len(sub):,} bids", flush=True)
        results_size.append(
            _fit_hdfe(
                sub,
                outcome="log_bid_ratio",
                regressors=["local"],
                absorb=["bidder_id_str", "year_month_str"],
                cluster="bidder_id_str",
                spec=f"{group_label} — bidder+month FE",
                fe_label="Bidder + Month FE",
                cluster_label="Bidder",
                group=group_label,
                model="firm_time",
            )
        )
        results_size.append(
            _fit_hdfe(
                sub[sub["buyer_region_norm"].notna()].copy(),
                outcome="log_bid_ratio",
                regressors=["local"],
                absorb=["bidder_id_str", "year_month_str", "buyer_region_norm"],
                cluster="bidder_id_str",
                spec=f"{group_label} — +buyer-region FE",
                fe_label="Bidder + Month + Buyer region FE",
                cluster_label="Bidder",
                group=group_label,
                model="firm_time_region",
            )
        )

    tidy_size = _save_tidy(results_size, OUT_BIDS_TBL / f"bids_part2_size_split{SAMPLE_SUFFIX_STR}.csv")
    _single_coef_plot(
        tidy_size,
        coef_name="local",
        label_col="spec",
        order=[
            "SME — bidder+month FE",
            "SME — +buyer-region FE",
            "Large — bidder+month FE",
            "Large — +buyer-region FE",
        ],
        color_col="group",
        palette={"SME": "#1f77b4", "Large": "#d95f02"},
        title="Within-firm local premium by firm size",
        xlabel="Local coefficient",
        path=OUT_BIDS_FIG / f"bids_part2_size_split_local{SAMPLE_SUFFIX_STR}.png",
    )

if RUN_REGION:
    print("\n" + "=" * 70, flush=True)
    print("PART B — Within-firm local/distance specs by firm home region", flush=True)
    print("=" * 70, flush=True)

    df_region = df[df["bidder_region_norm"].notna() & df["local"].notna()].copy()
    region_counts = df_region["bidder_region_norm"].value_counts()
    for region in REGION_ORDER:
        if region in region_counts.index:
            print(f"  {region}: {int(region_counts[region]):,} bids", flush=True)

    local_support = df_region.groupby("bidder_region_norm")["local"].agg(["min", "max"])
    local_cols: dict[str, str] = {}
    for idx, region in enumerate(REGION_ORDER):
        if region not in region_counts.index:
            continue
        if region not in local_support.index:
            continue
        if local_support.loc[region, "min"] == local_support.loc[region, "max"]:
            continue
        col = f"local_r{idx:02d}"
        df_region[col] = (
            (df_region["bidder_region_norm"] == region).astype(float)
            * df_region["local"].astype(float)
        )
        local_cols[col] = region

    bundle_local = _fit_hdfe(
        df_region,
        outcome="log_bid_ratio",
        regressors=list(local_cols),
        absorb=["bidder_id_str", "year_month_str"],
        cluster="bidder_id_str",
        spec="Pooled local × origin region",
        fe_label="Bidder + Month FE",
        cluster_label="Bidder",
    )
    tidy_local = _tidy_result(bundle_local)
    tidy_local = tidy_local[tidy_local["Coefficient"].isin(local_cols)].copy()
    tidy_local["region"] = tidy_local["Coefficient"].map(local_cols)
    tidy_local["model"] = "local_interacted"

    df_dist = df_region[(df_region["local"] == 0) & df_region["log_dist_km"].notna()].copy()
    dist_counts = df_dist["bidder_region_norm"].value_counts()
    dist_cols: dict[str, str] = {}
    for idx, region in enumerate(REGION_ORDER):
        if region not in dist_counts.index:
            continue
        col = f"dist_r{idx:02d}"
        df_dist[col] = (
            (df_dist["bidder_region_norm"] == region).astype(float)
            * df_dist["log_dist_km"]
        )
        dist_cols[col] = region

    bundle_dist = _fit_hdfe(
        df_dist,
        outcome="log_bid_ratio",
        regressors=list(dist_cols),
        absorb=["bidder_id_str", "year_month_str"],
        cluster="bidder_id_str",
        spec="Pooled log(dist) × origin region",
        fe_label="Bidder + Month FE",
        cluster_label="Bidder",
    )
    tidy_dist = _tidy_result(bundle_dist)
    tidy_dist = tidy_dist[tidy_dist["Coefficient"].isin(dist_cols)].copy()
    tidy_dist["region"] = tidy_dist["Coefficient"].map(dist_cols)
    tidy_dist["model"] = "dist_interacted"

    tidy_region = pd.concat([tidy_local, tidy_dist], ignore_index=True)
    tidy_region.to_csv(OUT_BIDS_TBL / f"bids_part2_region_split{SAMPLE_SUFFIX_STR}.csv", index=False)
    print(f"  Saved: output/bids/tables/bids_part2_region_split{SAMPLE_SUFFIX_STR}.csv")
    _region_split_plot(
        tidy_region,
        OUT_BIDS_FIG / f"bids_part2_region_split{SAMPLE_SUFFIX_STR}.png",
    )

print("\nDone.", flush=True)
