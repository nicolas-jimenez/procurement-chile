"""
02_run_bid_regressions.py
──────────────────────────────────────────────────────────────────────────────
Bid-level markup regressions using high-dimensional fixed effects.

Backend: linearmodels.AbsorbingLS (pyhdfe)

Outcome:  log_bid_ratio = log(submitted bid / estimated cost)

Outputs
  output/bids/tables/bids_part1_auction_fe.csv
  output/bids/tables/bids_part2_firm_fe.csv
  output/bids/tables/bids_part3_did.csv
  output/bids/tables/bids_part3_event_study.csv
  output/bids/tables/bids_part1_auction_fe.tex
  output/bids/tables/bids_part2_firm_fe.tex
  output/bids/tables/bids_part3_did.tex
  output/bids/figures/bids_part1_coefplot.png
  output/bids/figures/bids_part1_resid_density_by_size.png
  output/bids/figures/bids_part2_coefplot.png
  output/bids/figures/bids_part2_resid_density_local_vs_nonlocal.png
  output/bids/figures/bids_part3_did_coefplot.png
  output/bids/figures/bids_part3_event_study.png
"""

from __future__ import annotations

import argparse
import gc
import sys
import warnings
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from linearmodels.iv import AbsorbingLS

warnings.filterwarnings("ignore", category=FutureWarning)

SAMPLE_SUFFIX = {"all": "", "municipalidades": "_munic", "obras": "_obras"}
SAMPLE_KEYWORD = {"municipalidades": "municipal", "obras": "obras"}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run bid-level markup regressions.")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--part1-only", action="store_true", help="Run only Part 1 outputs.")
    group.add_argument("--part2-only", action="store_true", help="Run only Part 2 outputs.")
    group.add_argument("--part3-only", action="store_true", help="Run only Part 3 outputs.")
    parser.add_argument(
        "--sample",
        choices=["all", "municipalidades", "obras"],
        default="all",
        help="Sample restriction on buyer sector. Uses the same suffix convention as the DiD scripts.",
    )
    return parser.parse_args()


ARGS = _parse_args()
if ARGS.part1_only:
    RUN_PARTS = {1}
elif ARGS.part2_only:
    RUN_PARTS = {2}
elif ARGS.part3_only:
    RUN_PARTS = {3}
else:
    RUN_PARTS = {1, 2, 3}
RUN_PART1 = 1 in RUN_PARTS
RUN_PART2 = 2 in RUN_PARTS
RUN_PART3 = 3 in RUN_PARTS
SAMPLE = ARGS.sample
SAMPLE_SUFFIX_STR = SAMPLE_SUFFIX[SAMPLE]

# ── Paths ────────────────────────────────────────────────────────────────────
HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
sys.path.insert(0, str(ROOT / "code" / "analysis" / "did"))

from did_utils import REFORM_PERIOD  # noqa: E402

OUT_BIDS = ROOT / "output" / "bids"
OUT_BIDS_TBL = OUT_BIDS / "tables"
OUT_BIDS_FIG = OUT_BIDS / "figures"

# ── Configuration ────────────────────────────────────────────────────────────
ES_MIN, ES_MAX = -6, 9
ES_REF = -1
MIN_BIDS_FIRM = 3
PART1_PRE_ONLY = True
CI_Z = 1.96
DENSITY_SAMPLE_MAX = 150_000
DENSITY_TRIM = (0.01, 0.99)
DENSITY_SEED = 20260306
DIST_BIN_EDGES = [0, 150, 400, 800, np.inf]
DIST_BIN_LABELS = ["0-150 km", "150-400 km", "400-800 km", "800+ km"]
DIST_BIN_VARS = [
    "dist_bin_150_400",
    "dist_bin_400_800",
    "dist_bin_800_plus",
]


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


def _as_category(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for col in cols:
        df[col] = df[col].astype("category")
    return df


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
    out["r2"] = float(res.rsquared)
    out["fe"] = bundle["fe_label"]
    out["cluster"] = bundle["cluster_label"]
    out["n_clusters"] = bundle["n_clusters"]
    return out


def _save_tidy(results: list[dict], path: Path) -> pd.DataFrame:
    out = pd.concat([_tidy_result(bundle) for bundle in results], ignore_index=True)
    out.to_csv(path, index=False)
    print(f"  Saved: {path.relative_to(ROOT)}")
    return out


def _stars(pval: float) -> str:
    if pd.isna(pval):
        return ""
    if pval < 0.01:
        return "***"
    if pval < 0.05:
        return "**"
    if pval < 0.10:
        return "*"
    return ""


def _escape_tex(text: str) -> str:
    repl = {
        "\\": r"\textbackslash{}",
        "&": r"\&",
        "%": r"\%",
        "$": r"\$",
        "#": r"\#",
        "_": r"\_",
        "{": r"\{",
        "}": r"\}",
    }
    out = str(text)
    for src, dst in repl.items():
        out = out.replace(src, dst)
    return out


def _save_tex_table(
    results: list[dict],
    path: Path,
    coef_map: list[tuple[str, str]],
) -> None:
    tidy_by_spec = {
        bundle["spec"]: _tidy_result(bundle).set_index("Coefficient")
        for bundle in results
    }
    col_fmt = "l" + "c" * len(results)
    lines = [
        r"\begin{tabular}{" + col_fmt + "}",
        r"\toprule",
        " & " + " & ".join(_escape_tex(bundle["spec"]) for bundle in results) + r" \\",
        r"\midrule",
    ]

    for coef_name, coef_label in coef_map:
        est_cells = [_escape_tex(coef_label)]
        se_cells = [""]
        for bundle in results:
            tidy = tidy_by_spec[bundle["spec"]]
            if coef_name in tidy.index:
                row = tidy.loc[coef_name]
                est_cells.append(f"{row['Estimate']:.3f}{_stars(row['Pr(>|t|)'])}")
                se_cells.append(f"({row['Std. Error']:.3f})")
            else:
                est_cells.append("")
                se_cells.append("")
        lines.append(" & ".join(est_cells) + r" \\")
        lines.append(" & ".join(se_cells) + r" \\")

    lines.extend([
        r"\midrule",
        "Observations & "
        + " & ".join(f"{int(bundle['result'].nobs):,}" for bundle in results)
        + r" \\",
        "Fixed effects & "
        + " & ".join(_escape_tex(bundle["fe_label"]) for bundle in results)
        + r" \\",
        "Clusters & "
        + " & ".join(f"{bundle['n_clusters']:,}" for bundle in results)
        + r" \\",
        r"\bottomrule",
        r"\end{tabular}",
    ])

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"  Saved: {path.relative_to(ROOT)}")


def _fit_hdfe(
    data: pd.DataFrame,
    outcome: str,
    regressors: list[str],
    absorb: list[str],
    cluster: str,
    spec: str,
    fe_label: str,
    cluster_label: str,
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

    absorb_df = work[absorb].copy()
    absorb_df = _as_category(absorb_df, absorb)

    clusters = pd.Categorical(work[cluster]).codes.astype(np.int32)
    n_clusters = int(pd.Series(clusters).nunique())
    exog = work[regressors].astype(float)

    model = AbsorbingLS(
        dependent=work[outcome].astype(float),
        exog=exog,
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
    }


def _coefplot(
    results: list[dict],
    coef_map: list[tuple[str, str]],
    title: str,
    path: Path,
    figsize: tuple[float, float] = (8, 5),
) -> None:
    fig, ax = plt.subplots(figsize=figsize)
    n_specs = len(results)
    offsets = np.linspace(-0.25, 0.25, n_specs)
    colors = plt.cm.tab10(np.linspace(0, 0.6, n_specs))

    for si, (bundle, color) in enumerate(zip(results, colors)):
        tidy = _tidy_result(bundle).set_index("Coefficient")
        for ci, (coef_name, _) in enumerate(coef_map):
            if coef_name not in tidy.index:
                continue
            row = tidy.loc[coef_name]
            y_pos = ci + offsets[si]
            ax.errorbar(
                x=row["Estimate"],
                y=y_pos,
                xerr=CI_Z * row["Std. Error"],
                fmt="o",
                color=color,
                ms=5,
                lw=1.5,
                capsize=3,
                label=bundle["spec"] if ci == 0 else None,
            )

    ax.axvline(0, color="black", lw=0.8, linestyle="--")
    ax.set_yticks(range(len(coef_map)))
    ax.set_yticklabels([label for _, label in coef_map], fontsize=9)
    ax.set_xlabel("Coefficient estimate (log bid / cost)", fontsize=9)
    ax.set_title(title, fontsize=10)
    ax.legend(loc="lower right", fontsize=8, framealpha=0.7)
    plt.tight_layout()
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {path.relative_to(ROOT)}")


def _es_plot(
    tidy: pd.DataFrame,
    ref: int,
    title: str,
    path: Path,
) -> None:
    es = tidy[tidy["Coefficient"].str.startswith("k_rel_")].copy()
    es["k"] = es["Coefficient"].str.replace("k_rel_", "", regex=False).astype(int)
    es = es.sort_values("k")

    ref_row = pd.DataFrame({
        "k": [ref],
        "Estimate": [0.0],
        "Std. Error": [0.0],
    })
    es = pd.concat([es, ref_row], ignore_index=True).sort_values("k")

    fig, ax = plt.subplots(figsize=(9, 4))
    ax.axhline(0, color="black", lw=0.8)
    ax.axvline(-0.5, color="tomato", lw=1, linestyle="--", label="Reform (k=0)")

    ci95 = CI_Z * es["Std. Error"]
    ax.fill_between(
        es["k"],
        es["Estimate"] - ci95,
        es["Estimate"] + ci95,
        alpha=0.2,
        color="steelblue",
    )
    ax.plot(es["k"], es["Estimate"], "o-", color="steelblue", ms=4, lw=1.5)

    ax.set_xlabel("Months relative to reform (k=0 = Dec 2024)", fontsize=9)
    ax.set_ylabel("Coef on treated × 1[t=k]  (log bid / cost)", fontsize=9)
    ax.set_title(title, fontsize=10)
    ax.set_xticks(range(int(es["k"].min()), int(es["k"].max()) + 1))
    ax.legend(fontsize=8)
    ax.grid(axis="y", color="#e0e0e0", lw=0.5)
    plt.tight_layout()
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {path.relative_to(ROOT)}")


def _fit_fe_only_residuals(
    data: pd.DataFrame,
    outcome: str,
    absorb: list[str],
    keep_cols: list[str],
) -> pd.DataFrame:
    cols = list(dict.fromkeys([outcome, *absorb, *keep_cols]))
    work = data[cols].copy()
    work = work.dropna(subset=absorb)
    work[outcome] = pd.to_numeric(work[outcome], errors="coerce")
    work = work[np.isfinite(work[outcome])].copy()

    absorb_df = _as_category(work[absorb].copy(), absorb)
    exog = pd.DataFrame({"const": np.ones(len(work), dtype=float)}, index=work.index)
    model = AbsorbingLS(
        dependent=work[outcome].astype(float),
        exog=exog,
        absorb=absorb_df,
        drop_absorbed=True,
    )
    result = model.fit(
        method="auto",
        absorb_options={"drop_singletons": True},
        use_cache=True,
    )

    out = work[keep_cols].copy()
    out["residual"] = result.resids.astype(float)
    return out


def _sample_for_density(series: pd.Series, seed_offset: int = 0) -> pd.Series:
    s = series.dropna()
    if len(s) > DENSITY_SAMPLE_MAX:
        s = s.sample(DENSITY_SAMPLE_MAX, random_state=DENSITY_SEED + seed_offset)
    return s


def _add_distance_bin_dummies(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["dist_bin"] = pd.cut(
        out["dist_km"],
        bins=DIST_BIN_EDGES,
        labels=DIST_BIN_LABELS,
        include_lowest=True,
        right=True,
    )
    for label, var in zip(DIST_BIN_LABELS[1:], DIST_BIN_VARS):
        out[var] = (out["dist_bin"] == label).astype(float)
    return out


def _plot_part1_resid_density(plot_df: pd.DataFrame, path: Path) -> None:
    fig, axes = plt.subplots(1, 3, figsize=(12, 4), sharey=True)
    groups = ["SME", "Large", "Non-SII"]
    palette = {1: "#1f77b4", 0: "#d95f02", "overall": "#6a4c93"}

    for i, (ax, group) in enumerate(zip(axes, groups)):
        sub = plot_df[plot_df["size_group"] == group].copy()
        if sub.empty:
            ax.set_visible(False)
            continue

        lo, hi = sub["residual"].quantile(list(DENSITY_TRIM))
        sub = sub[sub["residual"].between(lo, hi)].copy()

        if group == "Non-SII":
            overall = _sample_for_density(sub["residual"], seed_offset=30 + i)
            sns.kdeplot(
                overall,
                ax=ax,
                color=palette["overall"],
                lw=2,
                cut=0,
                label="Locality unavailable",
            )
        else:
            for local_val, label in [(1, "Local"), (0, "Non-local")]:
                s = _sample_for_density(
                    sub.loc[sub["local"] == local_val, "residual"],
                    seed_offset=10 * i + local_val,
                )
                sns.kdeplot(
                    s,
                    ax=ax,
                    color=palette[local_val],
                    lw=2,
                    cut=0,
                    label=label,
                )

        ax.axvline(0, color="black", lw=0.8, linestyle="--")
        ax.set_title(group, fontsize=11)
        ax.set_xlabel("Auction-FE residual", fontsize=9)
        ax.grid(axis="y", color="#e0e0e0", lw=0.5)
        if i == 0:
            ax.set_ylabel("Density", fontsize=9)
        else:
            ax.set_ylabel("")
        ax.legend(fontsize=8, frameon=False)

    fig.suptitle("Bid markup residuals by locality and firm type", fontsize=12)
    fig.text(
        0.5,
        0.01,
        "Pre-reform sample; residuals from auction-FE-only model. Non-SII bids lack locality coding in the merged sample.",
        ha="center",
        fontsize=9,
    )
    plt.tight_layout(rect=(0, 0.05, 1, 0.95))
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {path.relative_to(ROOT)}")


def _plot_part2_resid_density(plot_df: pd.DataFrame, path: Path) -> None:
    fig, ax = plt.subplots(figsize=(8.5, 4.5))
    palette = {1: "#1f77b4", 0: "#d95f02"}

    lo, hi = plot_df["residual"].quantile(list(DENSITY_TRIM))
    sub = plot_df[plot_df["residual"].between(lo, hi)].copy()

    for local_val, label in [(1, "Local project"), (0, "Non-local project")]:
        s = _sample_for_density(sub.loc[sub["local"] == local_val, "residual"], seed_offset=100 + local_val)
        sns.kdeplot(
            s,
            ax=ax,
            color=palette[local_val],
            lw=2.2,
            cut=0,
            label=label,
        )

    ax.axvline(0, color="black", lw=0.8, linestyle="--")
    ax.set_xlim(-3, 2)
    ax.set_title("Within-firm bid markup residuals by project locality", fontsize=12)
    ax.set_xlabel("Bidder + month FE residual", fontsize=9)
    ax.set_ylabel("Density", fontsize=9)
    ax.grid(axis="y", color="#e0e0e0", lw=0.5)
    ax.legend(fontsize=9, frameon=False)
    fig.text(
        0.5,
        0.01,
        "Full sample; residuals from bidder + month FE-only model.",
        ha="center",
        fontsize=9,
    )
    plt.tight_layout(rect=(0, 0.05, 1, 1))
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {path.relative_to(ROOT)}")


def _print_key_rows(results: list[dict], coef_names: list[str]) -> None:
    for bundle in results:
        tidy = _tidy_result(bundle).set_index("Coefficient")
        print(f"    {bundle['spec']}")
        for coef_name in coef_names:
            if coef_name in tidy.index:
                row = tidy.loc[coef_name]
                print(
                    f"      {coef_name:18s} "
                    f"{row['Estimate']:>8.4f} "
                    f"(se {row['Std. Error']:.4f}, p={row['Pr(>|t|)']:.4g})"
                )


# ═════════════════════════════════════════════════════════════════════════════
print("=" * 70)
print("LOAD BID ANALYSIS SAMPLE")
print("=" * 70)
print(f"  Running parts: {', '.join(str(p) for p in sorted(RUN_PARTS))}")
print(f"  Sample: {SAMPLE}")

sample_path = OUT_BIDS / f"bid_analysis_sample{SAMPLE_SUFFIX_STR}.parquet"
filter_in_memory = False
if not sample_path.exists():
    fallback_path = OUT_BIDS / "bid_analysis_sample.parquet"
    if SAMPLE != "all" and fallback_path.exists():
        sample_path = fallback_path
        filter_in_memory = True
        print("  [WARN] Sample-specific bid analysis sample not found; "
              "falling back to full sample and filtering in memory.")
    else:
        sys.exit(
            f"ERROR: {sample_path.name} not found.\n"
            "Run 01_build_bid_sample.py first."
        )

part12_cols = [
    "log_bid_ratio",
    "local",
    "sme",
    "dist_km",
    "log_dist_km",
    "post",
    "sector",
    "bidder_id_str",
    "tender_id_str",
    "year_month_str",
    "buyer_region_norm",
    "bidder_region_norm",
]
if RUN_PART1 or RUN_PART2:
    df12 = pd.read_parquet(sample_path, columns=part12_cols)
    if filter_in_memory:
        df12 = _filter_sector_sample(df12, SAMPLE)
    print(f"  Loaded Part 1/2 columns: {len(df12):,} bids × {df12.shape[1]} columns")

    for col in ["log_bid_ratio", "local", "sme", "log_dist_km", "post"]:
        df12[col] = pd.to_numeric(df12[col], errors="coerce")

    print(f"  Pre-reform: {(df12['post'] == 0).sum():,}  Post: {(df12['post'] == 1).sum():,}")


# ═════════════════════════════════════════════════════════════════════════════
if RUN_PART1:
    print("\n" + "=" * 70)
    print("PART 1 — Cross-sectional bid differences  (auction FEs)")
    print("=" * 70)

    if PART1_PRE_ONLY:
        df1 = df12[df12["post"] == 0].copy()
        print(f"  Pre-reform only: {len(df1):,} bids")
    else:
        df1 = df12.copy()
        print(f"  Full period: {len(df1):,} bids")

    df1 = df1[df1["local"].notna() & df1["log_dist_km"].notna()].copy()
    df1["tender_id_str"] = df1["tender_id_str"].astype("category")
    df1_sme = df1[df1["sme"].notna()].copy()

    print(f"  Sample for local/distance specs: {len(df1):,}")
    print(f"  Sample for SME specs: {len(df1_sme):,}")
    print("\n  Fitting Part 1 specs …")

    results1 = [
        _fit_hdfe(
            df1,
            outcome="log_bid_ratio",
            regressors=["local"],
            absorb=["tender_id_str"],
            cluster="bidder_id_str",
            spec="(1a) local",
            fe_label="Tender FE",
            cluster_label="Bidder",
        ),
        _fit_hdfe(
            df1,
            outcome="log_bid_ratio",
            regressors=["local", "log_dist_km"],
            absorb=["tender_id_str"],
            cluster="bidder_id_str",
            spec="(1b) local+dist",
            fe_label="Tender FE",
            cluster_label="Bidder",
        ),
        _fit_hdfe(
            df1_sme,
            outcome="log_bid_ratio",
            regressors=["sme"],
            absorb=["tender_id_str"],
            cluster="bidder_id_str",
            spec="(1c) SME",
            fe_label="Tender FE",
            cluster_label="Bidder",
        ),
        _fit_hdfe(
            df1_sme,
            outcome="log_bid_ratio",
            regressors=["local", "sme"],
            absorb=["tender_id_str"],
            cluster="bidder_id_str",
            spec="(1d) local+SME",
            fe_label="Tender FE",
            cluster_label="Bidder",
        ),
        _fit_hdfe(
            df1_sme,
            outcome="log_bid_ratio",
            regressors=["local", "log_dist_km", "sme"],
            absorb=["tender_id_str"],
            cluster="bidder_id_str",
            spec="(1e) full",
            fe_label="Tender FE",
            cluster_label="Bidder",
        ),
    ]

    print("\n  Part 1 key coefficients:")
    _print_key_rows(results1, ["local", "log_dist_km", "sme"])

    tidy1 = _save_tidy(results1, OUT_BIDS_TBL / f"bids_part1_auction_fe{SAMPLE_SUFFIX_STR}.csv")
    _save_tex_table(
        results1,
        OUT_BIDS_TBL / f"bids_part1_auction_fe{SAMPLE_SUFFIX_STR}.tex",
        coef_map=[
            ("local", "Local"),
            ("log_dist_km", "log(dist km)"),
            ("sme", "SME"),
        ],
    )
    _coefplot(
        results1,
        coef_map=[
            ("local", "Local"),
            ("log_dist_km", "log(dist km)"),
            ("sme", "SME"),
        ],
        title="Part 1: Bid markup by bidder type (auction FEs, pre-reform)",
        path=OUT_BIDS_FIG / f"bids_part1_coefplot{SAMPLE_SUFFIX_STR}.png",
    )

    print("\n  Residual density figure (Part 1) …")
    df1_density = df12[df12["post"] == 0].copy()
    df1_density["tender_id_str"] = df1_density["tender_id_str"].astype("category")
    part1_resid = _fit_fe_only_residuals(
        df1_density,
        outcome="log_bid_ratio",
        absorb=["tender_id_str"],
        keep_cols=["local", "sme"],
    )
    part1_resid["size_group"] = np.select(
        [
            part1_resid["sme"].eq(1).fillna(False).to_numpy(),
            part1_resid["sme"].eq(0).fillna(False).to_numpy(),
            part1_resid["sme"].isna().to_numpy(),
        ],
        ["SME", "Large", "Non-SII"],
        default="Other",
    )
    _plot_part1_resid_density(
        part1_resid[part1_resid["size_group"].isin(["SME", "Large", "Non-SII"])].copy(),
        OUT_BIDS_FIG / f"bids_part1_resid_density_by_size{SAMPLE_SUFFIX_STR}.png",
    )


if RUN_PART2:
    print("\n" + "=" * 70)
    print("PART 2 — Within-firm bid differences  (firm FEs)")
    print("=" * 70)

    bid_counts = df12.groupby("bidder_id_str")["log_bid_ratio"].count()
    keep_firms = bid_counts[bid_counts >= MIN_BIDS_FIRM].index
    df2 = df12[df12["bidder_id_str"].isin(keep_firms)].copy()
    region_match = df2["bidder_region_norm"].notna() & df2["buyer_region_norm"].notna()
    df2["local_within"] = pd.Series(pd.NA, index=df2.index, dtype="Int8")
    df2.loc[region_match, "local_within"] = (
        df2.loc[region_match, "bidder_region_norm"]
        == df2.loc[region_match, "buyer_region_norm"]
    ).astype("int8")
    df2 = df2[df2["local_within"].notna()].copy()
    df2 = _as_category(df2, ["bidder_id_str", "year_month_str", "buyer_region_norm", "bidder_region_norm"])
    df2_nonlocal = df2[
        (df2["local_within"] == 0)
        & df2["log_dist_km"].notna()
        & df2["dist_km"].notna()
    ].copy()
    df2_nonlocal = _add_distance_bin_dummies(df2_nonlocal)
    print(f"  Firms with ≥{MIN_BIDS_FIRM} bids: {len(keep_firms):,}")
    print(f"  Part 2 local sample: {len(df2):,} bids")
    print(f"  Part 2 non-local distance sample: {len(df2_nonlocal):,} bids")

    print("\n  Fitting Part 2 specs …")

    results2_local = [
        _fit_hdfe(
            df2,
            outcome="log_bid_ratio",
            regressors=["local_within"],
            absorb=["bidder_id_str"],
            cluster="bidder_id_str",
            spec="(2a) firm FE",
            fe_label="Bidder FE",
            cluster_label="Bidder",
        ),
        _fit_hdfe(
            df2,
            outcome="log_bid_ratio",
            regressors=["local_within"],
            absorb=["bidder_id_str", "year_month_str"],
            cluster="bidder_id_str",
            spec="(2b) firm+time FE",
            fe_label="Bidder + Month FE",
            cluster_label="Bidder",
        ),
        _fit_hdfe(
            df2[df2["buyer_region_norm"].notna()].copy(),
            outcome="log_bid_ratio",
            regressors=["local_within"],
            absorb=["bidder_id_str", "year_month_str", "buyer_region_norm"],
            cluster="bidder_id_str",
            spec="(2c) +region FE",
            fe_label="Bidder + Month + Buyer region FE",
            cluster_label="Bidder",
        ),
    ]
    results2_distance = [
        _fit_hdfe(
            df2_nonlocal,
            outcome="log_bid_ratio",
            regressors=["log_dist_km"],
            absorb=["bidder_id_str", "year_month_str"],
            cluster="bidder_id_str",
            spec="(2d) non-local dist",
            fe_label="Bidder + Month FE",
            cluster_label="Bidder",
        ),
        _fit_hdfe(
            df2_nonlocal,
            outcome="log_bid_ratio",
            regressors=DIST_BIN_VARS,
            absorb=["bidder_id_str", "year_month_str"],
            cluster="bidder_id_str",
            spec="(2e) non-local bins",
            fe_label="Bidder + Month FE",
            cluster_label="Bidder",
        ),
        _fit_hdfe(
            df2_nonlocal,
            outcome="log_bid_ratio",
            regressors=DIST_BIN_VARS,
            absorb=["year_month_str", "bidder_region_norm"],
            cluster="bidder_id_str",
            spec="(2f) bins + firm-region FE",
            fe_label="Month + Firm-home-region FE",
            cluster_label="Bidder",
        ),
    ]
    results2 = results2_local + results2_distance

    print("\n  Part 2 key coefficients:")
    _print_key_rows(results2, ["local_within", "log_dist_km", *DIST_BIN_VARS])

    tidy2 = _save_tidy(results2, OUT_BIDS_TBL / f"bids_part2_firm_fe{SAMPLE_SUFFIX_STR}.csv")
    _save_tex_table(
        results2,
        OUT_BIDS_TBL / f"bids_part2_firm_fe{SAMPLE_SUFFIX_STR}.tex",
        coef_map=[
            ("local_within", "Local"),
            ("log_dist_km", "log(dist km), non-local only"),
            ("dist_bin_150_400", "Dist 150-400 km"),
            ("dist_bin_400_800", "Dist 400-800 km"),
            ("dist_bin_800_plus", "Dist 800+ km"),
        ],
    )
    _coefplot(
        results2_local + results2_distance[:1],
        coef_map=[
            ("local_within", "Local"),
            ("log_dist_km", "log(dist km), non-local only"),
        ],
        title="Part 2: Within-firm local effect and non-local distance slope",
        path=OUT_BIDS_FIG / f"bids_part2_coefplot{SAMPLE_SUFFIX_STR}.png",
    )

    print("\n  Residual density figure (Part 2) …")
    part2_resid = _fit_fe_only_residuals(
        df2.assign(local=df2["local_within"].astype(float)),
        outcome="log_bid_ratio",
        absorb=["bidder_id_str", "year_month_str"],
        keep_cols=["local"],
    )
    _plot_part2_resid_density(
        part2_resid[part2_resid["local"].notna()].copy(),
        OUT_BIDS_FIG / f"bids_part2_resid_density_local_vs_nonlocal{SAMPLE_SUFFIX_STR}.png",
    )

for _tmp_name in ["df1", "df1_sme", "df1_density", "part1_resid", "df2", "df2_nonlocal", "part2_resid", "df12"]:
    if _tmp_name in globals():
        del globals()[_tmp_name]
gc.collect()


# ═════════════════════════════════════════════════════════════════════════════
if RUN_PART3:
    print("\n" + "=" * 70)
    print("PART 3 — Pre/post reform DiD at bid level  (entity × time FEs)")
    print("=" * 70)

    part3_cols = [
        "log_bid_ratio",
        "treated",
        "post",
        "local",
        "sme",
        "band",
        "sector",
        "entity_str",
        "year_month_str",
        "k_rel",
    ]
    df3 = pd.read_parquet(sample_path, columns=part3_cols)
    if filter_in_memory:
        df3 = _filter_sector_sample(df3, SAMPLE)
    print(f"  Loaded Part 3 columns: {len(df3):,} bids × {df3.shape[1]} columns")

    for col in ["log_bid_ratio", "treated", "post", "local", "sme", "k_rel"]:
        df3[col] = pd.to_numeric(df3[col], errors="coerce")

    df3 = df3[df3["band"].isin(["treated", "control_high"])].copy()
    print(f"  Analysis sample rows: {len(df3):,} bids")

    df3 = _as_category(df3, ["entity_str", "year_month_str"])
    print(f"  Treated: {(df3['treated'] == 1).sum():,}  Control: {(df3['treated'] == 0).sum():,}")
    print(f"  Pre: {(df3['post'] == 0).sum():,}  Post: {(df3['post'] == 1).sum():,}")

    df3["treated_post"] = df3["treated"] * df3["post"]
    print("\n  Fitting Part 3 DiD specs …")

    results3 = [
        _fit_hdfe(
            df3,
            outcome="log_bid_ratio",
            regressors=["treated", "post", "treated_post"],
            absorb=["entity_str", "year_month_str"],
            cluster="entity_str",
            spec="(3a) DiD",
            fe_label="Entity + Month FE",
            cluster_label="Entity",
        ),
        _fit_hdfe(
            df3[df3["local"].notna()].assign(local=lambda x: x["local"].astype(float)),
            outcome="log_bid_ratio",
            regressors=["treated", "post", "treated_post", "local"],
            absorb=["entity_str", "year_month_str"],
            cluster="entity_str",
            spec="(3b) +local",
            fe_label="Entity + Month FE",
            cluster_label="Entity",
        ),
    ]

    df3_loc = df3[df3["local"].notna()].copy()
    df3_loc["treated_local"] = df3_loc["treated"] * df3_loc["local"]
    df3_loc["post_local"] = df3_loc["post"] * df3_loc["local"]
    df3_loc["treated_post_local"] = df3_loc["treated"] * df3_loc["post"] * df3_loc["local"]
    results3.append(
        _fit_hdfe(
            df3_loc,
            outcome="log_bid_ratio",
            regressors=[
                "treated",
                "post",
                "local",
                "treated_post",
                "treated_local",
                "post_local",
                "treated_post_local",
            ],
            absorb=["entity_str", "year_month_str"],
            cluster="entity_str",
            spec="(3c) DiD×local",
            fe_label="Entity + Month FE",
            cluster_label="Entity",
        )
    )

    df3_sme = df3[df3["sme"].notna()].copy()
    df3_sme["treated_sme"] = df3_sme["treated"] * df3_sme["sme"]
    df3_sme["post_sme"] = df3_sme["post"] * df3_sme["sme"]
    df3_sme["treated_post_sme"] = df3_sme["treated"] * df3_sme["post"] * df3_sme["sme"]
    results3.append(
        _fit_hdfe(
            df3_sme,
            outcome="log_bid_ratio",
            regressors=[
                "treated",
                "post",
                "sme",
                "treated_post",
                "treated_sme",
                "post_sme",
                "treated_post_sme",
            ],
            absorb=["entity_str", "year_month_str"],
            cluster="entity_str",
            spec="(3d) DiD×SME",
            fe_label="Entity + Month FE",
            cluster_label="Entity",
        )
    )

    print("\n  Part 3 key coefficients:")
    _print_key_rows(results3, ["treated_post", "treated_post_local", "treated_post_sme", "local", "sme"])

    tidy3 = _save_tidy(results3, OUT_BIDS_TBL / f"bids_part3_did{SAMPLE_SUFFIX_STR}.csv")
    _save_tex_table(
        results3,
        OUT_BIDS_TBL / f"bids_part3_did{SAMPLE_SUFFIX_STR}.tex",
        coef_map=[
            ("treated_post", "Treated × Post"),
            ("local", "Local"),
            ("treated_post_local", "Treated × Post × Local"),
            ("sme", "SME"),
            ("treated_post_sme", "Treated × Post × SME"),
        ],
    )
    _coefplot(
        results3,
        coef_map=[
            ("treated_post", "Treated × Post"),
            ("treated_post_local", "Treated × Post × Local"),
            ("treated_post_sme", "Treated × Post × SME"),
        ],
        title="Part 3: DiD bid markup — reform effects",
        path=OUT_BIDS_FIG / f"bids_part3_did_coefplot{SAMPLE_SUFFIX_STR}.png",
    )

    print("\n  Fitting Part 3 event study …")

    df3_es = df3[
        df3["k_rel"].notna()
        & (df3["k_rel"] >= ES_MIN)
        & (df3["k_rel"] <= ES_MAX)
    ].copy()
    df3_es["k_rel"] = df3_es["k_rel"].astype(int)
    es_cols = []
    for k in range(ES_MIN, ES_MAX + 1):
        if k == ES_REF:
            continue
        col = f"k_rel_{k}"
        df3_es[col] = ((df3_es["k_rel"] == k) & (df3_es["treated"] == 1)).astype(float)
        es_cols.append(col)

    print(f"  Event study sample: {len(df3_es):,} bids  (k ∈ [{ES_MIN}, {ES_MAX}])")

    result_es = _fit_hdfe(
        df3_es,
        outcome="log_bid_ratio",
        regressors=es_cols,
        absorb=["entity_str", "year_month_str"],
        cluster="entity_str",
        spec="event_study",
        fe_label="Entity + Month FE",
        cluster_label="Entity",
    )
    tidy_es = _tidy_result(result_es)
    tidy_es.to_csv(OUT_BIDS_TBL / f"bids_part3_event_study{SAMPLE_SUFFIX_STR}.csv", index=False)
    print(f"  Saved: output/bids/tables/bids_part3_event_study{SAMPLE_SUFFIX_STR}.csv")
    _es_plot(
        tidy_es,
        ref=ES_REF,
        title="Part 3 Event Study: bid markup × treated",
        path=OUT_BIDS_FIG / f"bids_part3_event_study{SAMPLE_SUFFIX_STR}.png",
    )

print("\n" + "=" * 70)
print(f"Reform period anchor: {REFORM_PERIOD}")
print("DONE — all bid regression outputs saved to output/bids/")
print("=" * 70)
