"""
03_summarize_choice.py
──────────────────────────────────────────────────────────────────────────────
Summarize the buyer-level choice function estimates:
  · Histogram of buyer-level β(same_region) by dataset × period.
  · Classification of buyers into "price-focused" vs "local-preferring".
  · Pre/post comparison for licitaciones vs compra_agil.
  · Correlates of local preference (sector, region, tender volume).
  · Final summary tables and figures.

Outputs (under {OUTPUT_ROOT}/choice_function/):
  tables/
    pooled_lpm_results.tex / .csv
    buyer_preference_summary.csv
    preference_by_sector.csv
    preference_by_region.csv
    reform_shift_test.csv
  figures/
    hist_local_preference_*.png
    coefplot_pooled.png
"""

from __future__ import annotations

import sys
from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy import stats

matplotlib.use("Agg")

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1]))
from config import OUTPUT_ROOT  # noqa: E402

OUT_DIR     = OUTPUT_ROOT / "choice_function"
OUT_EST     = OUT_DIR / "estimates"
OUT_SAMPLES = OUT_DIR / "samples"
OUT_TAB     = OUT_DIR / "tables"
OUT_FIG     = OUT_DIR / "figures"
for _d in [OUT_TAB, OUT_FIG]:
    _d.mkdir(parents=True, exist_ok=True)


def load_buyer_coefs() -> pd.DataFrame:
    path = OUT_EST / "buyer_level_coefficients.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Missing {path}. Run 02 first.")
    b = pd.read_parquet(path)
    b[["dataset", "period"]] = b["label"].str.rsplit("_", n=1, expand=True)
    b["period"] = b["period"].fillna("")
    return b


def load_pooled() -> pd.DataFrame:
    p = OUT_EST / "pooled_results.csv"
    return pd.read_csv(p) if p.exists() else pd.DataFrame()


def classify_buyer(row) -> str:
    b = row["coef_same_region"]
    se = row["se_same_region"]
    if pd.isna(b) or pd.isna(se) or se <= 0:
        return "undefined"
    z = b / se
    if z > 1.96:
        return "local_preferring"
    if z < -1.96:
        return "price_or_anti_local"
    return "no_preference"


def summarize_distribution(b: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for label, grp in b.groupby("label"):
        vals = grp["coef_same_region"].dropna()
        rows.append({
            "label": label,
            "n_buyers": len(grp),
            "mean":   float(vals.mean())  if len(vals) else np.nan,
            "median": float(vals.median()) if len(vals) else np.nan,
            "sd":     float(vals.std())    if len(vals) else np.nan,
            "q25":    float(vals.quantile(0.25)) if len(vals) else np.nan,
            "q75":    float(vals.quantile(0.75)) if len(vals) else np.nan,
            "min":    float(vals.min()) if len(vals) else np.nan,
            "max":    float(vals.max()) if len(vals) else np.nan,
            "share_positive_signif":  float(((grp["coef_same_region"] / grp["se_same_region"]) > 1.96).mean()),
            "share_negative_signif":  float(((grp["coef_same_region"] / grp["se_same_region"]) < -1.96).mean()),
        })
    return pd.DataFrame(rows)


def plot_histograms(b: pd.DataFrame) -> None:
    # One hist per label
    for label, grp in b.groupby("label"):
        vals = grp["coef_same_region"].dropna()
        if vals.empty:
            continue
        fig, ax = plt.subplots(figsize=(6, 4))
        # clip extremes for readability
        clipped = vals.clip(vals.quantile(0.01), vals.quantile(0.99))
        ax.hist(clipped, bins=40, color="#1B9E77", alpha=0.8, edgecolor="white")
        ax.axvline(0, color="red", lw=1.2, ls="--", label="0")
        ax.axvline(float(vals.median()), color="black", lw=1.2, ls="-", label=f"median={vals.median():.3f}")
        ax.set_xlabel("Buyer-level β(same_region)")
        ax.set_ylabel("Number of buyers")
        ax.set_title(f"Local preference distribution — {label}")
        ax.legend(fontsize=8)
        ax.spines[["top", "right"]].set_visible(False)
        fig.tight_layout()
        fname = OUT_FIG / f"hist_local_preference_{label}.png"
        fig.savefig(fname, dpi=140, bbox_inches="tight")
        plt.close(fig)
        print(f"  Saved {fname.name}")

    # Side-by-side pre/post overlay for each dataset
    for dataset in ["licitaciones", "compra_agil"]:
        pre = b[b["label"] == f"{dataset}_pre"]["coef_same_region"].dropna()
        post = b[b["label"] == f"{dataset}_post"]["coef_same_region"].dropna()
        if pre.empty and post.empty:
            continue
        fig, ax = plt.subplots(figsize=(7, 4))
        lo = min(pre.quantile(0.01) if len(pre) else np.inf,
                 post.quantile(0.01) if len(post) else np.inf)
        hi = max(pre.quantile(0.99) if len(pre) else -np.inf,
                 post.quantile(0.99) if len(post) else -np.inf)
        bins = np.linspace(lo, hi, 40)
        if len(pre):
            ax.hist(pre.clip(lo, hi),  bins=bins, alpha=0.55, color="#1B9E77",
                    edgecolor="white", label=f"pre (n={len(pre)})")
        if len(post):
            ax.hist(post.clip(lo, hi), bins=bins, alpha=0.55, color="#D95F02",
                    edgecolor="white", label=f"post (n={len(post)})")
        ax.axvline(0, color="red", lw=1.2, ls="--")
        ax.set_xlabel("Buyer-level β(same_region)")
        ax.set_ylabel("Number of buyers")
        ax.set_title(f"Local preference pre vs post — {dataset}")
        ax.legend(fontsize=8)
        ax.spines[["top", "right"]].set_visible(False)
        fig.tight_layout()
        fname = OUT_FIG / f"hist_local_preference_pre_vs_post_{dataset}.png"
        fig.savefig(fname, dpi=140, bbox_inches="tight")
        plt.close(fig)
        print(f"  Saved {fname.name}")


def reform_shift(b: pd.DataFrame) -> pd.DataFrame:
    """Mann-Whitney + mean diff test, pre vs post within each dataset."""
    rows = []
    for dataset in ["licitaciones", "compra_agil"]:
        pre = b.loc[b["label"] == f"{dataset}_pre",  "coef_same_region"].dropna()
        post = b.loc[b["label"] == f"{dataset}_post", "coef_same_region"].dropna()
        if len(pre) < 5 or len(post) < 5:
            continue
        mw = stats.mannwhitneyu(pre, post, alternative="two-sided")
        t  = stats.ttest_ind(pre, post, equal_var=False)
        rows.append({
            "dataset":       dataset,
            "n_pre":         len(pre),
            "n_post":        len(post),
            "median_pre":    float(pre.median()),
            "median_post":   float(post.median()),
            "diff_median":   float(post.median() - pre.median()),
            "mean_pre":      float(pre.mean()),
            "mean_post":     float(post.mean()),
            "diff_mean":     float(post.mean() - pre.mean()),
            "mannwhitney_p": float(mw.pvalue),
            "welch_t":       float(t.statistic),
            "welch_p":       float(t.pvalue),
        })
    return pd.DataFrame(rows)


def classify_and_share(b: pd.DataFrame) -> pd.DataFrame:
    b = b.copy()
    b["buyer_class"] = b.apply(classify_buyer, axis=1)
    shares = (b.groupby(["label", "buyer_class"]).size()
                .unstack(fill_value=0))
    shares["n_total"] = shares.sum(axis=1)
    for c in shares.columns:
        if c == "n_total":
            continue
        shares[f"pct_{c}"] = 100 * shares[c] / shares["n_total"]
    return shares.reset_index()


def correlates_by_sector_region(b: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    # Merge in buyer attributes from a sample file (licitaciones_post is comprehensive).
    all_frames = []
    for label in ["licitaciones_post", "licitaciones_pre", "compra_agil_post", "compra_agil_pre"]:
        p = OUT_SAMPLES / f"choice_sample_{label}.parquet"
        if p.exists():
            all_frames.append(pd.read_parquet(p, columns=["rut_unidad", "sector", "region_buyer"]))
    if not all_frames:
        return pd.DataFrame(), pd.DataFrame()
    attrs = pd.concat(all_frames, ignore_index=True).drop_duplicates("rut_unidad")
    merged = b.merge(attrs, on="rut_unidad", how="left")

    sector = (merged.groupby(["label", "sector"])
                     .agg(n_buyers=("coef_same_region", "count"),
                          median_beta=("coef_same_region", "median"),
                          mean_beta=("coef_same_region", "mean"),
                          share_positive=("coef_same_region",
                                          lambda s: float(((s / merged.loc[s.index, "se_same_region"]) > 1.96).mean())))
                     .reset_index()
                     .sort_values(["label", "median_beta"], ascending=[True, False]))

    region = (merged.groupby(["label", "region_buyer"])
                     .agg(n_buyers=("coef_same_region", "count"),
                          median_beta=("coef_same_region", "median"),
                          mean_beta=("coef_same_region", "mean"))
                     .reset_index()
                     .sort_values(["label", "median_beta"], ascending=[True, False]))
    return sector, region


def coefplot_pooled(pooled: pd.DataFrame) -> None:
    if pooled.empty:
        return
    # Keep coefficients for primary regressors only
    keys = ["coef_same_region", "coef_is_lowest_bid", "coef_log_bid_ratio",
            "coef_sme", "coef_sr_x_low"]
    labels = ["same_region", "is_lowest_bid", "log_bid_ratio", "sme", "sr × low"]
    n_labels = len(pooled)
    fig, axes = plt.subplots(1, len(keys), figsize=(13, 3.5), sharey=True)
    ys = np.arange(n_labels)
    color_map = {"licitaciones_pre": "#1B9E77", "licitaciones_post": "#D95F02",
                 "compra_agil_pre":  "#7570B3", "compra_agil_post":  "#E7298A"}
    for ax, key, lab in zip(axes, keys, labels):
        for i, (_, row) in enumerate(pooled.iterrows()):
            b = row.get(key, np.nan)
            se = row.get(key.replace("coef_", "se_"), np.nan)
            if pd.isna(b) or pd.isna(se):
                continue
            c = color_map.get(row["label"], "#666")
            ax.errorbar(b, ys[i], xerr=1.96 * se, fmt="o", ms=4, color=c, ecolor="#888", elinewidth=1.3, capsize=3)
        ax.axvline(0, color="#444", lw=1, ls="--")
        ax.set_title(lab, fontsize=10)
        ax.spines[["top", "right"]].set_visible(False)
    axes[0].set_yticks(ys)
    axes[0].set_yticklabels(pooled["label"].tolist(), fontsize=9)
    fig.suptitle("Pooled LPM coefficients (buyer + year-month FE, cluster SEs by buyer)", fontsize=11)
    fig.tight_layout(rect=[0, 0, 1, 0.95])
    fname = OUT_FIG / "coefplot_pooled.png"
    fig.savefig(fname, dpi=140, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved {fname.name}")


def main() -> None:
    b = load_buyer_coefs()
    print(f"Loaded {len(b):,} buyer-level estimates across "
          f"{b['label'].nunique()} labels.")

    dist = summarize_distribution(b)
    dist.to_csv(OUT_TAB / "buyer_preference_summary.csv", index=False)
    print("\n=== BUYER-LEVEL β(same_region) DISTRIBUTION ===")
    print(dist.to_string(index=False))

    shares = classify_and_share(b)
    shares.to_csv(OUT_TAB / "buyer_classification_shares.csv", index=False)
    print("\n=== BUYER TYPE SHARES ===")
    print(shares.to_string(index=False))

    reform = reform_shift(b)
    reform.to_csv(OUT_TAB / "reform_shift_test.csv", index=False)
    print("\n=== REFORM SHIFT (pre vs post) ===")
    print(reform.to_string(index=False))

    sector, region = correlates_by_sector_region(b)
    if not sector.empty:
        sector.to_csv(OUT_TAB / "preference_by_sector.csv", index=False)
    if not region.empty:
        region.to_csv(OUT_TAB / "preference_by_region.csv", index=False)

    pooled = load_pooled()
    if not pooled.empty:
        pooled.to_csv(OUT_TAB / "pooled_lpm_results.csv", index=False)
        coefplot_pooled(pooled)

    plot_histograms(b)

    print(f"\nAll tables to {OUT_TAB}")
    print(f"All figures to {OUT_FIG}")


if __name__ == "__main__":
    main()
