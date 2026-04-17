"""
11_bunching_100utm_2025q1.py
─────────────────────────────────────────────────────────────────────────────
Tests for bunching around a UTM threshold for licitaciones:
  - pre period: quarters < 2025Q1
  - post period: quarters >= 2025Q1

Input:
  data/clean/combined_sii_merged_filtered.parquet
  data/raw/other/utm_clp_2022_2025.csv

Outputs:
  output/diagnostics/figures/lic_bunching_<threshold>utm_pre_post_2025q1.png
  output/summary_stats/lic_bunching_<threshold>utm_pre_post_2025q1.csv
  output/summary_stats/lic_bunching_<threshold>utm_band_test.csv
"""

from pathlib import Path
import argparse

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from scipy.stats import chi2_contingency

ROOT = Path(__file__).resolve().parents[3]
IN_FILE = ROOT / "data" / "clean" / "combined_sii_merged_filtered.parquet"
UTM_FILE = ROOT / "data" / "raw" / "other" / "utm_clp_2022_2025.csv"
FIG_DIR = ROOT / "output" / "diagnostics" / "figures"
SUM_DIR = ROOT / "output" / "summary_stats"
FIG_DIR.mkdir(parents=True, exist_ok=True)
SUM_DIR.mkdir(parents=True, exist_ok=True)

WINDOW = 50.0
BUNCH_HALF = 5.0
BIN_W = 1.0
POLY_ORDER = 4


def load_tenders(dataset: str) -> pd.DataFrame:
    cols = [
        "dataset", "tender_id", "fecha_pub", "source_year", "source_month",
        "monto_estimado", "is_key_dup",
    ]
    df = pd.read_parquet(IN_FILE, columns=cols, filters=[("dataset", "=", dataset)])
    df = df[~df["is_key_dup"]].copy()
    df["fecha_pub"] = pd.to_datetime(df["fecha_pub"], errors="coerce")
    df = df.drop_duplicates("tender_id")
    df = df[
        df["fecha_pub"].notna()
        & df["monto_estimado"].notna()
        & (df["monto_estimado"] > 0)
        & df["source_year"].notna()
        & df["source_month"].notna()
    ].copy()

    utm = pd.read_csv(UTM_FILE).rename(columns={"year": "source_year", "month_num": "source_month", "utm_clp": "utm_clp_rate"})
    utm["source_year"] = pd.to_numeric(utm["source_year"], errors="coerce").astype("Int64")
    utm["source_month"] = pd.to_numeric(utm["source_month"], errors="coerce").astype("Int64")

    df["source_year"] = pd.to_numeric(df["source_year"], errors="coerce").astype("Int64")
    df["source_month"] = pd.to_numeric(df["source_month"], errors="coerce").astype("Int64")
    df = df.merge(utm[["source_year", "source_month", "utm_clp_rate"]], on=["source_year", "source_month"], how="left")
    df["monto_utm"] = df["monto_estimado"] / df["utm_clp_rate"]
    df = df[df["monto_utm"].notna() & (df["monto_utm"] > 0)].copy()
    df["quarter"] = df["fecha_pub"].dt.to_period("Q")
    df["period"] = np.where(df["quarter"] < pd.Period("2025Q1", freq="Q"), "pre_2025Q1", "post_2025Q1")
    return df


def bunching_stats(values: np.ndarray, threshold: float) -> tuple[pd.DataFrame, dict]:
    v = values[(values >= threshold - WINDOW) & (values <= threshold + WINDOW)]
    edges = np.arange(threshold - WINDOW, threshold + WINDOW + BIN_W, BIN_W)
    counts, edges = np.histogram(v, bins=edges)
    centers = (edges[:-1] + edges[1:]) / 2.0

    x = centers - threshold
    exclude = np.abs(x) <= BUNCH_HALF
    fit_mask = ~exclude

    coefs = np.polyfit(x[fit_mask], counts[fit_mask], POLY_ORDER)
    pred = np.polyval(coefs, x)
    pred = np.clip(pred, a_min=0, a_max=None)

    below = (centers >= threshold - BUNCH_HALF) & (centers < threshold)
    above = (centers >= threshold) & (centers < threshold + BUNCH_HALF)
    around = below | above

    actual_around = counts[around].sum()
    pred_around = pred[around].sum()
    excess_around = actual_around - pred_around
    excess_ratio = excess_around / pred_around if pred_around > 0 else np.nan

    out_df = pd.DataFrame(
        {
            "bin_left": edges[:-1],
            "bin_right": edges[1:],
            "bin_center": centers,
            "count": counts,
            "counterfactual_count": pred,
            "in_bunch_window": around,
            "below_threshold_window": below,
            "above_threshold_window": above,
        }
    )

    stats = {
        "n_window": int(len(v)),
        "actual_around": float(actual_around),
        "pred_around": float(pred_around),
        "excess_around": float(excess_around),
        "excess_ratio": float(excess_ratio),
        "actual_below": float(counts[below].sum()),
        "actual_above": float(counts[above].sum()),
    }
    return out_df, stats


def main() -> None:
    parser = argparse.ArgumentParser(description="Bunching test around UTM threshold (pre/post 2025Q1)")
    parser.add_argument(
        "--dataset",
        choices=["licitaciones", "compra_agil"],
        default="licitaciones",
        help="Dataset to analyze (default: licitaciones)",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=100.0,
        help="UTM threshold to test (default: 100.0)",
    )
    args = parser.parse_args()
    dataset = args.dataset
    threshold = float(args.threshold)
    threshold_tag = f"{int(threshold)}" if float(threshold).is_integer() else str(threshold).replace(".", "p")
    ds_label = "Licitaciones" if dataset == "licitaciones" else "Compra Ágil"
    ds_tag = "lic" if dataset == "licitaciones" else "ca"

    print("=" * 70)
    print(f"Bunching test around {threshold:g} UTM — {ds_label} — pre/post 2025Q1")
    print("=" * 70)

    df = load_tenders(dataset=dataset)
    print(f"  Tenders used: {len(df):,}")
    print(f"  Pre 2025Q1:  {(df['period'] == 'pre_2025Q1').sum():,}")
    print(f"  Post 2025Q1: {(df['period'] == 'post_2025Q1').sum():,}")

    pre_vals = df[df["period"] == "pre_2025Q1"]["monto_utm"].to_numpy()
    post_vals = df[df["period"] == "post_2025Q1"]["monto_utm"].to_numpy()

    pre_bins, pre_stats = bunching_stats(pre_vals, threshold=threshold)
    post_bins, post_stats = bunching_stats(post_vals, threshold=threshold)

    summary = pd.DataFrame(
        [
            {"period": "pre_2025Q1", **pre_stats},
            {"period": "post_2025Q1", **post_stats},
        ]
    )
    summary_name = f"{ds_tag}_bunching_{threshold_tag}utm_pre_post_2025q1.csv"
    summary.to_csv(SUM_DIR / summary_name, index=False)
    print(f"  Saved: {summary_name}")

    # Directional bunching test: below-vs-above mass in [95,100) and [100,105)
    table = np.array(
        [
            [pre_stats["actual_below"], pre_stats["actual_above"]],
            [post_stats["actual_below"], post_stats["actual_above"]],
        ]
    )
    chi2, pval, dof, _ = chi2_contingency(table)
    band_test = pd.DataFrame(
        {
            "chi2": [chi2],
            "p_value": [pval],
            "dof": [dof],
            "pre_below": [pre_stats["actual_below"]],
            "pre_above": [pre_stats["actual_above"]],
            "post_below": [post_stats["actual_below"]],
            "post_above": [post_stats["actual_above"]],
            "pre_below_to_above_ratio": [
                pre_stats["actual_below"] / pre_stats["actual_above"] if pre_stats["actual_above"] > 0 else np.nan
            ],
            "post_below_to_above_ratio": [
                post_stats["actual_below"] / post_stats["actual_above"] if post_stats["actual_above"] > 0 else np.nan
            ],
        }
    )
    band_name = f"{ds_tag}_bunching_{threshold_tag}utm_band_test.csv"
    band_test.to_csv(SUM_DIR / band_name, index=False)
    print(f"  Saved: {band_name}")

    # Plot
    fig, axes = plt.subplots(2, 1, figsize=(12, 9), sharex=True)

    for ax, bins_df, title, color in [
        (axes[0], pre_bins, "Pre 2025Q1", "#1f77b4"),
        (axes[1], post_bins, "Post 2025Q1", "#d62728"),
    ]:
        ax.bar(bins_df["bin_center"], bins_df["count"], width=BIN_W * 0.95, alpha=0.45, color=color, label="Observed count")
        ax.plot(bins_df["bin_center"], bins_df["counterfactual_count"], color="black", lw=2, label="Counterfactual (poly fit)")
        ax.axvline(threshold, color="#444", ls="--", lw=1.4, label=f"{threshold:g} UTM")
        ax.axvspan(threshold - BUNCH_HALF, threshold + BUNCH_HALF, color="#999", alpha=0.15, label="Bunching window")
        ax.set_title(title, fontweight="bold")
        ax.set_ylabel("Tender count (binned)")
        ax.grid(axis="y", alpha=0.25)
        ax.spines[["top", "right"]].set_visible(False)

    handles, labels = axes[0].get_legend_handles_labels()
    by_label = dict(zip(labels, handles))
    axes[0].legend(by_label.values(), by_label.keys(), fontsize=9)
    axes[1].set_xlabel("Estimated value (UTM)")
    fig.suptitle(f"{ds_label} bunching test around {threshold:g} UTM\nPre vs Post 2025Q1", fontsize=13, fontweight="bold")
    plt.tight_layout()
    fig_name = f"{ds_tag}_bunching_{threshold_tag}utm_pre_post_2025q1.png"
    fig.savefig(FIG_DIR / fig_name, dpi=150, bbox_inches="tight")
    print(f"  Saved: {fig_name}")
    plt.close()

    print("\nKey stats:")
    print(summary.to_string(index=False))
    print("\nBand test (below-vs-above around threshold):")
    print(band_test.to_string(index=False))
    print("\nDone.")


if __name__ == "__main__":
    main()
