"""
10_kde_licitaciones_value.py
─────────────────────────────────────────────────────────────────────────────
Kernel density distributions for licitaciones estimated value:
  - 2022–2024
  - 2025

Input:
  data/clean/combined_sii_merged_filtered.parquet

Outputs:
  output/diagnostics/figures/lic_kde_value_2022_2024.png
  output/diagnostics/figures/lic_kde_value_2025.png
  output/diagnostics/figures/lic_kde_value_2022_2025_overlay.png
  output/summary_stats/lic_kde_value_summary.csv
"""

from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from scipy.stats import gaussian_kde

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config import DATA_CLEAN, OUTPUT_ROOT  # noqa: E402

IN_FILE = DATA_CLEAN / "combined_sii_merged_filtered.parquet"
FIG_DIR = OUTPUT_ROOT / "diagnostics" / "figures"
SUM_DIR = OUTPUT_ROOT / "summary_stats"
FIG_DIR.mkdir(parents=True, exist_ok=True)
SUM_DIR.mkdir(parents=True, exist_ok=True)


def prepare_tender_panel() -> pd.DataFrame:
    cols = ["dataset", "tender_id", "fecha_pub", "monto_estimado", "is_key_dup"]
    df = pd.read_parquet(IN_FILE, columns=cols, filters=[("dataset", "=", "licitaciones")])
    df = df[~df["is_key_dup"]].copy()
    df["fecha_pub"] = pd.to_datetime(df["fecha_pub"], errors="coerce")
    df = df.drop_duplicates("tender_id")
    df = df[df["fecha_pub"].notna() & df["monto_estimado"].notna() & (df["monto_estimado"] > 0)].copy()
    df["year"] = df["fecha_pub"].dt.year
    df["period_group"] = np.where(df["year"] == 2025, "2025", "2022-2024")
    df = df[df["period_group"].isin(["2022-2024", "2025"])].copy()
    return df


def density_on_log_values(values: np.ndarray, grid_log: np.ndarray) -> np.ndarray:
    kde = gaussian_kde(np.log10(values))
    return kde(grid_log)


def plot_single(group_df: pd.DataFrame, title: str, out_name: str) -> None:
    vals = group_df["monto_estimado"].to_numpy()
    lo = np.log10(np.percentile(vals, 1))
    hi = np.log10(np.percentile(vals, 99.5))
    grid_log = np.linspace(lo, hi, 600)
    dens = density_on_log_values(vals, grid_log)
    x = 10 ** grid_log

    fig, ax = plt.subplots(figsize=(11, 5))
    ax.plot(x, dens, lw=2.5, color="#1f77b4")
    ax.set_xscale("log")
    ax.set_xlabel("Estimated value (CLP, log scale)")
    ax.set_ylabel("Kernel density (on log10 value)")
    ax.set_title(title, fontweight="bold")
    ax.grid(axis="both", alpha=0.25)
    ax.spines[["top", "right"]].set_visible(False)
    plt.tight_layout()
    fig.savefig(FIG_DIR / out_name, dpi=150, bbox_inches="tight")
    print(f"  Saved: {out_name}")
    plt.close()


def main() -> None:
    print("=" * 70)
    print("KDE distribution — licitaciones estimated value")
    print("=" * 70)

    df = prepare_tender_panel()
    print(f"  Tenders used: {len(df):,}")
    print(f"  2022–2024: {(df['period_group'] == '2022-2024').sum():,}")
    print(f"  2025:      {(df['period_group'] == '2025').sum():,}")

    # Summary table
    summary = (
        df.groupby("period_group")["monto_estimado"]
        .agg(
            n="count",
            mean="mean",
            median="median",
            p10=lambda s: s.quantile(0.10),
            p25=lambda s: s.quantile(0.25),
            p75=lambda s: s.quantile(0.75),
            p90=lambda s: s.quantile(0.90),
            min="min",
            max="max",
        )
        .reset_index()
    )
    summary.to_csv(SUM_DIR / "lic_kde_value_summary.csv", index=False)
    print("  Saved: lic_kde_value_summary.csv")

    # Individual KDEs
    plot_single(
        df[df["period_group"] == "2022-2024"],
        "Licitaciones estimated value KDE — 2022 to 2024",
        "lic_kde_value_2022_2024.png",
    )
    plot_single(
        df[df["period_group"] == "2025"],
        "Licitaciones estimated value KDE — 2025",
        "lic_kde_value_2025.png",
    )

    # Overlay KDE for direct comparison
    vals_pre = df[df["period_group"] == "2022-2024"]["monto_estimado"].to_numpy()
    vals_post = df[df["period_group"] == "2025"]["monto_estimado"].to_numpy()
    lo = np.log10(np.percentile(np.concatenate([vals_pre, vals_post]), 1))
    hi = np.log10(np.percentile(np.concatenate([vals_pre, vals_post]), 99.5))
    grid_log = np.linspace(lo, hi, 700)
    x = 10 ** grid_log
    dens_pre = density_on_log_values(vals_pre, grid_log)
    dens_post = density_on_log_values(vals_post, grid_log)

    fig, ax = plt.subplots(figsize=(11, 5))
    ax.plot(x, dens_pre, lw=2.5, color="#1f77b4", label="2022–2024")
    ax.plot(x, dens_post, lw=2.5, color="#d62728", label="2025")
    ax.set_xscale("log")
    ax.set_xlabel("Estimated value (CLP, log scale)")
    ax.set_ylabel("Kernel density (on log10 value)")
    ax.set_title("Licitaciones estimated value KDE — 2022–2024 vs 2025", fontweight="bold")
    ax.legend()
    ax.grid(axis="both", alpha=0.25)
    ax.spines[["top", "right"]].set_visible(False)
    plt.tight_layout()
    fig.savefig(FIG_DIR / "lic_kde_value_2022_2025_overlay.png", dpi=150, bbox_inches="tight")
    print("  Saved: lic_kde_value_2022_2025_overlay.png")
    plt.close()

    print("\nDone.")


if __name__ == "__main__":
    main()
