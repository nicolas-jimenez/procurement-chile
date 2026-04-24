"""
10_municipal_quarterly_reform.py
─────────────────────────────────────────────────────────────────────────────
Municipalidades procurement around the December 2024 reform.

Builds a tender-level sample from the combined merged file (which is still at
bid/item level), then runs the requested quarterly descriptives and buyer-FE
regressions for two samples:

  1. all_projects  : all municipal tenders (licitaciones + compra_agil)
  2. lt500utm      : municipal tenders with estimated value < 500 UTM

The post indicator follows the requested definition:
  post = 1[fecha_pub >= 2025-01-01]
which is equivalent to 2025Q1 onward at the quarter level.

Input:
  data/clean/combined_sii_merged_filtered.parquet
  data/raw/other/utm_clp_2022_2025.csv

Outputs:
  output/municipal_quarterly_reform/
    sample_summary.csv
    tender_diagnostics.csv
    all_projects/
      figures/
      tables/
    lt500utm/
      figures/
      tables/
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import numpy as np
import pandas as pd
import pyfixest as pf

matplotlib.use("Agg")

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1]))
from config import DATA_CLEAN, DATA_RAW_OTHER, OUTPUT_ROOT  # noqa: E402


IN_FILE = DATA_CLEAN / "combined_sii_merged_filtered.parquet"
UTM_FILE = DATA_RAW_OTHER / "utm_clp_2022_2025.csv"
OUT_DIR = OUTPUT_ROOT / "municipal_quarterly_reform"

POST_START = pd.Timestamp("2025-01-01")
POST_QUARTER = pd.Period("2025Q1", freq="Q")
REFORM_LABEL = "Post = 1 starting 2025Q1"

SAMPLES = [
    ("all_projects", "All municipal projects", None),
    ("lt500utm", "Municipal projects < 500 UTM", 500.0),
]

OUT_DIR.mkdir(parents=True, exist_ok=True)


def ensure_sample_dirs(sample_key: str) -> tuple[Path, Path]:
    sample_dir = OUT_DIR / sample_key
    fig_dir = sample_dir / "figures"
    tab_dir = sample_dir / "tables"
    fig_dir.mkdir(parents=True, exist_ok=True)
    tab_dir.mkdir(parents=True, exist_ok=True)
    return fig_dir, tab_dir


def savefig(path: Path) -> None:
    plt.tight_layout()
    plt.savefig(path, dpi=160, bbox_inches="tight")
    print(f"  Saved figure: {path}")
    plt.close()


def fmt_mm(x: float, _pos: int) -> str:
    return f"{x:,.0f}"


def _coef_or_nan(series: pd.Series, key: str) -> float:
    if key not in series.index:
        return np.nan
    return float(series[key])


def winsorize_series(
    s: pd.Series,
    *,
    lower_q: float = 0.01,
    upper_q: float = 0.99,
) -> tuple[pd.Series, float, float]:
    lo = float(s.quantile(lower_q))
    hi = float(s.quantile(upper_q))
    return s.clip(lower=lo, upper=hi), lo, hi


def load_utm_table() -> pd.DataFrame:
    utm = pd.read_csv(UTM_FILE)
    utm.columns = [c.strip() for c in utm.columns]
    rename: dict[str, str] = {}
    for col in utm.columns:
        cl = col.lower().strip()
        if cl in ("year", "anio", "año"):
            rename[col] = "source_year"
        elif cl in ("month_num", "month", "mes"):
            rename[col] = "source_month"
        elif "utm" in cl:
            rename[col] = "utm_clp"
    seen: dict[str, str] = {}
    for src, tgt in rename.items():
        seen[tgt] = src
    rename = {src: tgt for tgt, src in seen.items()}
    utm = utm.rename(columns=rename)
    utm["source_year"] = pd.to_numeric(utm["source_year"], errors="coerce").astype("Int64")
    utm["source_month"] = pd.to_numeric(utm["source_month"], errors="coerce").astype("Int64")
    utm["utm_clp"] = pd.to_numeric(utm["utm_clp"], errors="coerce")
    return utm[["source_year", "source_month", "utm_clp"]].dropna()


def add_utm_value(df: pd.DataFrame, utm: pd.DataFrame) -> pd.DataFrame:
    out = df.merge(utm, on=["source_year", "source_month"], how="left")
    out["monto_utm"] = out["monto_estimado"] / out["utm_clp"]
    return out


def extract_fe_result(
    res,
    *,
    sample_key: str,
    spec: str,
    outcome: str,
    sample: pd.DataFrame,
) -> dict[str, float | int | str]:
    coef = res.coef()
    se = res.se()
    pval = res.pvalue()
    return {
        "sample_key": sample_key,
        "spec": spec,
        "outcome": outcome,
        "n_obs": int(res._N),
        "n_buyers": int(sample["buyer_id"].nunique()),
        "n_quarters": int(sample["quarter"].nunique()),
        "post_start": POST_START.date().isoformat(),
        "coef_post": _coef_or_nan(coef, "post"),
        "se_post": _coef_or_nan(se, "post"),
        "p_post": _coef_or_nan(pval, "post"),
        "r2_within": float(getattr(res, "_r2_within", np.nan)),
        "mean_pre": float(sample.loc[sample["post"] == 0, outcome].mean()),
        "mean_post": float(sample.loc[sample["post"] == 1, outcome].mean()),
    }


def load_tender_level_sample() -> pd.DataFrame:
    print("=" * 70)
    print("STEP 1 — Build tender-level Municipalidades sample")
    print("=" * 70)

    if not IN_FILE.exists():
        raise FileNotFoundError(f"Missing input: {IN_FILE}")

    con = duckdb.connect()
    con.read_parquet(str(IN_FILE)).create_view("raw")

    query = """
    with tender as (
        select
            dataset,
            tender_id,
            min(rut_unidad) as rut_unidad,
            min(fecha_pub) as fecha_pub,
            min(source_year) as source_year,
            min(source_month) as source_month,
            max(monto_estimado) as monto_estimado,
            count(*) as raw_rows,
            count(distinct rut_unidad) as n_rut_unidad_vals,
            count(distinct fecha_pub) as n_fecha_pub_vals,
            count(distinct monto_estimado) as n_monto_vals,
            count(distinct source_year) as n_source_year_vals,
            count(distinct source_month) as n_source_month_vals
        from raw
        where coalesce(is_key_dup, false) = false
          and sector ilike '%municipal%'
        group by 1, 2
    )
    select *
    from tender
    """
    tender = con.execute(query).fetchdf()
    con.close()

    tender["fecha_pub"] = pd.to_datetime(tender["fecha_pub"], errors="coerce")
    tender["rut_unidad"] = tender["rut_unidad"].astype("string")
    tender["source_year"] = pd.to_numeric(tender["source_year"], errors="coerce").astype("Int64")
    tender["source_month"] = pd.to_numeric(tender["source_month"], errors="coerce").astype("Int64")

    before = len(tender)
    tender = tender.dropna(
        subset=["rut_unidad", "fecha_pub", "monto_estimado", "source_year", "source_month"]
    ).copy()
    tender = tender[tender["monto_estimado"] > 0].copy()

    utm = load_utm_table()
    tender = add_utm_value(tender, utm)
    tender = tender[tender["monto_utm"].notna() & np.isfinite(tender["monto_utm"])].copy()
    tender = tender[tender["monto_utm"] > 0].copy()

    tender["quarter"] = tender["fecha_pub"].dt.to_period("Q")
    tender["post"] = (tender["fecha_pub"] >= POST_START).astype(int)
    tender["buyer_id"] = tender["rut_unidad"].astype(str)
    tender["monto_estimado_mm"] = tender["monto_estimado"] / 1e6
    tender["log_monto_estimado"] = np.log(tender["monto_estimado"])

    diagnostics = pd.DataFrame(
        [
            {"metric": "tenders_before_dropna", "value": before},
            {"metric": "tenders_final", "value": len(tender)},
            {"metric": "buyers_final", "value": tender["buyer_id"].nunique()},
            {"metric": "quarters_final", "value": tender["quarter"].nunique()},
            {
                "metric": "tenders_multi_rut_unidad",
                "value": int((tender["n_rut_unidad_vals"] > 1).sum()),
            },
            {
                "metric": "tenders_multi_fecha_pub",
                "value": int((tender["n_fecha_pub_vals"] > 1).sum()),
            },
            {
                "metric": "tenders_multi_monto_estimado",
                "value": int((tender["n_monto_vals"] > 1).sum()),
            },
            {
                "metric": "tenders_multi_source_year",
                "value": int((tender["n_source_year_vals"] > 1).sum()),
            },
            {
                "metric": "tenders_multi_source_month",
                "value": int((tender["n_source_month_vals"] > 1).sum()),
            },
            {"metric": "first_quarter", "value": str(tender["quarter"].min())},
            {"metric": "last_quarter", "value": str(tender["quarter"].max())},
            {"metric": "post_start", "value": POST_START.date().isoformat()},
        ]
    )
    diagnostics.to_csv(OUT_DIR / "tender_diagnostics.csv", index=False)

    print(f"  Tender rows after collapse/filtering: {len(tender):,}")
    print(f"  Buyers: {tender['buyer_id'].nunique():,}")
    print(f"  Quarter span: {tender['quarter'].min()} to {tender['quarter'].max()}")
    print("  Dataset mix:")
    print(tender["dataset"].value_counts(dropna=False).to_string())

    return tender


def subset_tender(tender: pd.DataFrame, utm_upper: float | None) -> pd.DataFrame:
    if utm_upper is None:
        return tender.copy()
    out = tender[tender["monto_utm"] < utm_upper].copy()
    return out


def build_municipality_quarter_panel(tender: pd.DataFrame, tab_dir: Path) -> pd.DataFrame:
    panel = (
        tender.groupby(["buyer_id", "quarter"], as_index=False)
        .agg(
            n_tenders=("tender_id", "nunique"),
            total_expenditure_clp=("monto_estimado", "sum"),
            avg_project_size_clp=("monto_estimado", "mean"),
        )
    )

    all_buyers = pd.Index(sorted(tender["buyer_id"].dropna().unique()), name="buyer_id")
    all_quarters = pd.Index(sorted(tender["quarter"].dropna().unique()), name="quarter")
    full_index = pd.MultiIndex.from_product([all_buyers, all_quarters], names=["buyer_id", "quarter"])

    balanced = panel.set_index(["buyer_id", "quarter"]).reindex(full_index).reset_index()
    balanced["n_tenders"] = balanced["n_tenders"].fillna(0).astype(int)
    balanced["total_expenditure_clp"] = balanced["total_expenditure_clp"].fillna(0.0)
    balanced["avg_project_size_clp"] = balanced["avg_project_size_clp"].astype(float)
    balanced["total_expenditure_mm"] = balanced["total_expenditure_clp"] / 1e6
    balanced["avg_project_size_mm"] = balanced["avg_project_size_clp"] / 1e6
    balanced["log1p_total_expenditure"] = np.log1p(balanced["total_expenditure_clp"])
    balanced["post"] = (balanced["quarter"] >= POST_QUARTER).astype(int)

    balanced.to_csv(tab_dir / "municipality_quarter_panel.csv", index=False)
    return balanced


def plot_avg_project_size(
    tender: pd.DataFrame,
    *,
    sample_label: str,
    fig_dir: Path,
    tab_dir: Path,
) -> pd.DataFrame:
    quarterly = (
        tender.groupby("quarter", as_index=False)
        .agg(
            avg_project_size_mm=("monto_estimado_mm", "mean"),
            median_project_size_mm=("monto_estimado_mm", "median"),
            n_tenders=("tender_id", "nunique"),
            total_expenditure_mm=("monto_estimado_mm", "sum"),
        )
        .sort_values("quarter")
    )
    quarterly["quarter_str"] = quarterly["quarter"].astype(str)
    quarterly.to_csv(tab_dir / "quarterly_avg_project_size.csv", index=False)

    x = np.arange(len(quarterly))
    fig, ax = plt.subplots(figsize=(11, 5))
    ax.plot(
        x,
        quarterly["avg_project_size_mm"],
        color="#0b6e4f",
        marker="o",
        lw=2.2,
        ms=4.5,
    )

    if POST_QUARTER in set(quarterly["quarter"]):
        reform_idx = quarterly.index[quarterly["quarter"] == POST_QUARTER][0]
        ax.axvline(reform_idx - 0.5, color="#7a1c1c", lw=1.5, ls="--")
        ax.text(
            reform_idx - 0.45,
            float(quarterly["avg_project_size_mm"].max()) * 1.03,
            REFORM_LABEL,
            color="#7a1c1c",
            fontsize=9,
            ha="left",
            va="bottom",
        )

    ax.set_title(f"{sample_label}: average project size by quarter", fontweight="bold")
    ax.set_xlabel("Quarter")
    ax.set_ylabel("Average project size (million CLP)")
    ax.set_xticks(x)
    ax.set_xticklabels(quarterly["quarter_str"], rotation=45, ha="right")
    ax.yaxis.set_major_formatter(mtick.FuncFormatter(fmt_mm))
    ax.grid(axis="y", alpha=0.3)
    ax.spines[["top", "right"]].set_visible(False)
    savefig(fig_dir / "avg_project_size_by_quarter.png")
    return quarterly


def plot_avg_expenditure(
    panel: pd.DataFrame,
    *,
    sample_label: str,
    fig_dir: Path,
    tab_dir: Path,
) -> pd.DataFrame:
    quarterly = (
        panel.groupby("quarter", as_index=False)
        .agg(
            avg_expenditure_mm=("total_expenditure_mm", "mean"),
            median_expenditure_mm=("total_expenditure_mm", "median"),
            share_active=("n_tenders", lambda s: float((s > 0).mean())),
            n_active_buyers=("n_tenders", lambda s: int((s > 0).sum())),
        )
        .sort_values("quarter")
    )
    quarterly["n_buyers"] = int(panel["buyer_id"].nunique())
    quarterly["quarter_str"] = quarterly["quarter"].astype(str)
    quarterly.to_csv(tab_dir / "quarterly_avg_expenditure_per_municipality.csv", index=False)

    x = np.arange(len(quarterly))
    fig, ax = plt.subplots(figsize=(11, 5))
    ax.plot(
        x,
        quarterly["avg_expenditure_mm"],
        color="#1d4e89",
        marker="o",
        lw=2.2,
        ms=4.5,
    )

    if POST_QUARTER in set(quarterly["quarter"]):
        reform_idx = quarterly.index[quarterly["quarter"] == POST_QUARTER][0]
        ax.axvline(reform_idx - 0.5, color="#7a1c1c", lw=1.5, ls="--")
        ax.text(
            reform_idx - 0.45,
            float(quarterly["avg_expenditure_mm"].max()) * 1.03,
            REFORM_LABEL,
            color="#7a1c1c",
            fontsize=9,
            ha="left",
            va="bottom",
        )

    ax.set_title(
        f"{sample_label}: average expenditure per municipality by quarter",
        fontweight="bold",
    )
    ax.set_xlabel("Quarter")
    ax.set_ylabel("Average expenditure per municipality (million CLP)")
    ax.set_xticks(x)
    ax.set_xticklabels(quarterly["quarter_str"], rotation=45, ha="right")
    ax.yaxis.set_major_formatter(mtick.FuncFormatter(fmt_mm))
    ax.grid(axis="y", alpha=0.3)
    ax.spines[["top", "right"]].set_visible(False)
    savefig(fig_dir / "avg_expenditure_per_municipality_by_quarter.png")
    return quarterly


def plot_stacked_value_histogram(
    tender: pd.DataFrame,
    *,
    sample_label: str,
    sample_key: str,
    utm_upper: float | None,
    xline_utm: float | None = None,
    title_prefix: str | None = None,
    xlim: tuple[float, float] | None = None,
    use_log_x: bool | None = None,
    winsorize: bool = False,
    winsor_lower_q: float = 0.01,
    winsor_upper_q: float = 0.99,
    fig_dir: Path,
    tab_dir: Path,
) -> pd.DataFrame:
    sub = tender[["dataset", "monto_utm"]].dropna().copy()
    sub = sub[sub["monto_utm"] > 0].copy()

    winsor_note = ""
    hist_suffix = ""
    if winsorize:
        sub["monto_utm"], w_lo, w_hi = winsorize_series(
            sub["monto_utm"],
            lower_q=winsor_lower_q,
            upper_q=winsor_upper_q,
        )
        winsor_note = f"; winsorized p{int(winsor_lower_q*100)} to p{int(winsor_upper_q*100)}"
        hist_suffix = "_winsor_p01_p99"

    dataset_order = ["licitaciones", "compra_agil"]
    dataset_labels = {"licitaciones": "Licitaciones", "compra_agil": "Compra Agil"}
    dataset_colors = {"licitaciones": "#1d4e89", "compra_agil": "#c75b12"}

    if use_log_x is None:
        use_log_x = utm_upper is None

    if use_log_x:
        lower = max(float(sub["monto_utm"].min()), 1e-4)
        upper = float(xlim[1]) if xlim is not None else float(sub["monto_utm"].max())
        bins = np.geomspace(lower, upper, 45)
        xscale = "log"
        title_suffix = "log x-axis"
        hist_table = pd.DataFrame(
            {
                "bin_left_utm": bins[:-1],
                "bin_right_utm": bins[1:],
            }
        )
    else:
        if xlim is not None:
            linear_upper = float(xlim[1])
        elif utm_upper is not None:
            linear_upper = float(utm_upper)
        else:
            linear_upper = float(sub["monto_utm"].max())
        bins = np.linspace(0.0, linear_upper, 41)
        xscale = "linear"
        title_suffix = "linear x-axis"
        hist_table = pd.DataFrame(
            {
                "bin_left_utm": bins[:-1],
                "bin_right_utm": bins[1:],
            }
        )

    values = [sub.loc[sub["dataset"] == ds, "monto_utm"].to_numpy() for ds in dataset_order]

    fig, ax = plt.subplots(figsize=(11, 5.5))
    counts, _, _ = ax.hist(
        values,
        bins=bins,
        stacked=True,
        color=[dataset_colors[ds] for ds in dataset_order],
        label=[dataset_labels[ds] for ds in dataset_order],
        edgecolor="white",
        linewidth=0.25,
    )

    if xscale == "log":
        ax.set_xscale("log")

    ax.set_title(
        (
            f"{title_prefix}: stacked project-value histogram by mechanism ({title_suffix}{winsor_note})"
            if title_prefix
            else f"{sample_label}: stacked project-value histogram by mechanism ({title_suffix}{winsor_note})"
        ),
        fontweight="bold",
    )
    ax.set_xlabel("Estimated project value (UTM)")
    ax.set_ylabel("Tender count")
    if xline_utm is not None:
        ax.axvline(xline_utm, color="#7a1c1c", lw=1.6, ls="--")
        ymax = ax.get_ylim()[1]
        ax.text(
            xline_utm,
            ymax * 0.98,
            f"{xline_utm:,.0f} UTM",
            color="#7a1c1c",
            fontsize=9,
            ha="left",
            va="top",
        )
    if xlim is not None:
        ax.set_xlim(*xlim)
    ax.yaxis.set_major_formatter(mtick.FuncFormatter(lambda x, _pos: f"{int(x):,}"))
    ax.grid(axis="y", alpha=0.3)
    ax.spines[["top", "right"]].set_visible(False)
    ax.legend(frameon=False)
    savefig(fig_dir / f"stacked_project_value_histogram_utm{hist_suffix}.png")

    if len(counts) == 2:
        hist_table["licitaciones_count"] = counts[0]
        hist_table["compra_agil_count"] = counts[1] - counts[0]
        hist_table["stacked_total_count"] = counts[1]
    hist_table["sample_key"] = sample_key
    if winsorize:
        hist_table["winsor_lower_utm"] = w_lo
        hist_table["winsor_upper_utm"] = w_hi
    hist_table.to_csv(tab_dir / f"stacked_project_value_histogram_utm_bins{hist_suffix}.csv", index=False)
    return hist_table


def plot_log_value_histogram(
    tender: pd.DataFrame,
    *,
    title_prefix: str,
    xline_utm: float,
    xlim: tuple[float, float] | None = None,
    winsorize: bool = False,
    winsor_lower_q: float = 0.01,
    winsor_upper_q: float = 0.99,
    out_path: Path,
) -> None:
    sub = tender[["monto_utm"]].dropna().copy()
    sub = sub[sub["monto_utm"] > 0].copy()
    winsor_note = ""
    if winsorize:
        sub["monto_utm"], _w_lo, _w_hi = winsorize_series(
            sub["monto_utm"],
            lower_q=winsor_lower_q,
            upper_q=winsor_upper_q,
        )
        winsor_note = f" (winsorized p{int(winsor_lower_q*100)} to p{int(winsor_upper_q*100)})"
    sub["log_monto_utm"] = np.log(sub["monto_utm"])

    fig, ax = plt.subplots(figsize=(11, 5.5))
    ax.hist(
        sub["log_monto_utm"].to_numpy(),
        bins=45,
        color="#4c956c",
        edgecolor="white",
        linewidth=0.25,
        alpha=0.95,
    )
    xline = np.log(xline_utm)
    ax.axvline(xline, color="#7a1c1c", lw=1.6, ls="--")
    ymax = ax.get_ylim()[1]
    ax.text(
        xline,
        ymax * 0.98,
        f"log({xline_utm:,.0f})",
        color="#7a1c1c",
        fontsize=9,
        ha="left",
        va="top",
    )
    if xlim is not None:
        ax.set_xlim(*xlim)
    ax.set_title(f"{title_prefix}: log project-value histogram{winsor_note}", fontweight="bold")
    ax.set_xlabel("log(estimated project value in UTM)")
    ax.set_ylabel("Tender count")
    ax.yaxis.set_major_formatter(mtick.FuncFormatter(lambda x, _pos: f"{int(x):,}"))
    ax.grid(axis="y", alpha=0.3)
    ax.spines[["top", "right"]].set_visible(False)
    savefig(out_path)


def plot_log_stacked_value_histogram(
    tender: pd.DataFrame,
    *,
    title_prefix: str,
    sample_key: str,
    xlines_utm: list[float],
    xlim: tuple[float, float] | None = None,
    winsorize: bool = False,
    winsor_lower_q: float = 0.01,
    winsor_upper_q: float = 0.99,
    name_prefix: str = "",
    fig_dir: Path,
    tab_dir: Path,
) -> pd.DataFrame:
    sub = tender[["dataset", "monto_utm"]].dropna().copy()
    sub = sub[sub["monto_utm"] > 0].copy()

    winsor_note = ""
    hist_suffix = ""
    if winsorize:
        sub["monto_utm"], w_lo, w_hi = winsorize_series(
            sub["monto_utm"],
            lower_q=winsor_lower_q,
            upper_q=winsor_upper_q,
        )
        winsor_note = f" (winsorized p{int(winsor_lower_q*100)} to p{int(winsor_upper_q*100)})"
        hist_suffix = "_winsor_p01_p99"

    sub["log_monto_utm"] = np.log(sub["monto_utm"])

    dataset_order = ["licitaciones", "compra_agil"]
    dataset_labels = {"licitaciones": "Licitaciones", "compra_agil": "Compra Agil"}
    dataset_colors = {"licitaciones": "#1d4e89", "compra_agil": "#c75b12"}
    values = [sub.loc[sub["dataset"] == ds, "log_monto_utm"].to_numpy() for ds in dataset_order]

    if xlim is None:
        xmin = float(sub["log_monto_utm"].min())
        xmax = float(sub["log_monto_utm"].max())
    else:
        xmin, xmax = xlim
    bins = np.linspace(xmin, xmax, 45)

    fig, ax = plt.subplots(figsize=(11, 5.5))
    counts, edges, _ = ax.hist(
        values,
        bins=bins,
        stacked=True,
        color=[dataset_colors[ds] for ds in dataset_order],
        label=[dataset_labels[ds] for ds in dataset_order],
        edgecolor="white",
        linewidth=0.25,
    )
    ymax = ax.get_ylim()[1]
    for xline_utm in xlines_utm:
        xline = np.log(xline_utm)
        ax.axvline(xline, color="#7a1c1c", lw=1.6, ls="--")
        ax.text(
            xline,
            ymax * 0.98,
            f"{xline_utm:,.0f} UTM",
            color="#7a1c1c",
            fontsize=9,
            ha="left",
            va="top",
        )
    if xlim is not None:
        ax.set_xlim(*xlim)
    ax.set_title(f"{title_prefix}: stacked log project-value histogram by mechanism{winsor_note}", fontweight="bold")
    ax.set_xlabel("log(estimated project value in UTM)")
    ax.set_ylabel("Tender count")
    ax.yaxis.set_major_formatter(mtick.FuncFormatter(lambda x, _pos: f"{int(x):,}"))
    ax.grid(axis="y", alpha=0.3)
    ax.spines[["top", "right"]].set_visible(False)
    ax.legend(frameon=False)
    savefig(fig_dir / f"{name_prefix}log_stacked_project_value_histogram_utm{hist_suffix}.png")

    hist_table = pd.DataFrame(
        {
            "bin_left_log_utm": edges[:-1],
            "bin_right_log_utm": edges[1:],
        }
    )
    if len(counts) == 2:
        hist_table["licitaciones_count"] = counts[0]
        hist_table["compra_agil_count"] = counts[1] - counts[0]
        hist_table["stacked_total_count"] = counts[1]
    hist_table["sample_key"] = sample_key
    hist_table["xlines_utm"] = ",".join(str(v) for v in xlines_utm)
    if winsorize:
        hist_table["winsor_lower_utm"] = w_lo
        hist_table["winsor_upper_utm"] = w_hi
    hist_table.to_csv(tab_dir / f"{name_prefix}log_stacked_project_value_histogram_utm_bins{hist_suffix}.csv", index=False)
    return hist_table


def plot_event_study(
    panel: pd.DataFrame,
    *,
    outcome: str,
    outcome_label: str,
    sample_label: str,
    ref_t: int = -1,
    min_event_time: int = -8,
    fig_dir: Path,
    tab_dir: Path,
    filename: str,
) -> pd.DataFrame:
    """Event study relative to 2025Q1 reform.

    FE: buyer_id + quarter_of_year (1–4 seasonal dummies).
    Calendar-quarter FEs are collinear with event-time dummies under universal
    treatment timing, so seasonal FEs are used instead.
    Reference period: ref_t (default -1 = 2024Q4).
    Periods earlier than min_event_time are binned together.
    """
    reform_ord = pd.Period("2025Q1", freq="Q").ordinal

    df = panel.copy()
    df["event_time"] = df["quarter"].apply(lambda q: q.ordinal - reform_ord)
    df["quarter_of_year"] = df["quarter"].apply(lambda q: q.quarter)
    df.loc[df["event_time"] < min_event_time, "event_time"] = min_event_time

    df = df[["buyer_id", "event_time", "quarter_of_year", outcome]].dropna(subset=[outcome]).copy()

    event_times = sorted(df["event_time"].unique())

    dummy_info: list[tuple[int, str]] = []
    for t in event_times:
        if t == ref_t:
            continue
        col = f"et_p{t}" if t >= 0 else f"et_m{abs(t)}"
        df[col] = (df["event_time"] == t).astype(int)
        dummy_info.append((t, col))

    if not dummy_info:
        print(f"  No event-time variation for {outcome}, skipping.")
        return pd.DataFrame()

    dummy_vars = " + ".join(c for _, c in dummy_info)
    formula = f"{outcome} ~ {dummy_vars} | buyer_id + quarter_of_year"

    print(f"  Event study [{outcome}]: {len(df):,} obs, {df['buyer_id'].nunique():,} buyers")
    res = pf.feols(formula, data=df, vcov={"CRV1": "buyer_id"})
    coef = res.coef()
    se = res.se()

    rows: list[dict] = [{"event_time": ref_t, "coef": 0.0, "se": 0.0}]
    for t, col in dummy_info:
        if col in coef.index:
            b, s = float(coef[col]), float(se[col])
            rows.append({"event_time": t, "coef": b, "se": s})

    plot_df = pd.DataFrame(rows).sort_values("event_time").reset_index(drop=True)
    plot_df["ci_lo"] = plot_df["coef"] - 1.96 * plot_df["se"]
    plot_df["ci_hi"] = plot_df["coef"] + 1.96 * plot_df["se"]
    plot_df.to_csv(tab_dir / f"{filename}.csv", index=False)

    fig, ax = plt.subplots(figsize=(11, 5))
    ax.axhline(0, color="gray", lw=0.8, zorder=0)
    ax.axvline(-0.5, color="#7a1c1c", lw=1.5, ls="--", zorder=1)
    ax.fill_between(
        plot_df["event_time"], plot_df["ci_lo"], plot_df["ci_hi"],
        alpha=0.15, color="#0b6e4f", zorder=2,
    )
    ax.plot(
        plot_df["event_time"], plot_df["coef"],
        color="#0b6e4f", marker="o", lw=2.2, ms=4.5, zorder=3,
    )

    yrange = float(plot_df["ci_hi"].max()) - float(plot_df["ci_lo"].min())
    ymax_text = float(plot_df["ci_hi"].max()) + yrange * 0.04
    ax.text(-0.45, ymax_text, REFORM_LABEL, color="#7a1c1c", fontsize=9, ha="left", va="bottom")

    xticks = sorted(plot_df["event_time"].unique())
    xlabels = [f"≤{t}" if t == min_event_time else str(t) for t in xticks]
    ax.set_xticks(xticks)
    ax.set_xticklabels(xlabels, rotation=45, ha="right")

    ax.set_xlabel("Quarter relative to reform (2025Q1 = 0)")
    ax.set_ylabel(f"{outcome_label}, relative to 2024Q4")
    ax.set_title(
        f"{sample_label}: event study — {outcome_label}\n"
        "(buyer FE + seasonal quarter FE, SE clustered by buyer)",
        fontweight="bold",
    )
    ax.yaxis.set_major_formatter(mtick.FuncFormatter(lambda x, _pos: f"{x:,.2f}"))
    ax.grid(axis="y", alpha=0.3)
    ax.spines[["top", "right"]].set_visible(False)
    savefig(fig_dir / f"{filename}.png")
    return plot_df


def run_regressions(
    tender: pd.DataFrame,
    panel: pd.DataFrame,
    *,
    sample_key: str,
    tab_dir: Path,
) -> pd.DataFrame:
    rows: list[dict[str, float | int | str]] = []

    tender_specs = [
        ("Tender-level project size (mm CLP)", "monto_estimado_mm", tender),
        ("Tender-level log project size", "log_monto_estimado", tender),
    ]
    for label, outcome, sample in tender_specs:
        sub = sample[["buyer_id", "quarter", "post", outcome]].dropna().copy()
        print(f"  {sample_key} | {label}: n={len(sub):,}")
        res = pf.feols(f"{outcome} ~ post | buyer_id", data=sub, vcov={"CRV1": "buyer_id"})
        rows.append(extract_fe_result(res, sample_key=sample_key, spec=label, outcome=outcome, sample=sub))

    panel_specs = [
        (
            "Municipality-quarter avg project size (mm CLP)",
            "avg_project_size_mm",
            panel.dropna(subset=["avg_project_size_mm"]).copy(),
        ),
        (
            "Municipality-quarter expenditure (mm CLP)",
            "total_expenditure_mm",
            panel.copy(),
        ),
        (
            "Municipality-quarter log(1 + expenditure)",
            "log1p_total_expenditure",
            panel.copy(),
        ),
    ]
    for label, outcome, sample in panel_specs:
        sub = sample[["buyer_id", "quarter", "post", outcome]].dropna().copy()
        print(f"  {sample_key} | {label}: n={len(sub):,}")
        res = pf.feols(f"{outcome} ~ post | buyer_id", data=sub, vcov={"CRV1": "buyer_id"})
        rows.append(extract_fe_result(res, sample_key=sample_key, spec=label, outcome=outcome, sample=sub))

    out = pd.DataFrame(rows)
    out["post_stars"] = np.select(
        [out["p_post"] < 0.01, out["p_post"] < 0.05, out["p_post"] < 0.10],
        ["***", "**", "*"],
        default="",
    )
    out.to_csv(tab_dir / "regression_results.csv", index=False)
    return out


def run_sample(
    tender_all: pd.DataFrame,
    *,
    sample_key: str,
    sample_label: str,
    utm_upper: float | None,
) -> tuple[pd.DataFrame, dict[str, float | int | str]]:
    fig_dir, tab_dir = ensure_sample_dirs(sample_key)

    print("\n" + "=" * 70)
    print(f"STEP 2+ — Running sample: {sample_label}")
    print("=" * 70)

    tender = subset_tender(tender_all, utm_upper)
    if tender.empty:
        raise ValueError(f"No rows left in sample {sample_key}")

    print(f"  Tenders: {len(tender):,}")
    print(f"  Buyers: {tender['buyer_id'].nunique():,}")
    print(f"  Mean UTM: {tender['monto_utm'].mean():.2f}")
    print(f"  Quarter span: {tender['quarter'].min()} to {tender['quarter'].max()}")
    print("  Dataset mix:")
    print(tender["dataset"].value_counts(dropna=False).to_string())

    tender.to_csv(tab_dir / "tender_sample.csv", index=False)

    panel = build_municipality_quarter_panel(tender, tab_dir)
    plot_stacked_value_histogram(
        tender,
        sample_label=sample_label,
        sample_key=sample_key,
        utm_upper=utm_upper,
        fig_dir=fig_dir,
        tab_dir=tab_dir,
    )
    plot_avg_project_size(tender, sample_label=sample_label, fig_dir=fig_dir, tab_dir=tab_dir)
    plot_avg_expenditure(panel, sample_label=sample_label, fig_dir=fig_dir, tab_dir=tab_dir)
    plot_event_study(
        panel,
        outcome="avg_project_size_mm",
        outcome_label="Avg project size (mill. CLP)",
        sample_label=sample_label,
        fig_dir=fig_dir,
        tab_dir=tab_dir,
        filename="event_study_avg_project_size",
    )
    plot_event_study(
        panel,
        outcome="n_tenders",
        outcome_label="N tenders",
        sample_label=sample_label,
        fig_dir=fig_dir,
        tab_dir=tab_dir,
        filename="event_study_n_tenders",
    )
    reg = run_regressions(tender, panel, sample_key=sample_key, tab_dir=tab_dir)

    summary = {
        "sample_key": sample_key,
        "sample_label": sample_label,
        "utm_upper": utm_upper if utm_upper is not None else "",
        "n_tenders": len(tender),
        "n_buyers": int(tender["buyer_id"].nunique()),
        "n_quarters": int(tender["quarter"].nunique()),
        "mean_monto_utm": float(tender["monto_utm"].mean()),
        "median_monto_utm": float(tender["monto_utm"].median()),
        "mean_project_size_mm_pre": float(tender.loc[tender["post"] == 0, "monto_estimado_mm"].mean()),
        "mean_project_size_mm_post": float(tender.loc[tender["post"] == 1, "monto_estimado_mm"].mean()),
    }
    return reg, summary


def run_period_split(tender_all: pd.DataFrame) -> None:
    """Pre/post Dec-2024 log-stacked histograms under municipal_quarterly_reform/period_split/."""
    split_date = pd.Timestamp("2025-01-01")
    out_dir = OUT_DIR / "period_split"
    out_dir.mkdir(parents=True, exist_ok=True)

    periods = [
        ("pre_dec2024",  "Pre Dec 2024",  tender_all[tender_all["fecha_pub"] < split_date]),
        ("post_dec2024", "Post Dec 2024", tender_all[tender_all["fecha_pub"] >= split_date]),
    ]

    print("\n" + "=" * 70)
    print("STEP — Period split histograms (pre/post Dec 2024)")
    print("=" * 70)

    for key, label, subset in periods:
        print(f"  {label}: {len(subset):,} tenders")
        for winsorize in (False, True):
            plot_log_stacked_value_histogram(
                subset,
                title_prefix=label,
                sample_key=key,
                xlines_utm=[30, 100],
                winsorize=winsorize,
                name_prefix=f"{key}_",
                fig_dir=out_dir,
                tab_dir=out_dir,
            )


def main() -> None:
    tender_all = load_tender_level_sample()

    run_period_split(tender_all)

    regression_tables: list[pd.DataFrame] = []
    sample_rows: list[dict[str, float | int | str]] = []

    for sample_key, sample_label, utm_upper in SAMPLES:
        reg, summary = run_sample(
            tender_all,
            sample_key=sample_key,
            sample_label=sample_label,
            utm_upper=utm_upper,
        )
        regression_tables.append(reg)
        sample_rows.append(summary)

    sample_summary = pd.DataFrame(sample_rows)
    sample_summary.to_csv(OUT_DIR / "sample_summary.csv", index=False)

    reg_all = pd.concat(regression_tables, ignore_index=True)
    reg_all.to_csv(OUT_DIR / "regression_results_all_samples.csv", index=False)

    display_cols = ["sample_key", "spec", "coef_post", "se_post", "p_post", "post_stars", "n_obs", "n_buyers"]
    print("\n" + "=" * 70)
    print("FINAL REGRESSION SUMMARY")
    print("=" * 70)
    print(reg_all[display_cols].to_string(index=False))
    print("\nDone.")


if __name__ == "__main__":
    main()
