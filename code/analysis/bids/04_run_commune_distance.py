"""
04_run_commune_distance.py
──────────────────────────────────────────────────────────────────────────────
Bid-markup regressions using commune-level (municipality) distances.

Motivation:  The existing specs (2d–2f) use region-centroid distances, which
produces only 16 distinct distance values for non-local bids and conflates the
distance gradient (δ) with the local-advantage discontinuity (λ).  Commune-
level distances give ~346 distinct values and — crucially — create within-
region variation for *local* bids (same region, different commune), which
identifies δ independently of the local/non-local boundary.

Specs produced (mirroring existing 2a–2f naming convention; suffix _com):
  (3a_com) local dummy + bidder FE + month FE
           → replicates spec 2b with a same-region local flag
  (3b_com) log(dist_km_com) on ALL bids (local + non-local) + bidder FE + month FE
           → identifies δ from *within-region* distance variation among local
             bids, without requiring the non-local sample
  (3c_com) log(dist_km_com) on non-local bids only + bidder FE + month FE
           → direct analogue of spec 2d; compare slope to region-level estimate
  (3f_com) local dummy + log(dist_km_com) on ALL bids + bidder FE + month FE
           → direct joint estimate of the discrete local premium λ and the
             continuous distance gradient δ in the matched commune sample
  (3d_com) distance bins (non-local bids only) + bidder FE + month FE
           → fine-grained analogue of spec 2e with 5 bins: 1-50, 50-150,
             150-300, 300-600, 600+ km  (ref: 1-50 km non-local)
  (3e_com) distance bins (ALL bids) + bidder FE + month FE
           → combines local within-region distance variation with non-local;
             bin "0" (same commune) is the reference

Outputs
  output/bids/tables/bids_commune_distance.csv
  output/bids/tables/bids_commune_distance.tex
  output/bids/figures/bids_commune_distance_gradient.png
  output/bids/figures/bids_commune_bins_comparison.png
"""

from __future__ import annotations

import argparse
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

# ── Paths ─────────────────────────────────────────────────────────────────────
HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]

OUT_BIDS     = ROOT / "output" / "bids"
OUT_BIDS_TBL = OUT_BIDS / "tables"
OUT_BIDS_FIG = OUT_BIDS / "figures"
for _d in [OUT_BIDS_TBL, OUT_BIDS_FIG]:
    _d.mkdir(parents=True, exist_ok=True)

CI_Z = 1.96
MIN_BIDS_FIRM = 3
SPEC_ORDER = [
    "(3a_com) local, bidder+month FE",
    "(3b_com) log dist, all bids",
    "(3c_com) log dist, non-local",
    "(3f_com) local + log dist, all bids",
    "(3d_com) commune bins, non-local",
    "(3e_com) commune bins, all bids",
]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run commune-distance bid regressions.")
    parser.add_argument(
        "--sample",
        choices=["all", "municipalidades", "obras"],
        default="all",
        help="Sample restriction on buyer sector.",
    )
    return parser.parse_args()


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


ARGS = _parse_args()
SAMPLE = ARGS.sample
SAMPLE_SUFFIX_STR = SAMPLE_SUFFIX[SAMPLE]

# ── Load sample ───────────────────────────────────────────────────────────────
print("Loading bid analysis sample …")
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
        raise FileNotFoundError(
            f"{sample_path.name} not found. Re-run 01_build_bid_sample.py first."
        )

bid = pd.read_parquet(sample_path)
if filter_in_memory:
    bid = _filter_sector_sample(bid, SAMPLE)
print(f"  {len(bid):,} bids, {bid.shape[1]} columns")
print(f"  Sample: {SAMPLE}")

# Check commune distance availability
n_com = bid["dist_km_com"].notna().sum()
print(f"  Commune distance non-null: {n_com:,} ({n_com/len(bid):.1%})")

if n_com == 0:
    raise RuntimeError(
        "dist_km_com is entirely null. "
        "Re-run 01_build_bid_sample.py with comunas_centroids.csv present."
    )

# ── Restrict to pre-reform, to keep sample comparable to existing specs ───────
# (Existing specs 2a-2f use the full sample; we do likewise but note the option)
# bid = bid[bid["k_rel"] < 0].copy()  # uncomment to restrict to pre-reform only

# Drop rows with missing outcome
bid = bid[bid["log_bid_ratio"].notna() & np.isfinite(bid["log_bid_ratio"])].copy()
print(f"  After outcome filter: {len(bid):,}")

# Ensure FE columns are string categories
for col in ["bidder_id_str", "year_month_str", "buyer_region_norm"]:
    bid[col] = bid[col].astype("category")

# Add same-commune flag (distance == 0)
bid["same_commune"] = (bid["dist_km_com"] == 0).astype("Int8")


# ── Helpers ───────────────────────────────────────────────────────────────────
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


def _save_tex_table(results_df: pd.DataFrame, path: Path) -> None:
    if results_df.empty:
        raise RuntimeError("No commune-distance regression results available for TeX export.")

    col_headers = {
        "(3a_com) local, bidder+month FE": "(3a_com)",
        "(3b_com) log dist, all bids": "(3b_com)",
        "(3c_com) log dist, non-local": "(3c_com)",
        "(3f_com) local + log dist, all bids": "(3f_com)",
        "(3d_com) commune bins, non-local": "(3d_com)",
        "(3e_com) commune bins, all bids": "(3e_com)",
    }
    coef_rows = [
        ("local_within", "Local (same region)"),
        ("log_dist_km_com_f", "log(dist km)"),
        ("dist_bin_50_150", "50--150 km (non-local)"),
        ("dist_bin_150_300", "150--300 km (non-local)"),
        ("dist_bin_300_600", "300--600 km (non-local)"),
        ("dist_bin_600plus", "600+ km (non-local)"),
        ("dist_bin_all_1_50", "1--50 km (all bids)"),
        ("dist_bin_all_50_150", "50--150 km (all bids)"),
        ("dist_bin_all_150_300", "150--300 km (all bids)"),
        ("dist_bin_all_300_600", "300--600 km (all bids)"),
        ("dist_bin_all_600plus", "600+ km (all bids)"),
    ]

    tidy = {
        spec: results_df.loc[results_df["spec"] == spec].set_index("Coefficient")
        for spec in SPEC_ORDER
    }
    lines = [
        r"\begin{tabular}{l" + "c" * len(SPEC_ORDER) + "}",
        r"\toprule",
        " & " + " & ".join(_escape_tex(col_headers[spec]) for spec in SPEC_ORDER) + r" \\",
        r"\midrule",
    ]

    for coef_name, coef_label in coef_rows:
        est_cells = [_escape_tex(coef_label)]
        se_cells = [""]
        for spec in SPEC_ORDER:
            spec_df = tidy[spec]
            if coef_name in spec_df.index:
                row = spec_df.loc[coef_name]
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
        + " & ".join(f"{int(tidy[spec]['nobs'].iloc[0]):,}" if not tidy[spec].empty else "" for spec in SPEC_ORDER)
        + r" \\",
        "$R^2$ & "
        + " & ".join(f"{float(tidy[spec]['r2'].iloc[0]):.3f}" if not tidy[spec].empty else "" for spec in SPEC_ORDER)
        + r" \\",
        "Fixed effects & "
        + " & ".join(_escape_tex(str(tidy[spec]['fe'].iloc[0])) if not tidy[spec].empty else "" for spec in SPEC_ORDER)
        + r" \\",
        "Clusters & "
        + " & ".join(f"{int(tidy[spec]['n_clusters'].iloc[0]):,}" if not tidy[spec].empty else "" for spec in SPEC_ORDER)
        + r" \\",
        r"\bottomrule",
        r"\end{tabular}",
    ])

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Saved: {path}")


def _fit(data, outcome, regressors, absorb, cluster, spec, fe_label):
    cols_needed = list(dict.fromkeys([outcome] + regressors + absorb + [cluster]))
    df = data[cols_needed].dropna()
    if len(df) < 500:
        print(f"  [WARN] {spec}: only {len(df)} obs after dropna, skipping")
        return []

    Y  = df[[outcome]]
    X  = df[regressors]
    Ab = df[absorb]
    Cl = df[cluster]

    mod = AbsorbingLS(Y, X, absorb=Ab, drop_absorbed=True)
    try:
        res = mod.fit(cov_type="clustered", clusters=Cl.astype(str).to_numpy())
    except Exception as e:
        print(f"  [WARN] {spec}: fit failed: {e}")
        return []

    rows = []
    for var in res.params.index:
        rows.append({
            "Coefficient": var,
            "Estimate":    res.params[var],
            "Std. Error":  res.std_errors[var],
            "t value":     res.tstats[var],
            "Pr(>|t|)":    res.pvalues[var],
            "CI Low":      res.params[var] - CI_Z * res.std_errors[var],
            "CI High":     res.params[var] + CI_Z * res.std_errors[var],
            "spec":        spec,
            "nobs":        int(res.nobs),
            "r2":          float(res.rsquared),
            "fe":          fe_label,
            "cluster":     cluster,
            "n_clusters":  int(Cl.nunique()),
        })
    return rows


# ── Spec (3a_com): local (same-region) dummy ─────────────────────────────────
print("\nSpec (3a_com): local dummy, bidder+month FE …")
rows_all = []
rows_all += _fit(
    bid.assign(local_within=bid["local"].astype(float)),
    outcome="log_bid_ratio",
    regressors=["local_within"],
    absorb=["bidder_id_str", "year_month_str"],
    cluster="bidder_id_str",
    spec="(3a_com) local, bidder+month FE",
    fe_label="Bidder + Month FE",
)

# ── Spec (3b_com): log commune distance on ALL bids ───────────────────────────
print("Spec (3b_com): log(dist_km_com) all bids, bidder+month FE …")
sub_all = bid[bid["dist_km_com"].notna()].copy()
sub_all["log_dist_km_com_f"] = sub_all["log_dist_km_com"].astype(float)
rows_all += _fit(
    sub_all,
    outcome="log_bid_ratio",
    regressors=["log_dist_km_com_f"],
    absorb=["bidder_id_str", "year_month_str"],
    cluster="bidder_id_str",
    spec="(3b_com) log dist, all bids",
    fe_label="Bidder + Month FE",
)

# ── Spec (3c_com): log commune distance on NON-LOCAL bids only ────────────────
print("Spec (3c_com): log(dist_km_com) non-local bids, bidder+month FE …")
sub_nonloc = bid[(bid["same_region"] == 0) & bid["dist_km_com"].notna()].copy()
sub_nonloc["log_dist_km_com_f"] = sub_nonloc["log_dist_km_com"].astype(float)
rows_all += _fit(
    sub_nonloc,
    outcome="log_bid_ratio",
    regressors=["log_dist_km_com_f"],
    absorb=["bidder_id_str", "year_month_str"],
    cluster="bidder_id_str",
    spec="(3c_com) log dist, non-local",
    fe_label="Bidder + Month FE",
)

# ── Spec (3f_com): local + log commune distance on ALL bids ───────────────────
print("Spec (3f_com): local + log(dist_km_com) all bids, bidder+month FE …")
sub_joint = bid[bid["dist_km_com"].notna()].copy()
sub_joint["local_within"] = sub_joint["local"].astype(float)
sub_joint["log_dist_km_com_f"] = sub_joint["log_dist_km_com"].astype(float)
rows_all += _fit(
    sub_joint,
    outcome="log_bid_ratio",
    regressors=["local_within", "log_dist_km_com_f"],
    absorb=["bidder_id_str", "year_month_str"],
    cluster="bidder_id_str",
    spec="(3f_com) local + log dist, all bids",
    fe_label="Bidder + Month FE",
)

# ── Spec (3d_com): distance BINS on non-local bids (ref = 1-50 km) ────────────
print("Spec (3d_com): commune distance bins, non-local bids, bidder+month FE …")
BIN_LABELS_NONLOC = ["50-150", "150-300", "300-600", "600+"]   # ref = "1-50"
sub_bins_nonloc = bid[
    (bid["same_region"] == 0) &
    bid["dist_bin_com"].isin(["1-50"] + BIN_LABELS_NONLOC)
].copy()
for lab in BIN_LABELS_NONLOC:
    sub_bins_nonloc[f"dist_bin_{lab.replace('-','_').replace('+','plus')}"] = \
        (sub_bins_nonloc["dist_bin_com"] == lab).astype(float)

bin_regressors_nonloc = [
    f"dist_bin_{lab.replace('-','_').replace('+','plus')}"
    for lab in BIN_LABELS_NONLOC
]
rows_all += _fit(
    sub_bins_nonloc,
    outcome="log_bid_ratio",
    regressors=bin_regressors_nonloc,
    absorb=["bidder_id_str", "year_month_str"],
    cluster="bidder_id_str",
    spec="(3d_com) commune bins, non-local",
    fe_label="Bidder + Month FE",
)

# ── Spec (3e_com): distance BINS on ALL bids (ref = 0, same commune) ──────────
print("Spec (3e_com): commune distance bins, all bids, bidder+month FE …")
ALL_BIN_LABELS = ["1-50", "50-150", "150-300", "300-600", "600+"]  # ref = "0"
sub_bins_all = bid[
    bid["dist_bin_com"].isin(["0"] + ALL_BIN_LABELS) &
    bid["dist_km_com"].notna()
].copy()
for lab in ALL_BIN_LABELS:
    sub_bins_all[f"dist_bin_all_{lab.replace('-','_').replace('+','plus')}"] = \
        (sub_bins_all["dist_bin_com"] == lab).astype(float)

bin_regressors_all = [
    f"dist_bin_all_{lab.replace('-','_').replace('+','plus')}"
    for lab in ALL_BIN_LABELS
]
rows_all += _fit(
    sub_bins_all,
    outcome="log_bid_ratio",
    regressors=bin_regressors_all,
    absorb=["bidder_id_str", "year_month_str"],
    cluster="bidder_id_str",
    spec="(3e_com) commune bins, all bids",
    fe_label="Bidder + Month FE",
)

# ── Save results ──────────────────────────────────────────────────────────────
results_df = pd.DataFrame(rows_all)
if results_df.empty:
    raise RuntimeError("All commune-distance regressions failed; no outputs were generated.")
out_csv = OUT_BIDS_TBL / f"bids_commune_distance{SAMPLE_SUFFIX_STR}.csv"
results_df.to_csv(out_csv, index=False)
print(f"\nSaved: {out_csv}")
_save_tex_table(results_df, OUT_BIDS_TBL / f"bids_commune_distance{SAMPLE_SUFFIX_STR}.tex")

# ── Figures ───────────────────────────────────────────────────────────────────

# Figure 1: Log-distance gradient comparison (region-level 2d vs commune-level 3c)
fig, axes = plt.subplots(1, 2, figsize=(11, 4.5), sharey=False)

for ax, (spec_label, col_label, color) in zip(axes, [
    ("(2d) non-local dist",         "Region-level\nlog(dist_km)",     "#2166ac"),
    ("(3c_com) log dist, non-local","Commune-level\nlog(dist_km_com)","#d6604d"),
]):
    row = results_df.loc[results_df["spec"] == spec_label].copy()
    if len(row) == 0:
        # Fall back to existing CSV for region-level
        try:
            existing = pd.read_csv(OUT_BIDS_TBL / f"bids_part2_firm_fe{SAMPLE_SUFFIX_STR}.csv")
            row = existing[existing["spec"] == spec_label]
        except Exception:
            pass

    if len(row):
        b  = float(row["Estimate"].iloc[0])
        lo = float(row["CI Low"].iloc[0])
        hi = float(row["CI High"].iloc[0])
        n  = int(row["nobs"].iloc[0])
        ax.barh(0, b, color=color, alpha=0.8, height=0.4)
        ax.errorbar(b, 0, xerr=[[b - lo], [hi - b]], fmt="none",
                    color="black", capsize=5, linewidth=1.5)
        ax.axvline(0, color="black", linewidth=0.8, linestyle="--")
        ax.set_yticks([])
        ax.set_xlabel("Coefficient on log(distance)")
        ax.set_title(f"{col_label}\nβ = {b:.3f}  [{lo:.3f}, {hi:.3f}]\n"
                     f"N = {n:,}", fontsize=10)
    else:
        ax.set_title(f"{col_label}\n(no results yet)", fontsize=10)

fig.suptitle("Distance gradient in bid markups: region vs. commune level\n"
             "(Non-local bids; bidder + month FE; clustered SE)", fontsize=11)
fig.tight_layout()
fig.savefig(OUT_BIDS_FIG / f"bids_commune_distance_gradient{SAMPLE_SUFFIX_STR}.png", dpi=150, bbox_inches="tight")
plt.close()
print(f"Saved: bids_commune_distance_gradient{SAMPLE_SUFFIX_STR}.png")

# Figure 2: Bin comparison — region-level 2e vs commune-level 3d (non-local)
fig, axes = plt.subplots(1, 2, figsize=(13, 5))

# Left: existing region-level bins (spec 2e)
try:
    reg_bins = pd.read_csv(OUT_BIDS_TBL / f"bids_part2_firm_fe{SAMPLE_SUFFIX_STR}.csv")
    reg_bins = reg_bins[reg_bins["spec"] == "(2e) non-local bins"].copy()
    reg_bins["mid"] = reg_bins["Coefficient"].map({
        "dist_bin_150_400": 275, "dist_bin_400_800": 600, "dist_bin_800_plus": 900
    })
    reg_bins = reg_bins.dropna(subset=["mid"]).sort_values("mid")
    axes[0].errorbar(
        reg_bins["mid"], reg_bins["Estimate"],
        yerr=[reg_bins["Estimate"] - reg_bins["CI Low"],
              reg_bins["CI High"] - reg_bins["Estimate"]],
        fmt="o-", color="#2166ac", capsize=5, linewidth=1.5, markersize=7,
    )
    axes[0].axhline(0, color="black", linewidth=0.8, linestyle="--")
    axes[0].set_xlabel("Distance midpoint (km)")
    axes[0].set_ylabel("Coefficient vs. ref. bin")
    axes[0].set_title("Spec (2e): Region-level bins\n"
                      "Ref = 0–150 km  (non-local bids)", fontsize=10)
except Exception as e:
    axes[0].set_title(f"Region bins not available\n({e})", fontsize=9)

# Right: commune-level bins (spec 3d_com)
com_bin_spec = results_df[results_df["spec"] == "(3d_com) commune bins, non-local"].copy()
if len(com_bin_spec):
    bin_mids = {
        "dist_bin_50_150":  100, "dist_bin_150_300": 225,
        "dist_bin_300_600": 450, "dist_bin_600plus": 750,
    }
    com_bin_spec["mid"] = com_bin_spec["Coefficient"].map(bin_mids)
    com_bin_spec = com_bin_spec.dropna(subset=["mid"]).sort_values("mid")
    axes[1].errorbar(
        com_bin_spec["mid"], com_bin_spec["Estimate"],
        yerr=[com_bin_spec["Estimate"] - com_bin_spec["CI Low"],
              com_bin_spec["CI High"] - com_bin_spec["Estimate"]],
        fmt="s-", color="#d6604d", capsize=5, linewidth=1.5, markersize=7,
    )
    axes[1].axhline(0, color="black", linewidth=0.8, linestyle="--")
    axes[1].set_xlabel("Distance midpoint (km)")
    axes[1].set_ylabel("Coefficient vs. ref. bin")
    axes[1].set_title("Spec (3d_com): Commune-level bins\n"
                      "Ref = 1–50 km  (non-local bids)", fontsize=10)
else:
    axes[1].set_title("Commune bins not yet available", fontsize=10)

fig.suptitle("Distance bins: region-centroid vs. commune-centroid\n"
             "(Non-local bids; bidder + month FE; clustered SE)", fontsize=11)
fig.tight_layout()
fig.savefig(OUT_BIDS_FIG / f"bids_commune_bins_comparison{SAMPLE_SUFFIX_STR}.png", dpi=150, bbox_inches="tight")
plt.close()
print(f"Saved: bids_commune_bins_comparison{SAMPLE_SUFFIX_STR}.png")

print("\nDone.")
