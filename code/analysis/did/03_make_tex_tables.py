"""
03_make_tex_tables.py
─────────────────────────────────────────────────────────────────────────────
Read DiD estimation results and produce publication-style LaTeX tables
suitable for inclusion in the Compra Ágil beamer deck.

Produces single-spec tables (from did_results_all.csv):
  did_table_panel_A.tex   (Entry)
  did_table_panel_B.tex   (Composition)
  did_table_panel_D.tex   (Costs / Process)
  did_table_combined.tex  (all panels)

Produces side-by-side comparison tables (cl = 0–30 UTM ctrl, ch = 100–200 UTM ctrl):
  did_table_compare_A.tex   (Entry, two-control comparison)
  did_table_compare_B.tex   (Composition, two-control comparison)
  did_table_compare_CD.tex  (Costs & Process, two-control comparison)
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from did_utils import OUT_TABLES

# ── Outcome ordering & labels ─────────────────────────────────────────────────
# (outcome_col, display_label, panel)
OUTCOME_ORDER = [
    # Panel A — Entry at Bidding Stage
    ("n_bidders",            "N bidders",                    "A"),
    ("n_local",              "N local bidders",              "A"),
    ("n_nonlocal",           "N non-local bidders",          "A"),
    ("n_sme_bidders",        "N SME bidders",                "A"),
    ("n_large_bidders",      "N large firm bidders",         "A"),
    ("n_nonsii_bidders",     "N non-SII bidders",            "A"),
    ("single_bidder",        "Pr(single-bidder tender)",     "A"),
    # Panel B — Bidder Composition
    ("any_sme_sii",          "Any SME bidder",               "B"),
    ("sme_share_sii",        r"\% bidders: SME",             "B"),
    ("share_large_bidders",  r"\% bidders: large firm",      "B"),
    ("share_bidders_not_in_sii", r"\% bidders: not in SII",  "B"),
    ("share_sme_local_bidders", r"\% bidders: SME$\times$local", "B"),
    ("share_large_local_bidders", r"\% bidders: large$\times$local", "B"),
    ("share_nonsii_local_bidders", r"\% bidders: non-SII$\times$local", "B"),
    # Panel C — Winner Characteristics
    ("winner_is_sme_sii",    "Pr(winner: SME)",              "C"),
    ("winner_is_large",      "Pr(winner: large firm)",       "C"),
    ("winner_not_in_sii",    "Pr(winner: non-SII)",          "C"),
    ("winner_is_sme_local",  "Pr(winner: SME \\& local)",    "C"),
    ("winner_is_large_local", "Pr(winner: large \\& local)", "C"),
    ("winner_is_nonsii_local", "Pr(winner: non-SII \\& local)", "C"),
    # Panel D — Bid Outcomes
    ("log_win_price_ratio",  r"$\log(\text{win bid}/\text{ref})$",   "D"),
    ("log_min_price_ratio",  r"$\log(\text{min bid}/\text{ref})$",   "D"),
    ("log_sub_price_ratio",  r"$\log(\text{bid}/\text{ref})$",       "D"),
    ("bid_cv",               "Bid CV",                               "D"),
]

PANEL_TITLES = {
    "A": "Panel A: Entry at Bidding Stage",
    "B": "Panel B: Bidder Composition",
    "C": "Panel C: Winner Characteristics",
    "D": "Panel D: Bid Outcomes",
}


# ── Formatting helpers ────────────────────────────────────────────────────────
def _fmt_coef(val: float, stars: str = "") -> str:
    if pd.isna(val):
        return ""
    return f"{val:.4f}{stars}"


def _fmt_se(val: float) -> str:
    if pd.isna(val):
        return ""
    return f"({val:.4f})"


def _fmt_int(val) -> str:
    if pd.isna(val):
        return ""
    return f"{int(val):,}"


def _stars(pval: float) -> str:
    if pd.isna(pval):
        return ""
    if pval < 0.01:
        return "^{***}"
    if pval < 0.05:
        return "^{**}"
    if pval < 0.10:
        return "^{*}"
    return ""


# ── Build one table ───────────────────────────────────────────────────────────
def build_tex_table(
    results: pd.DataFrame,
    panels: list[str],
    caption: str,
    label: str,
) -> str:
    """
    Build a LaTeX booktabs table for the specified panels.
    Designed for beamer (uses \\footnotesize, no float).
    """
    lines = []
    lines.append(r"{\footnotesize")
    lines.append(r"\begin{tabular}{l r@{\hskip 6pt} r@{\hskip 6pt} r}")
    lines.append(r"\toprule")
    lines.append(r"Outcome & $\hat\beta_{\text{DiD}}$ & N & Entities \\")
    lines.append(r"\midrule")

    for panel in panels:
        title = PANEL_TITLES.get(panel, f"Panel {panel}")
        lines.append(rf"\multicolumn{{4}}{{l}}{{\textit{{{title}}}}} \\[2pt]")

        panel_outcomes = [(o, l, p) for o, l, p in OUTCOME_ORDER if p == panel]
        for outcome_col, display_label, _ in panel_outcomes:
            row = results[results["outcome"] == outcome_col]
            if row.empty:
                # Outcome not estimated — show placeholder
                lines.append(
                    rf"  \quad {display_label} & --- & --- & --- \\"
                )
                continue
            r = row.iloc[0]
            stars = _stars(r.get("pval_did", None))
            coef_str = _fmt_coef(r["coef_did"], stars)
            se_str   = _fmt_se(r["se_did"])
            n_str    = _fmt_int(r["n_obs"])
            ent_str  = _fmt_int(r["n_entities"])

            # Coefficient line
            lines.append(
                rf"  \quad {display_label} & ${coef_str}$ & {n_str} & {ent_str} \\"
            )
            # SE line (below coefficient)
            lines.append(
                rf"  & ${se_str}$ & & \\"
            )
        lines.append(r"[4pt]")

    lines.append(r"\bottomrule")
    lines.append(r"\end{tabular}")
    lines.append(r"}")  # close \footnotesize
    lines.append("")
    return "\n".join(lines)


# ── Side-by-side comparison table (two control groups) ───────────────────────
def build_comparison_table(
    results_cl: pd.DataFrame,
    results_ch: pd.DataFrame,
    panels: list[str],
) -> str:
    """
    Build a side-by-side LaTeX table comparing two control-group specs.

    Columns: Outcome | β̂_cl  (SE)  N_cl | β̂_ch  (SE)  N_ch
    where cl = control_low (0–30 UTM) and ch = control_high (100–200 UTM).
    """
    lines = []
    lines.append(r"{\footnotesize")
    lines.append(
        r"\begin{tabular}{l r@{\hskip 4pt} r@{\hskip 10pt} r@{\hskip 4pt} r}"
    )
    lines.append(r"\toprule")
    lines.append(
        r" & \multicolumn{2}{c}{Control: 0--30 UTM}"
        r" & \multicolumn{2}{c}{Control: 100--200 UTM} \\"
    )
    lines.append(
        r"Outcome & $\hat\beta_{\text{DiD}}$ & $N$"
        r" & $\hat\beta_{\text{DiD}}$ & $N$ \\"
    )
    lines.append(r"\midrule")

    for panel in panels:
        title = PANEL_TITLES.get(panel, f"Panel {panel}")
        lines.append(rf"\multicolumn{{5}}{{l}}{{\textit{{{title}}}}} \\[2pt]")

        panel_outcomes = [(o, l, p) for o, l, p in OUTCOME_ORDER if p == panel]
        for outcome_col, display_label, _ in panel_outcomes:
            row_cl = results_cl[results_cl["outcome"] == outcome_col]
            row_ch = results_ch[results_ch["outcome"] == outcome_col]

            def _cell(row):
                if row.empty:
                    return "---", "---", "---"
                r = row.iloc[0]
                stars = _stars(r.get("pval_did", None))
                coef  = _fmt_coef(r["coef_did"], stars)
                se    = _fmt_se(r["se_did"])
                n     = _fmt_int(r["n_obs"])
                return coef, se, n

            coef_cl, se_cl, n_cl = _cell(row_cl)
            coef_ch, se_ch, n_ch = _cell(row_ch)

            # Coefficient row
            lines.append(
                rf"  \quad {display_label}"
                rf" & ${coef_cl}$ & {n_cl}"
                rf" & ${coef_ch}$ & {n_ch} \\"
            )
            # SE row
            lines.append(
                rf"  & ${se_cl}$ & & ${se_ch}$ & \\"
            )
        lines.append(r"[4pt]")

    lines.append(r"\bottomrule")
    lines.append(
        r"\multicolumn{5}{l}{\scriptsize"
        r" SE clustered by procuring entity."
        r" $^{***}p{<}0.01$, $^{**}p{<}0.05$, $^{*}p{<}0.10$.} \\"
    )
    lines.append(r"\end{tabular}")
    lines.append(r"}")
    lines.append("")
    return "\n".join(lines)


# ── First-stage summary table ─────────────────────────────────────────────────
# Representative outcomes for the first-stage table (one per distinct sample).
FS_SAMPLE_LABELS = [
    ("n_bidders",            "Full tender sample"),
    ("winner_is_sme_sii",    "Winner sub-sample"),
    ("log_min_price_ratio",  "Full tender (min bid)"),
    ("log_sub_price_ratio",  "Bid-level sample"),
]


def build_first_stage_table(res_iv_ch: pd.DataFrame) -> str:
    """
    Compact first-stage table: one row per representative sample,
    showing π̂ (with SE below) and F-stat for control = 100–200 UTM.
    SE is derived from the saved coef and F-stat: se = |coef| / sqrt(F).
    """
    import math

    def _fmt_pi(val) -> str:
        return f"{val:.4f}" if pd.notna(val) else "---"

    def _fmt_se(val) -> str:
        return f"({val:.4f})" if pd.notna(val) else ""

    def _fmt_f(val) -> str:
        if pd.isna(val):
            return "---"
        return f"{val:,.0f}" if val >= 100 else f"{val:.1f}"

    def _cell(df, outcome):
        row = df[df["outcome"] == outcome]
        if row.empty or "first_stage_coef" not in df.columns:
            return "---", "", "---", "---"
        r = row.iloc[0]
        pi_val = r.get("first_stage_coef", float("nan"))
        f_val  = r.get("first_stage_f",    float("nan"))
        se_val = (
            abs(pi_val) / math.sqrt(f_val)
            if pd.notna(pi_val) and pd.notna(f_val) and f_val > 0
            else float("nan")
        )
        # Two-sided p-value via normal approximation: t = sqrt(F), p = erfc(sqrt(F/2))
        p_val = (
            math.erfc(math.sqrt(f_val / 2))
            if pd.notna(f_val) and f_val > 0
            else float("nan")
        )
        pi = _fmt_pi(pi_val) + _stars(p_val)
        se = _fmt_se(se_val)
        f  = _fmt_f(f_val)
        n  = _fmt_int(r["n_obs"])
        return pi, se, f, n

    lines: list[str] = []
    lines.append(r"{\footnotesize")
    lines.append(r"\begin{tabular}{l r@{\hskip 4pt} r@{\hskip 4pt} r}")
    lines.append(r"\toprule")
    lines.append(
        r"Sample & $\hat\pi$ & $F$ & $N$ \\"
    )
    lines.append(r"\midrule")

    for outcome_col, sample_label in FS_SAMPLE_LABELS:
        pi, se, f, n = _cell(res_iv_ch, outcome_col)
        # Coefficient row
        lines.append(rf"  {sample_label} & ${pi}$ & {f} & {n} \\")
        # SE row
        lines.append(rf"  & ${se}$ & & \\")

    lines.append(r"\bottomrule")
    lines.append(
        r"\multicolumn{4}{l}{\scriptsize"
        r" $\hat\pi$ = OLS coef.\ of $CA_{it}$ on $\text{Treated}_i"
        r"\times\text{Post}_t$ (entity + year-month FE).} \\"
    )
    lines.append(
        r"\multicolumn{4}{l}{\scriptsize"
        r" SE in parentheses, clustered by procuring entity."
        r" $^{***}p{<}0.01$, $^{**}p{<}0.05$, $^{*}p{<}0.10$.} \\"
    )
    lines.append(
        r"\multicolumn{4}{l}{\scriptsize"
        r" $F$ = cluster-robust first-stage $F$-stat. Control: 100--200 UTM.} \\"
    )
    lines.append(r"\end{tabular}")
    lines.append(r"}")
    lines.append("")
    return "\n".join(lines)


# ── OLS vs IV comparison table (same control group, different estimator) ──────
def build_ols_iv_comparison_table(
    results_ols: pd.DataFrame,
    results_iv: pd.DataFrame,
    panels: list[str],
) -> str:
    """
    Build a side-by-side LaTeX table comparing OLS DiD vs IV-DiD,
    both using the same control group (100–200 UTM).

    Columns: Outcome | β̂_OLS  (SE)  N | β̂_IV  (SE)  N
    """
    lines = []
    lines.append(r"{\footnotesize")
    lines.append(
        r"\begin{tabular}{l r@{\hskip 4pt} r@{\hskip 10pt} r@{\hskip 4pt} r}"
    )
    lines.append(r"\toprule")
    lines.append(
        r" & \multicolumn{2}{c}{OLS DiD: Control 100--200 UTM}"
        r" & \multicolumn{2}{c}{IV-DiD: Control 100--200 UTM} \\"
    )
    lines.append(
        r"Outcome & $\hat\beta_{\text{DiD}}$ & $N$"
        r" & $\hat\beta_{\text{IV-DiD}}$ & $N$ \\"
    )
    lines.append(r"\midrule")

    for panel in panels:
        title = PANEL_TITLES.get(panel, f"Panel {panel}")
        lines.append(rf"\multicolumn{{5}}{{l}}{{\textit{{{title}}}}} \\[2pt]")

        panel_outcomes = [(o, l, p) for o, l, p in OUTCOME_ORDER if p == panel]
        for outcome_col, display_label, _ in panel_outcomes:
            row_ols = results_ols[results_ols["outcome"] == outcome_col]
            row_iv  = results_iv[results_iv["outcome"] == outcome_col]

            def _cell(row):
                if row.empty:
                    return "---", "---", "---"
                r = row.iloc[0]
                stars = _stars(r.get("pval_did", None))
                coef  = _fmt_coef(r["coef_did"], stars)
                se    = _fmt_se(r["se_did"])
                n     = _fmt_int(r["n_obs"])
                return coef, se, n

            coef_ols, se_ols, n_ols = _cell(row_ols)
            coef_iv,  se_iv,  n_iv  = _cell(row_iv)

            # Coefficient row
            lines.append(
                rf"  \quad {display_label}"
                rf" & ${coef_ols}$ & {n_ols}"
                rf" & ${coef_iv}$ & {n_iv} \\"
            )
            # SE row
            lines.append(
                rf"  & ${se_ols}$ & & ${se_iv}$ & \\"
            )
        lines.append(r"[4pt]")

    lines.append(r"\bottomrule")
    lines.append(
        r"\multicolumn{5}{l}{\scriptsize"
        r" SE clustered by procuring entity."
        r" $^{***}p{<}0.01$, $^{**}p{<}0.05$, $^{*}p{<}0.10$.} \\"
    )
    lines.append(r"\end{tabular}")
    lines.append(r"}")
    lines.append("")
    return "\n".join(lines)


# ── Main ──────────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--sample",
        choices=["all", "municipalidades", "obras"],
        default="all",
        help="Sample restriction (must match the suffix used in 02_run_did.py)",
    )
    args = parser.parse_args()

    _SAMPLE_SUFFIX = {"all": "", "municipalidades": "_munic", "obras": "_obras"}
    ssuffix = _SAMPLE_SUFFIX[args.sample]

    print("=" * 70)
    print("03_make_tex_tables.py — Compile DiD results into LaTeX tables")
    print(f"  Sample: {args.sample}  (suffix='{ssuffix}')")
    print("=" * 70)

    results_path = OUT_TABLES / f"did_results_all{ssuffix}.csv"
    if not results_path.exists():
        print(f"  [ERROR] {results_path} not found. Run 02_run_did.py first.")
        return

    results = pd.read_csv(results_path)
    print(f"  Loaded {len(results)} rows from {results_path.name}")

    # ── Single-spec tables (both control groups pooled) ────────────────────
    tex_A = build_tex_table(results, panels=["A"],
                            caption="Entry at Bidding Stage", label="tab:did_panel_a")
    fname_A = f"did_table_panel_A{ssuffix}.tex"
    (OUT_TABLES / fname_A).write_text(tex_A)
    print(f"  Saved: {fname_A}")

    tex_B = build_tex_table(results, panels=["B"],
                            caption="Bidder Composition", label="tab:did_panel_b")
    fname_B = f"did_table_panel_B{ssuffix}.tex"
    (OUT_TABLES / fname_B).write_text(tex_B)
    print(f"  Saved: {fname_B}")

    tex_C = build_tex_table(results, panels=["C"],
                            caption="Winner Characteristics", label="tab:did_panel_c")
    fname_C = f"did_table_panel_C{ssuffix}.tex"
    (OUT_TABLES / fname_C).write_text(tex_C)
    print(f"  Saved: {fname_C}")

    tex_D = build_tex_table(results, panels=["D"],
                            caption="Bid Outcomes", label="tab:did_panel_d")
    fname_D = f"did_table_panel_D{ssuffix}.tex"
    (OUT_TABLES / fname_D).write_text(tex_D)
    print(f"  Saved: {fname_D}")

    fname_combined = f"did_table_combined{ssuffix}.tex"
    tex_all = build_tex_table(results, panels=["A", "B", "C", "D"],
                              caption="Compra \\'Agil DiD", label="tab:did_all")
    (OUT_TABLES / fname_combined).write_text(tex_all)
    print(f"  Saved: {fname_combined}")

    # ── Side-by-side comparison tables ────────────────────────────────────
    cl_path = OUT_TABLES / f"did_results_cl{ssuffix}.csv"
    ch_path = OUT_TABLES / f"did_results_ch{ssuffix}.csv"
    if not cl_path.exists() or not ch_path.exists():
        print(f"  [WARN] {cl_path.name} / {ch_path.name} not found — "
              "skipping comparison tables. Run 02_run_did.py first.")
        print("\nDone.")
        return

    res_cl = pd.read_csv(cl_path)
    res_ch = pd.read_csv(ch_path)
    print(f"  Loaded {len(res_cl)} / {len(res_ch)} rows from cl / ch result files")

    for panel in ["A", "B", "C", "D"]:
        tex_cmp = build_comparison_table(res_cl, res_ch, panels=[panel])
        fname   = f"did_table_compare_{panel}{ssuffix}.tex"
        (OUT_TABLES / fname).write_text(tex_cmp)
        print(f"  Saved: {fname}")

    # ── IV side-by-side comparison tables ─────────────────────────────────
    iv_ch_path = OUT_TABLES / f"did_results_iv_ch{ssuffix}.csv"
    if not iv_ch_path.exists():
        print(f"  [WARN] {iv_ch_path.name} not found — skipping IV tables.")
        print("\nDone.")
        return

    res_iv_ch = pd.read_csv(iv_ch_path)
    print(f"  Loaded {len(res_iv_ch)} rows from IV ch file")

    for panel in ["A", "B", "C", "D"]:
        tex_iv = build_ols_iv_comparison_table(res_ch, res_iv_ch, panels=[panel])
        fname  = f"did_table_compare_iv_{panel}{ssuffix}.tex"
        (OUT_TABLES / fname).write_text(tex_iv)
        print(f"  Saved: {fname}")

    # ── First-stage table ──────────────────────────────────────────────────
    tex_fs = build_first_stage_table(res_iv_ch)
    fname_fs = f"did_table_first_stage{ssuffix}.tex"
    (OUT_TABLES / fname_fs).write_text(tex_fs)
    print(f"  Saved: {fname_fs}")

    print("\nDone.")


if __name__ == "__main__":
    main()
