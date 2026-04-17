# Project Handoff for Coding Agent Review and Expansion

Date: 2026-04-07

Workspace:

```text
/Users/nicolasjimenez/Documents/Research-local/procurement-chile-local
```

This note summarizes the current repository structure, economic model, empirical design, analysis code, generated outputs, and the main open extension points. It is intended as a handoff to another coding agent.

## 1. Project Purpose

The project studies the effects of Chile's December 12, 2024 Compra Agil / local-preference procurement reform. The central empirical object is the shift in procurement rules for purchases in the 30-100 UTM band: these purchases moved from standard licitaciones into Compra Agil eligibility, while lower-value purchases and higher-value purchases serve as comparison groups.

The research focus has several connected pieces:

- Direct procurement-market effects: entry, number of bidders, local vs. non-local bidders, SME composition, winner composition, and prices.
- Cross-market spillovers and capacity reallocation: how restrictions on out-of-region participation change firms' bidding portfolios and other markets.
- Municipalidades vs. Obras Publicas heterogeneity: goods-heavy municipal procurement may differ from construction/public works procurement because capacity constraints and cost complementarities differ.
- Buyer-side responses: how procuring entities changed product mix, product bundling, quantities, and mechanism choice after the policy change.
- Bidder workload effects: whether recent bids or recent wins predict bid markups, separately for Municipalidades and Obras Publicas.

The latest deck appears to be:

```text
deck/full_project_deck_260309.tex
deck/full_project_deck_260309.pdf
```

The clean data directory is large (`data/clean` is about 13 GB) and generated output is also large (`output` is about 1.5 GB).

## 2. Top-Level Repository Structure

```text
code/
  clean/                    Cleaning and SII merge pipeline.
  analysis/
    did/                    Workhorse DiD samples, TWFE, IV, event studies, diagnostics, heterogeneity, spillovers.
    bids/                   Bid-level markup samples and regressions, distance, recent activity / workload.
    simultaneousbids/       Firm-month simultaneous bidding panels, descriptives, regressions.
    product_mix/            Buyer-side product-code, bundling, and quantity analyses.
    bunching/               Threshold/bunching diagnostics.
    descriptives/           Compra Agil diagnostics.

data/
  raw/
    chilecompra/
      licitaciones/
      compra_agil/
      examples/
    sii/
    other/
  clean/                    Clean panels and merged analysis inputs.
  diagnostics/
  documentation/

deck/                       Beamer deck source and rendered PDFs.
gov_docs/                   Policy/procurement source documents.
literature/                 Literature notes.
model/                      Spatial auction model writeup and compiled PDF.
notes/                      Research notes and handoff notes.
output/
  did/                      DiD samples, tables, figures.
  bids/                     Bid-level samples, tables, figures.
  product_mix/              Buyer-side product-mix samples and tables.
  simultaneousbids/         Simultaneous bidding panels, tables, figures.
  diagnostics/
  summary_stats/
```

There is no Git repository at this path at the time of this handoff (`git status` reports not a git repository).

## 3. Core Clean Data Files

Important clean files:

```text
data/clean/chilecompra_panel.parquet
data/clean/compra_agil_panel.parquet
data/clean/licitaciones_sii_merged.parquet
data/clean/compra_agil_sii_merged.parquet
data/clean/combined_sii_merged.parquet
data/clean/combined_sii_merged_filtered.parquet
data/clean/rut_unidad_sector_crosswalk.parquet
data/clean/comunas_centroids.csv
data/clean/comuna_distance_matrix.csv
data/clean/comuna_distances_long.csv
```

Key interpretations:

- `chilecompra_panel.parquet`: cleaned licitaciones panel, still close to raw line/bid structure.
- `compra_agil_panel.parquet`: cleaned Compra Agil panel.
- `licitaciones_sii_merged.parquet`: licitaciones merged to SII bidder registry.
- `compra_agil_sii_merged.parquet`: Compra Agil merged to SII bidder registry.
- `combined_sii_merged_filtered.parquet`: combined bid-level universe after estimated-cost outlier filtering. This is the main input for `code/analysis/did/01_build_did_sample.py`.
- `rut_unidad_sector_crosswalk.parquet`: buyer-sector fill used because Compra Agil sector is sometimes missing.
- `comunas_centroids.csv` and related distance files support commune-level distance analyses.

Important caveat: licitaciones bid amounts often need to be reconstructed from `chilecompra_panel.parquet` fields such as `Valor Total Ofertado`. Do not assume `monto_oferta` is fully populated for licitaciones in all downstream files without checking the specific sample.

## 4. Cleaning Pipeline

Master runner:

```text
code/clean/01_run_pipeline.py
```

Stages:

```text
1  code/clean/02_clean_licitaciones.py
   Output: data/clean/chilecompra_panel.parquet

2  code/clean/03_clean_compra_agil.py
   Output: data/clean/compra_agil_panel.parquet

3  code/clean/04_merge_sii_licitaciones.py
   Output: data/clean/licitaciones_sii_merged.parquet

4  code/clean/05_merge_sii_compra_agil.py
   Output: data/clean/compra_agil_sii_merged.parquet

5  code/clean/06_combine_sii_merged.py
   Output: data/clean/combined_sii_merged.parquet

6  code/clean/07_filter_estimated_cost_outliers.py
   Output: data/clean/combined_sii_merged_filtered.parquet

7  code/clean/08_diagnostics.py
   Output: data/diagnostics/figures/

8  code/clean/09_quarterly_trends.py
   Output: data/diagnostics/figures/

9  code/clean/10_fill_sector_from_rutunidad.py
   Output: data/clean/rut_unidad_sector_crosswalk.parquet
```

Example commands:

```bash
python code/clean/01_run_pipeline.py --list
python code/clean/01_run_pipeline.py
python code/clean/01_run_pipeline.py 9
```

The runner treats stages 1 and 2 as parallelizable, and stages 3 and 4 as parallelizable.

## 5. Economic Model

Primary files:

```text
model/spatial_auction_model.md
model/spatial_auction_model.tex
model/spatial_auction_model.pdf
```

The model is a spatial auction model with entry, geography, capacity constraints, and local-preference policy.

Environment:

- Regions are indexed by `r = 1, ..., R`, with pairwise distances `d_rs`.
- Government demand in region `r` is `Q_r` projects per period.
- Firms have home region `h_i`.
- Local potential supply is `N_r^pot`.
- Firms have capacity `K_i`, the maximum number of projects/markets they can handle per period.

Cost structure:

```text
c_ir = c_bar + delta * d_{h_i,r} - lambda * 1[h_i = r] + epsilon_ir
```

Interpretation:

- `delta * distance` is a pure distance/logistics cost.
- `lambda` is a non-distance local advantage: information, subcontractor networks, local rules, terrain, relationships.
- A non-local firm has a cost disadvantage of `delta * d_sr + lambda` relative to a local firm.

Entry cost:

```text
kappa_r^i = kappa_0 + kappa_d * d_{h_i,r}
```

Policy environment in the model:

- Pre-reform: all firms can enter all markets.
- Strong post-reform preference: for Compra Agil / below-100 UTM projects, only local EMT firms may enter, so non-local firms are effectively excluded.
- Weak post-reform preference: for 100-500 UTM projects, all firms may enter but local firms get a bid preference `alpha`.
- No preference: above-threshold projects remain status quo.

Empirical focus:

- The main DiD in the code focuses on the 30-100 UTM treated band because the old Compra Agil ceiling was 30 UTM and the new ceiling is 100 UTM.
- Some model text also discusses a 500 UTM weak-preference tier. That is conceptually relevant for future Obras / larger-value work, but the workhorse DiD code is primarily the 1-200 UTM design.

Core prediction channels:

- Direct exclusion: removing non-local firms can reduce competition and raise procurement costs if excluded firms were competitive.
- Local entry response: if local potential supply and demand are thick enough, local entry can offset the loss of non-local bidders.
- Cross-market spillovers: capacity freed by excluded non-local firms may be redirected to home or other markets, changing competition outside the directly affected tenders.
- Heterogeneous effects: costs rise most in thin markets with low local potential supply, low distance/local disadvantage for non-locals, and limited local entry response.
- Buyer-side adaptation: buyers may change order size, bundling, and product mix when the mechanism changes.

## 6. Core Empirical Design

Shared DiD utilities:

```text
code/analysis/did/did_utils.py
```

Key constants:

```text
REFORM_DATE   = 2024-12-12
REFORM_PERIOD = 2024-12
OMIT_PERIOD   = 2024-11
```

Value bands:

```text
control_low  : [1, 30) UTM
treated      : [30, 100] UTM
control_high : (100, 200] UTM
```

Indicators:

```text
treated = 1[band == "treated"]
post    = 1[fecha_pub >= 2024-12-12]
did     = treated * post
```

The main DiD estimator uses two-way fixed effects with entity and year-month FEs. The default entity is `rut_unidad` (procuring entity / buyer), and standard errors are clustered by `rut_unidad`.

`did_utils.py` implements:

- UTM loading and CLP-to-UTM conversion.
- Band assignment.
- Cluster-robust covariance / SE utilities.
- Two-way demeaning by alternating projections.
- `run_twfe_did`: pooled TWFE DiD.
- `run_twfe_iv`: IV-TWFE where `did` instruments Compra Agil mechanism use (`ca_post`).
- `run_twfe_event_study`: monthly event-study interactions.
- Results-to-dataframe and plotting helpers.

Main estimating equation, conceptually:

```text
y_it = beta * (treated_i * post_t) + buyer FE_i + year-month FE_t + controls + error_it
```

For bid-level recent-activity analysis, the entity is instead the bidder, and FEs are bidder plus year-month.

Control-group variants used across the project:

```text
all controls : control_low + treated + control_high
low control  : control_low + treated
high control : treated + control_high
```

The current preferred buyer-side/product-mix interpretation often treats `high_control` (`100-200` UTM) as the cleaner comparison for formal-procurement counterfactuals, while `low_control` (`1-30` UTM) is substantively useful for asking whether treated tenders begin to resemble very small purchases.

## 7. DiD Analysis Code

Main files:

```text
code/analysis/did/01_build_did_sample.py
code/analysis/did/02_run_did.py
code/analysis/did/03_make_tex_tables.py
code/analysis/did/04_diagnose_did.py
code/analysis/did/05_heterogeneity_region.py
code/analysis/did/06_spillovers_region.py
code/analysis/did/07_binscatter_moderators.py
code/analysis/did/08_distance_moderator.py
code/analysis/did/did_utils.py
```

### 7.1 Build DiD Samples

Script:

```text
code/analysis/did/01_build_did_sample.py
```

Inputs:

```text
data/clean/combined_sii_merged_filtered.parquet
data/clean/chilecompra_panel.parquet
data/clean/rut_unidad_sector_crosswalk.parquet
data/raw/other/utm_clp_2022_2025.csv
```

Outputs:

```text
output/did/samples/did_tender_sample.parquet
output/did/samples/did_bid_sample.parquet
output/did/tables/sme_diagnostics.csv
```

Tasks:

- Load combined SII-merged procurement rows.
- Fill Compra Agil sector from `rut_unidad` crosswalk.
- Build canonical bidder IDs from RUT + DV.
- Pull licitaciones submitted bids and adjudication metadata from `chilecompra_panel.parquet`.
- Convert estimated values to UTM.
- Assign bands and DiD indicators.
- Compute first-time bidder / new entrant indicators.
- Compute SME indicators using Compra Agil `tamano` and SII `tramoventas`.
- Collapse to one tender row per tender.
- Create bid-level sample with `log_sub_price_ratio = log(submitted bid / estimated value)`.

Useful arguments:

```bash
python code/analysis/did/01_build_did_sample.py --sample all
python code/analysis/did/01_build_did_sample.py --sample municipalidades
python code/analysis/did/01_build_did_sample.py --sample obras_o2
python code/analysis/did/01_build_did_sample.py --sample obras_sector
python code/analysis/did/01_build_did_sample.py --sample tipo_o2
```

Note: by default, downstream scripts generally expect the broad `all` version of the DiD samples unless a script explicitly filters by sector in memory.

### 7.2 Run Workhorse DiD

Script:

```text
code/analysis/did/02_run_did.py
```

Inputs:

```text
output/did/samples/did_tender_sample.parquet
output/did/samples/did_bid_sample.parquet
```

Outputs include:

```text
output/did/tables/did_results_all.csv
output/did/tables/did_results_cl.csv
output/did/tables/did_results_ch.csv
output/did/tables/did_results_iv_ch.csv
output/did/tables/event_study_*.csv
output/did/figures/did_coef_*.png
output/did/figures/event_study_*.png
```

It estimates tender-level outcomes and bid-level outcomes.

Tender-level outcome groups:

- Entry: `n_bidders`, `n_local`, `n_nonlocal`, `n_sme_bidders`, `n_large_bidders`, `n_nonsii_bidders`, `single_bidder`.
- Composition: `any_sme_sii`, `sme_share_sii`, `share_large_bidders`, `share_bidders_not_in_sii`, local-SME / local-large shares.
- Winner characteristics: `winner_is_sme_sii`, `winner_is_large`, `winner_not_in_sii`, `winner_is_sme_local`, etc.
- Bid outcomes: `log_win_price_ratio`, `log_min_price_ratio`, `bid_cv`.

Bid-level outcome:

```text
log_sub_price_ratio = log(submitted bid / reference price)
```

The script runs:

- Spec 1: both control groups (`control_low + treated + control_high`).
- Spec 2: low control (`control_low + treated`).
- Spec 3: high control (`control_high + treated`).
- IV spec: `ca_post` instrumented by `did`, usually in the high-control sample.
- Event studies unless `--did-only` is passed.

Useful commands:

```bash
python code/analysis/did/02_run_did.py --sample all
python code/analysis/did/02_run_did.py --sample municipalidades
python code/analysis/did/02_run_did.py --sample obras
python code/analysis/did/02_run_did.py --sample municipalidades --did-only
python code/analysis/did/02_run_did.py --drop-k0
```

### 7.3 Tables and Diagnostics

`03_make_tex_tables.py` builds TeX tables from DiD CSVs:

```text
output/did/tables/did_table_panel_*.tex
output/did/tables/did_table_compare_*.tex
output/did/tables/did_table_first_stage*.tex
```

`04_diagnose_did.py` runs high-control diagnostic tests:

- Pre-trend Wald tests.
- Time placebos.
- Pre-reform balance tests.
- Diagnostic event studies.

Outputs:

```text
output/did/tables/diag_pretrend_wald*.csv
output/did/tables/diag_placebo*.csv
output/did/tables/diag_balance*.csv
output/did/tables/diag_event_study_*.csv
```

`05_heterogeneity_region.py` builds region-level moderators and runs interacted OLS-DiD and IV-DiD. Top-level config currently has:

```text
SUBSAMPLE = "munic"
CONTROL   = "high"
```

Moderators:

```text
nonlocal_share_pre
q_pre
totval_pre
n_pot_local
dist_from_santiago
```

Outputs:

```text
output/did/tables/hetero_interacted_*_munic.csv
output/did/figures/hetero_coefplot_*_munic.png
```

`06_spillovers_region.py`, `07_binscatter_moderators.py`, and `08_distance_moderator.py` implement regional spillover, binscatter, and distance-moderator analyses.

## 8. Bid-Level Analysis Code

Main files:

```text
code/analysis/bids/01_build_bid_sample.py
code/analysis/bids/02_run_bid_regressions.py
code/analysis/bids/03_run_bid_followups.py
code/analysis/bids/04_run_commune_distance.py
code/analysis/bids/05_run_recent_activity_fe.py
```

### 8.1 Bid Markup Sample

Script:

```text
code/analysis/bids/01_build_bid_sample.py
```

Input:

```text
output/did/samples/did_bid_sample.parquet
data/clean/combined_sii_merged_filtered.parquet
data/clean/comunas_centroids.csv
```

Outputs:

```text
output/bids/bid_analysis_sample.parquet
output/bids/bid_analysis_sample_munic.parquet
output/bids/bid_analysis_sample_obras.parquet
```

Key added variables:

- `log_sub_price_ratio`: bid markup outcome.
- `bidder_region_norm`.
- `dist_km`, `log_dist_km`: region-centroid distance.
- `comuna_bidder`, `comuna_buyer`.
- `dist_km_com`, `log_dist_km_com`, `dist_bin_com`: commune-level distances.
- `local`, `sme`, `large`.
- `k_rel`: months relative to reform.
- `year_month_str`.

Useful commands:

```bash
python code/analysis/bids/01_build_bid_sample.py --sample all
python code/analysis/bids/01_build_bid_sample.py --sample municipalidades
python code/analysis/bids/01_build_bid_sample.py --sample obras
```

### 8.2 Bid Markup Regressions

Script:

```text
code/analysis/bids/02_run_bid_regressions.py
```

Backend:

```text
linearmodels.iv.AbsorbingLS
```

Outputs:

```text
output/bids/tables/bids_part1_auction_fe.csv
output/bids/tables/bids_part2_firm_fe.csv
output/bids/tables/bids_part3_did.csv
output/bids/tables/bids_part3_event_study.csv
output/bids/figures/bids_part*_*.png
```

Parts:

- Part 1: auction FE analyses.
- Part 2: firm FE analyses.
- Part 3: DiD and event-study bid markup analyses, using treated vs high control for the event-study sample.

Useful commands:

```bash
python code/analysis/bids/02_run_bid_regressions.py --sample all
python code/analysis/bids/02_run_bid_regressions.py --sample municipalidades
python code/analysis/bids/02_run_bid_regressions.py --sample obras
python code/analysis/bids/02_run_bid_regressions.py --part3-only --sample municipalidades
```

### 8.3 Followups and Commune Distance

`03_run_bid_followups.py` runs size-split and region-split bid-level followups.

`04_run_commune_distance.py` runs commune-distance regressions and writes:

```text
output/bids/tables/bids_commune_distance.csv
output/bids/tables/bids_commune_distance.tex
```

### 8.4 Recent Activity / Workload Analysis

Script:

```text
code/analysis/bids/05_run_recent_activity_fe.py
```

Purpose:

Estimate bid-level markup regressions with bidder and year-month fixed effects, using recent bidder activity as a workload proxy. Runs separately for:

```text
sector in ["Municipalidades", "Obras Publicas"]
activity in ["bids", "wins"]
scope in ["all", "same_sector"]
window in ["1m", "3m"]
transform in ["count", "any", "log1p"]
```

Outcome:

```text
y = log_sub_price_ratio
```

Controls:

```text
log_monto_est + same_region + dataset_ca
```

Fixed effects:

```text
bidder + year_month
```

For recent bids:

- Activity universe comes from `output/did/samples/did_bid_sample.parquet`.
- Timing uses `fecha_pub` for consistency across Compra Agil and licitaciones.

For recent wins:

- Activity universe comes from awarded tenders in `combined_sii_merged_filtered.parquet`.
- Licitaciones win timing uses `FechaAdjudicacion` from `chilecompra_panel.parquet` when available, otherwise publication date.
- Compra Agil lacks comparable contract start/end fields, so wins analysis is a proxy rather than true ongoing-project workload.

Commands:

```bash
python code/analysis/bids/05_run_recent_activity_fe.py --activity bids --scope both
python code/analysis/bids/05_run_recent_activity_fe.py --activity wins --scope both
```

Outputs:

```text
output/bids/tables/recent_bids_fe_results.csv
output/bids/tables/recent_bids_sample_stats.csv
output/bids/tables/recent_wins_fe_results.csv
output/bids/tables/recent_wins_sample_stats.csv
```

Current recent-activity findings:

- Recent wins are basically null across raw counts, `1(any recent win)`, and `log(1 + recent wins)`, for both Municipalidades and Obras Publicas and both all-activity and same-sector scopes.
- Recent bids show a positive relationship with bid markup, especially in the same-sector scope and with `log(1 + recent bids)`.
- Same-sector recent bids, log1p transform:
  - Municipalidades: `log1p_recent_1m = 0.0162`, `p = 0.020`; `log1p_recent_3m = 0.0176`, `p = 0.011`.
  - Obras Publicas: `log1p_recent_1m = 0.0173`, `p = 0.040`; `log1p_recent_3m = 0.0175`, `p = 0.013`.
- The `any_recent_bid` dummy is often noisy or saturated, especially in the all-activity scope.

Older one-off fallback workload outputs also exist:

```text
output/bids/tables/ongoing_wins_fe_results.csv
output/bids/tables/ongoing_wins_fe_results_same_sector.csv
output/bids/tables/ongoing_wins_sample_stats.csv
```

## 9. Simultaneous Bidding Analysis

Main notes:

```text
notes/simultaneousbids/plan.md
notes/simultaneousbids/results.md
```

Main code:

```text
code/analysis/simultaneousbids/01_build_simultaneous_bids.py
code/analysis/simultaneousbids/02_descriptives.py
code/analysis/simultaneousbids/03_entry_bidding.py
code/analysis/simultaneousbids/04_extensions.py
code/analysis/simultaneousbids/05_clp_distribution.py
code/analysis/simultaneousbids/06_did_fe.py
```

Outputs:

```text
output/simultaneousbids/firm_month_panel.parquet
output/simultaneousbids/bid_level_simult.parquet
output/simultaneousbids/tables/t1_*.csv ... t21_*.csv
output/simultaneousbids/figures/f1_*.png ... f9_*.png
```

Definition:

```text
n_sim = number of distinct tenders a firm bid on in the same calendar month
```

Key findings from `notes/simultaneousbids/results.md`:

- More than half of firm-months have only one active bid, but the distribution has a long right tail.
- Larger firms are much more simultaneously active and more non-local.
- Santiago / Region Metropolitana firms are the most active and nationally oriented.
- Obras Publicas has fewer observations but strong simultaneous bidding intensity among active firms; construction firms bid from farther away.
- Firm FE extensions attenuate but do not fully eliminate some workload/entry relationships.

This module is conceptually related to, but distinct from, `code/analysis/bids/05_run_recent_activity_fe.py`. The simultaneous-bidding module uses same-calendar-month portfolio measures; recent-activity uses rolling prior 1-month and 3-month windows.

## 10. Product Mix / Buyer-Side Analysis

Main note:

```text
notes/empirics/product_mix/product_mix_note_260319.md
```

Code:

```text
code/analysis/product_mix/01_build_product_mix_sample.py
code/analysis/product_mix/02_run_product_mix_analysis.py
```

### 10.1 Build Product-Mix Sample

Script:

```text
code/analysis/product_mix/01_build_product_mix_sample.py
```

Inputs:

```text
output/did/samples/did_bid_sample.parquet
data/clean/chilecompra_panel.parquet
data/clean/compra_agil_panel.parquet
```

Outputs:

```text
output/product_mix/samples/product_mix_lines.parquet
output/product_mix/samples/product_mix_tenders.parquet
```

Construction:

- Builds tender metadata from the DiD bid sample.
- For licitaciones, collapses raw rows to buyer-requested product lines by `(Codigo, Correlativo)`.
- For Compra Agil, collapses to unique `(CodigoCotizacion, CodigoProducto, ProductoCotizado, CantidadSolicitada)`.
- Standardizes product codes into:
  - `product_code8`
  - `product_class6`
  - `product_family4`
  - `product_segment2`
- Builds tender-level metrics:
  - `n_lines`
  - `n_product8`
  - `n_family4`
  - `n_segment2`
  - `single_line`
  - `mean_quantity_requested`
  - `sum_quantity_requested`

Important product-code fields:

```text
licitaciones product code : CodigoProductoONU
Compra Agil product code  : CodigoProducto
licitaciones quantity     : Cantidad
Compra Agil quantity      : CantidadSolicitada
licitaciones unit         : UnidadMedida
Compra Agil unit          : not comparable / missing in cleaned panel
```

Quantity caveat:

- Compra Agil lacks a comparable unit-of-measure field.
- Tender-level quantity analysis is therefore suggestive, not fully unit-harmonized.
- Quantity comparisons are safest within exact 8-digit product codes.

### 10.2 Product-Mix Analysis

Script:

```text
code/analysis/product_mix/02_run_product_mix_analysis.py
```

Outputs:

```text
output/product_mix/tables/sector_shift_treated_band.csv
output/product_mix/tables/segment_shift_treated_band.csv
output/product_mix/tables/family_shift_treated_band.csv
output/product_mix/tables/product_code_shift_treated_band.csv
output/product_mix/tables/top_product_quantity_examples.csv
output/product_mix/tables/bundling_cell_means_all_controls.csv
output/product_mix/tables/bundling_cell_means_low_control.csv
output/product_mix/tables/bundling_cell_means_high_control.csv
output/product_mix/tables/bundling_did_tender_all_controls.csv
output/product_mix/tables/bundling_did_tender_low_control.csv
output/product_mix/tables/bundling_did_tender_high_control.csv
output/product_mix/tables/bundling_did_comparison.csv
```

Current buyer-side findings:

- The treated 30-100 UTM band almost fully shifted into Compra Agil after the reform.
- Compra Agil share in treated-band tenders:
  - Municipalidades: `3.8% -> 90.9%`.
  - Obras Publicas: `2.6% -> 92.9%`.
  - FFAA: `1.2% -> 92.5%`.
  - Gob. Central / Universidades: `6.9% -> 95.0%`.
  - Salud: `36.6% -> 97.5%`.
- Exact product-code movers include tools, hardware, keys, books, steel profiles, drill bits, uniforms, electrical accessories, silicones, paints, solvents, desktop computers, and police vehicles.
- Substantive categories: routine goods, maintenance/hardware supplies, paint and construction materials, electrical fittings, tools, IT, uniforms/apparel, books/school materials, and some vehicle-related items.

Bundling DiD interpretation by control group:

High control (`30-100` treated vs `100-200` UTM control):

```text
n_lines                       -0.185   se 0.141   p = 0.191
n_product8                    +0.374   se 0.065   p < 0.001
n_family4                     +0.234   se 0.032   p < 0.001
n_segment2                    +0.163   se 0.019   p < 0.001
single_line                   +0.009   se 0.007   p = 0.181
log1p_mean_quantity_requested +0.582   se 0.040   p < 0.001
```

Interpretation: relative to somewhat larger formal tenders, treated tenders did not clearly gain more line items, but did become broader in product composition and requested quantities.

Low control (`30-100` treated vs `1-30` UTM control):

```text
n_lines                       +0.679   se 0.071   p < 0.001
n_product8                    +0.283   se 0.037   p < 0.001
n_family4                     +0.128   se 0.020   p < 0.001
n_segment2                    +0.055   se 0.012   p < 0.001
single_line                   -0.001   se 0.005   p = 0.894
log1p_mean_quantity_requested +0.370   se 0.055   p < 0.001
```

Interpretation: relative to very small purchases, treated tenders look more bundled in the literal sense of more lines and broader products.

Recommended framing:

- Use high control as the preferred formal-procurement counterfactual.
- Use low control as a distinct economic question: whether treated tenders began to resemble very small Compra Agil purchases.
- The line-count conclusion depends on control group.
- Product breadth and quantity results are more robust.

## 11. Bunching and Descriptives

Bunching scripts:

```text
code/analysis/bunching/10_kde_licitaciones_value.py
code/analysis/bunching/11_bunching_100utm_2025q1.py
```

These study value distributions and threshold behavior around policy-relevant UTM thresholds.

Descriptives:

```text
code/analysis/descriptives/09_compra_agil_diagnostics.py
```

This writes diagnostic figures and summary statistics for Compra Agil to:

```text
output/diagnostics/
output/summary_stats/
```

## 12. Notes and Existing Writeups

Important notes:

```text
notes/empirics/product_mix/product_mix_note_260319.md
notes/simultaneousbids/plan.md
notes/simultaneousbids/results.md
notes/law_changes/
```

Important policy docs:

```text
gov_docs/Informe-Final-FNE.pdf
gov_docs/Minuta Brechas Licitaciones MOP.docx
```

Literature notes:

```text
literature/chatgpt_5_4-litreview-deep-research-report.md
```

## 13. Dependencies and Runtime Notes

There is no obvious `requirements.txt` or `pyproject.toml` at the project root. The scripts currently import:

```text
pandas
numpy
pyarrow
duckdb
scipy
matplotlib
seaborn
linearmodels
pyfixest
```

Some scripts operate on millions to tens of millions of rows. DuckDB is used in the product-mix and recent-activity code to avoid loading everything into memory too early. `did_utils.py` implements custom two-way demeaning and clustered SEs to avoid huge dummy matrices.

Prefer running the analysis scripts from the project root:

```bash
cd /Users/nicolasjimenez/Documents/Research-local/procurement-chile-local
```

Then run scripts as `python code/.../script.py`. Several scripts use path logic relative to their location, but root execution is the safest convention.

## 14. Known Data and Interpretation Caveats

- Compra Agil does not have comparable contract start/end dates in the cleaned data, so true ongoing-project workload is not available for both mechanisms.
- Recent wins analysis therefore uses prior wins in rolling windows, not true overlapping active contracts.
- Recent bids analysis uses `fecha_pub` as the activity date for consistency across mechanisms, not exact bid submission date.
- Compra Agil lacks a comparable quantity-unit field, so product-mix quantity comparisons are not fully unit harmonized.
- Licitaciones line-item rows can duplicate buyer-requested product lines across bidders; product-mix code collapses by `(Codigo, Correlativo)` to avoid treating bidder offers as separate buyer lines.
- Compra Agil raw rows can duplicate requested products across supplier responses; product-mix code collapses to unique request-line tuples.
- SII linkage is incomplete for some bidders. Variables like `share_bidders_not_in_sii` and `winner_not_in_sii` are explicitly analyzed; missing SII linkage is not just a nuisance.
- SME definitions differ between Compra Agil platform `tamano` and SII `tramoventas`. The current main definition generally uses SII when possible.
- Sector fields for Compra Agil are partly filled from a `rut_unidad` crosswalk; sector-specific analyses should preserve this step.
- High-control vs low-control comparisons have different economic interpretations. Do not pool them blindly.
- The reform month, December 2024, is a transition month. Some scripts support `--drop-k0` to exclude it.
- The model has a weak-preference tier up to 500 UTM, but most current empirical DiD code uses the 1-200 UTM design around the 100 UTM Compra Agil threshold.

## 15. Recommended Extension Points

Near-term review tasks:

- Review whether the current preferred control group should be high control for all headline analyses or only for selected product-mix / formal-procurement comparisons.
- Check whether all newer analyses are integrated into the deck or still only live in notes and CSVs.
- Confirm that `did_bid_sample.parquet` was generated from the intended broad sample before running product-mix code, because product-mix inherits the DiD sample universe.
- Add lightweight run logs to `output/product_mix/tables/` and `output/bids/tables/` for reproducibility, matching the existing DiD logs.

Empirical extensions:

- Product mix by sector: run the product-mix and bundling DiD separately for Municipalidades and Obras Publicas.
- Buyer-product panels: build buyer x product-code x month panels to test whether procuring entities consolidate purchases over time, not just within tender.
- Quantity robustness: compare quantities only within exact 8-digit product codes with stable units in licitaciones, and avoid cross-code aggregation unless unit harmonization is added.
- Bundle detection: identify repeated same-buyer same-product or same-family tenders before the reform and test whether they consolidate into fewer multi-product Compra Agil orders after the reform.
- Mechanism choice: compare similar product codes and buyer sectors around thresholds to test whether PEs strategically choose Compra Agil vs licitacion.
- Recent activity: turn `recent_bids_fe_results.csv` and `recent_wins_fe_results.csv` into a compact deck table with same-sector log1p as the headline and all-scope / dummy / count as sensitivity.
- True backlog for licitaciones-only: for Obras Publicas, use `FechaAdjudicacion`, `FechaInicio`, and `FechaFinal` in licitaciones to build active-contract workload where possible, even if Compra Agil cannot be included symmetrically.
- Weak-preference tier: extend beyond 200 UTM to study the 100-500 UTM margin for public works if the legal interpretation and data coverage are clear.

Code-quality extensions:

- Add a top-level analysis runner or Makefile so that DiD, bid-level, simultaneous-bid, and product-mix scripts can be reproduced in order.
- Add a small `requirements.txt` or `environment.yml`.
- Add schema checks for key input parquet files, especially before expensive scripts.
- Cache expensive intermediate DuckDB temp tables when outputs are stable.
- Standardize sample suffix conventions (`_munic`, `_obras`, `_ch`, `_cl`, `_all`) across all modules.
- Consider moving shared bidder-ID construction and sector filtering into a common utility module rather than duplicating in several scripts.

## 16. Quick Start for a New Agent

Do not start by re-running the whole clean pipeline; it is expensive and the clean data already exists. Start with the current generated samples and outputs.

Suggested first checks:

```bash
python code/clean/01_run_pipeline.py --list
python code/analysis/did/02_run_did.py --sample municipalidades --did-only
python code/analysis/product_mix/02_run_product_mix_analysis.py
python code/analysis/bids/05_run_recent_activity_fe.py --activity bids --scope same_sector
```

Use the existing notes and outputs to avoid duplicating work:

```text
notes/empirics/product_mix/product_mix_note_260319.md
notes/simultaneousbids/results.md
output/product_mix/tables/bundling_did_comparison.csv
output/bids/tables/recent_bids_fe_results.csv
output/bids/tables/recent_wins_fe_results.csv
output/did/tables/did_results_ch_munic.csv
```

If expanding product-mix work, start with:

```text
code/analysis/product_mix/01_build_product_mix_sample.py
code/analysis/product_mix/02_run_product_mix_analysis.py
```

If expanding capacity/workload work, start with:

```text
code/analysis/bids/05_run_recent_activity_fe.py
code/analysis/simultaneousbids/06_did_fe.py
notes/simultaneousbids/results.md
```

If expanding the main DiD model, start with:

```text
code/analysis/did/did_utils.py
code/analysis/did/01_build_did_sample.py
code/analysis/did/02_run_did.py
code/analysis/did/05_heterogeneity_region.py
```
