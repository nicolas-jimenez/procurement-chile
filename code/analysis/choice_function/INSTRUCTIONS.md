# Choice Function Analysis: Instructions for Claude Code Agent

## Goal

Estimate a **buyer-level choice function** that models how each public buyer selects among competing bidders. The key research question: **to what degree do different buyers prefer same-region (local) bidders vs. simply choosing the lowest price?**

We want to quantify heterogeneity across buyers in their revealed preference for local firms, separately by procurement mechanism (licitaciones vs. compra agil) and by time period (pre- vs. post-December 2024 reform).

---

## Data Location and Setup

### Paths
All paths resolve through `code/config.py`, which reads from a `.env` file at the repo root. The key paths are:

- **Code root**: the `code/` directory containing `config.py`
- **Dropbox root**: set via `PROCUREMENT_CHILE_DB` env var
- **Input data**: `{DROPBOX_ROOT}/data/clean/combined_sii_merged_filtered.parquet`
- **Existing bid sample**: `{DROPBOX_ROOT}/output/bids/bid_analysis_sample.parquet`
- **DiD bid sample**: `{DROPBOX_ROOT}/output/did/samples/did_bid_sample.parquet`
- **UTM conversion**: `{DROPBOX_ROOT}/data/raw/other/utm_clp_2022_2025.csv`
- **Commune centroids**: `{DROPBOX_ROOT}/data/clean/comunas_centroids.csv`
- **Output for this analysis**: `{DROPBOX_ROOT}/output/choice_function/` (create this directory)

### How to import config
```python
import sys
from pathlib import Path
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1]))  # adjust depth to reach code/
from config import DATA_CLEAN, OUTPUT_ROOT, DATA_RAW_OTHER
```

### Key existing utilities
From `code/analysis/did/did_utils.py`, you can import:
- `COMBINED` — path to `combined_sii_merged_filtered.parquet`
- `REFORM_DATE = pd.Timestamp("2024-12-12")`
- `REFORM_PERIOD = pd.Period("2024-12", freq="M")`
- `load_utm_table()`, `add_utm_value()`, `assign_band()`

---

## Data Schema: What You Have

The main input is `combined_sii_merged_filtered.parquet` — one row per **bidder x tender** (line-level), covering both licitaciones and compra agil from 2022–2025. Key columns:

### Identifiers
| Column | Description |
|--------|-------------|
| `dataset` | `"licitaciones"` or `"compra_agil"` |
| `tender_id` | Unique tender/cotizacion code |
| `rut_bidder` | Bidder's RUT (numeric) |
| `dv_bidder` | Bidder's RUT check digit |
| `rut_bidder_raw` | Bidder RUT as raw string |
| `rut_unidad` | Buyer organization RUT (use as **buyer ID**) |

### Buyer characteristics
| Column | Description |
|--------|-------------|
| `region_buyer` | Buyer's administrative region (16 regions, format: "Region de X") |
| `comuna_buyer` | Buyer's commune/municipality |
| `sector` | Buyer sector (Health, Education, Defense, etc.) — may be missing for compra agil rows; fill using `rut_unidad_sector_crosswalk.parquet` |

### Bidder characteristics (from SII tax authority match)
| Column | Description |
|--------|-------------|
| `region` | Bidder's registered region (SII; format: "XV REGION ARICA Y PARINACOTA" — needs normalization via alias map) |
| `comuna` | Bidder's registered commune |
| `tramoventas` | Sales volume tranche (1–9 = SME, 10–13 = large) |
| `ntrabajadores` | Employee count |
| `rubro` | ISIC industry code |
| `tipodecontribuyente` | Firm type |
| `same_region` | **Pre-computed**: 1 if bidder region == buyer region, 0 otherwise |

### Tender/bid variables
| Column | Description |
|--------|-------------|
| `tipo` | Procurement type code (L1, LE, LP, LQ, LR for licitaciones) |
| `codigo_tipo` | 1=Public, 2=Private |
| `tipo_convocatoria` | 1=Open, 0=Closed |
| `monto_estimado` | Government's estimated cost (CLP) |
| `monto_oferta` | Bidder's submitted bid amount (CLP) |
| `is_selected` | 1 if this bidder was awarded the contract |
| `n_oferentes` | Number of bidders (licitaciones header-level count) |
| `fecha_pub` | Publication date |
| `source_year`, `source_month` | Data extraction period (use to merge UTM rates) |

### Variables you should derive
| Variable | How to compute |
|----------|---------------|
| `bidder_id` | Canonical `"{rut_bidder}-{dv_bidder}"` string (see `_build_bidder_id()` in `01_build_did_sample.py`) |
| `post` | `1 if fecha_pub >= pd.Timestamp("2024-12-12") else 0` |
| `monto_utm` | `monto_estimado / utm_clp` (merge UTM table on source_year, source_month) |
| `log_bid` | `log(monto_oferta)` |
| `log_est` | `log(monto_estimado)` |
| `log_bid_ratio` | `log(monto_oferta / monto_estimado)` |
| `is_lowest_bid` | Within each tender_id: `1 if monto_oferta == min(monto_oferta)` |
| `bid_rank` | Rank of bid within tender (1 = lowest) |
| `n_bidders` | Count of distinct bidders per tender_id (compute from data, don't rely solely on `n_oferentes`) |
| `sme` | From `tramoventas`: 1 if codes 1–9, 0 if 10–13, NA otherwise |

---

## Analysis to Implement

### Overview

For each **buyer** (`rut_unidad`), estimate a choice model where the dependent variable is `is_selected` (whether a particular bidder won the contract) and the independent variables capture bidder characteristics. This is essentially a **conditional logit** (McFadden's choice model) or a **linear probability model** at the bid level within each tender.

### Step 1: Build the Choice Sample

Create a script `code/analysis/choice_function/01_build_choice_sample.py`.

1. **Load** `combined_sii_merged_filtered.parquet` with the columns listed above.
2. **Filter** to awarded tenders only: keep tenders where at least one bidder has `is_selected == 1`. Drop tenders in cancelled/failed states if an `estado` or `estado_tender` column is available.
3. **Filter** to tenders with 2+ bidders (choice is only meaningful with competition).
4. **Compute derived variables**:
   - `is_lowest_bid`: within each `tender_id`, flag the bid(s) with the minimum `monto_oferta`.
   - `bid_rank`: rank bids within tender by `monto_oferta` (ascending; 1 = cheapest).
   - `log_bid_ratio`: `np.log(monto_oferta / monto_estimado)` — the markup over reference price.
   - `bid_discount`: `(monto_estimado - monto_oferta) / monto_estimado` — percentage discount.
   - `sme`: from `tramoventas` (1–9 → 1, 10–13 → 0).
   - `post`: `(fecha_pub >= "2024-12-12").astype(int)`.
   - `n_bidders`: count distinct bidders per tender.
   - `year_month`: `fecha_pub.dt.to_period("M")`.
5. **Split** by `dataset` into licitaciones and compra_agil subsamples. Save separately.
6. **Split** each by `post` into pre-reform and post-reform. Save separately.

Output files (parquet):
- `choice_sample_licitaciones_pre.parquet`
- `choice_sample_licitaciones_post.parquet`
- `choice_sample_compra_agil_pre.parquet`
- `choice_sample_compra_agil_post.parquet`
- `choice_sample_full.parquet` (combined, with all indicators)

### Step 1b: Sample Diagnostics (run before any regressions)

Before estimating anything, the build script should print and save a comprehensive diagnostics report to `{OUTPUT_ROOT}/choice_function/diagnostics/`. This ensures the regression sample is high quality and helps catch data problems early. Save all tables as CSVs and print summaries to stdout.

#### A. Sample construction funnel

Print a row-count funnel showing how many rows survive each filter, separately for licitaciones and compra_agil:

```
Raw combined file:                         N rows
  After dropping is_key_dup == True:       N rows
  After requiring fecha_pub not null:      N rows
  After requiring monto_oferta > 0:        N rows
  After requiring is_selected not null:    N rows
  After keeping awarded tenders only:      N rows  (tenders with >= 1 selected bidder)
  After requiring 2+ bidders per tender:   N rows
  After requiring same_region not null:    N rows  (SII match exists)
  Final choice sample:                     N rows
```

Report each step's drop count and percentage. Flag any step that drops more than 30% of remaining rows — that's a sign something may be off.

#### B. Coverage and missingness

For each variable used in the regressions, report:
- Count and share of non-missing values
- For `same_region`: what share of bids have an SII match? Does this differ by dataset, region, or time period?
- For `monto_oferta`: distribution of zeros and negatives (should be dropped)
- For `tramoventas` / `sme`: share with valid classification vs. NA
- For `sector`: share missing, especially in compra_agil (where it comes from the crosswalk)

Save as `diagnostics/variable_coverage.csv`.

#### C. Buyer-level sample depth

For each buyer (`rut_unidad`), compute:
- Number of awarded tenders (total, pre, post)
- Number of bids received across all tenders
- Average number of bidders per tender
- Share of tenders with a local winner (`is_selected == 1 & same_region == 1`)

Then report the **distribution of buyer sample sizes**: how many buyers have >= 5, >= 10, >= 20, >= 50, >= 100 awarded tenders? This determines whether buyer-level regressions are feasible and what the minimum-tender threshold should be.

Save as `diagnostics/buyer_sample_depth.csv` (one row per buyer) and `diagnostics/buyer_depth_summary.csv` (the distribution table).

#### D. Variation within tenders

For the choice model to be identified, there must be variation **within** each tender in the key regressors. Report:
- Share of tenders where `same_region` varies across bidders (i.e., at least one local and one non-local bidder). If most tenders have all-local or all-non-local bidders, the model won't identify the same_region effect well.
- Share of tenders where `sme` varies across bidders.
- Share of tenders where `is_lowest_bid == 1` and the winner is NOT the lowest bidder (this is the "interesting" variation — price wasn't the only criterion).

Report by dataset and pre/post. Save as `diagnostics/within_tender_variation.csv`.

#### E. Outcome diagnostics

- Tabulate `is_selected` by dataset: what share of bids are winners? (Should be roughly 1/n_bidders on average.)
- Check for tenders with multiple winners (`sum(is_selected) > 1` within a tender). If the data is at the line-item level, this is expected; if collapsed to tender level, it's a problem. Report counts.
- Cross-tab: `is_selected` × `is_lowest_bid`. What fraction of winners are the lowest bidder? Report by dataset and pre/post. This is a key descriptive — it tells us how much non-price selection is happening.
- Cross-tab: `is_selected` × `same_region`. What fraction of winners are local? Compare to the share of local bidders overall (to see if local firms win disproportionately).

Save as `diagnostics/outcome_crosstabs.csv`.

#### F. Price and bid-ratio distributions

- Report percentiles (1, 5, 25, 50, 75, 95, 99) of `monto_oferta`, `monto_estimado`, and `log_bid_ratio` by dataset.
- Flag extreme outliers: bids where `log_bid_ratio > 2` (bid is 7x the estimate) or `log_bid_ratio < -2` (bid is 14% of estimate). Report counts and consider winsorizing or dropping.
- Check for suspiciously round bids (e.g., `monto_oferta == monto_estimado` exactly — bidder may just be copying the reference price).

Save as `diagnostics/price_distributions.csv`.

#### G. Temporal coverage

- Count tenders and bids by `year_month` × `dataset`. Ensure there are no gaps in the time series.
- Verify the pre/post split: count observations on each side of December 12, 2024. Is the post period long enough for each dataset?
- For compra_agil specifically: since this mechanism expanded post-reform (tenders in the 30–100 UTM band moved from licitaciones to compra agil), the composition of compra agil tenders changes. Report the value distribution (`monto_utm`) pre vs. post.

Save as `diagnostics/temporal_coverage.csv`.

All diagnostics should be generated by `01_build_choice_sample.py` after building the sample. Print a summary to stdout and save detailed tables as CSVs. **Do not proceed to Step 2 if any red flags appear** (e.g., <1000 observations in a subsample, >50% missing same_region, no within-tender variation in same_region for most tenders).

---

### Step 2: Estimate Buyer-Level Choice Functions

Create a script `code/analysis/choice_function/02_estimate_choice_functions.py`.

For each subsample (licitaciones-pre, licitaciones-post, compra_agil-pre, compra_agil-post):

#### 2a. Pooled model (all buyers together)

Run a **linear probability model** (LPM) at the bid level:

```
is_selected_it = beta_1 * is_lowest_bid_it
               + beta_2 * same_region_it
               + beta_3 * log_bid_ratio_it
               + beta_4 * sme_it
               + beta_5 * (same_region * is_lowest_bid)_it
               + buyer_FE + year_month_FE + epsilon_it
```

Cluster standard errors at the buyer (`rut_unidad`) level.

Also estimate a **conditional logit** version where the choice set is the set of bidders within each tender:

```python
# Using statsmodels or pyfixest
# Group: tender_id (each tender is a "choice occasion")
# Dependent: is_selected
# Regressors: is_lowest_bid, same_region, log_bid_ratio, sme, interactions
```

#### 2b. Buyer-level estimates (the core deliverable)

For each buyer `rut_unidad` with sufficient observations (e.g., >= 20 awarded tenders), estimate a **separate** logit or LPM:

```
is_selected_it = alpha_j + beta_j1 * is_lowest_bid_it
                         + beta_j2 * same_region_it
                         + beta_j3 * log_bid_ratio_it
                         + epsilon_it
```

where `j` indexes the buyer.

Store the estimated coefficients `beta_j2` (the same-region preference) for each buyer. This is the **local preference parameter**.

**Alternative (preferred for small-sample buyers):** Instead of separate regressions, use a **random coefficients** or **correlated random effects** approach where `beta_2` (same_region coefficient) is allowed to vary by buyer:

```
is_selected_it = beta_1 * is_lowest_bid_it
               + (beta_2 + u_j) * same_region_it
               + beta_3 * log_bid_ratio_it
               + buyer_FE + epsilon_it
```

where `u_j` is the buyer-specific deviation in local preference. Estimate via:
1. Run the pooled LPM with buyer FE and `same_region * buyer_FE` interactions (this gives buyer-specific same_region coefficients directly).
2. Or: run pooled model, extract residuals, compute buyer-level correlation between residuals and same_region.

**Practical implementation with interactions:**
```python
# Create buyer × same_region interactions
# For buyers with >= N tenders (e.g., N=10 or N=20)
# Estimate: is_selected ~ is_lowest_bid + log_bid_ratio + sme 
#           + same_region:C(rut_unidad) + C(year_month) + C(rut_unidad)
# using pyfixest or statsmodels
```

The coefficient on `same_region` interacted with each buyer dummy gives the buyer-specific local preference.

#### 2c. Additional useful variables to include

Pull these from the data and include as controls or heterogeneity dimensions:

**Bid-level:**
- `log_bid_ratio` — markup over reference price
- `bid_rank` — ordinal rank of bid (1=lowest)
- `is_lowest_bid` — indicator for cheapest bid
- `bid_discount` — percentage below reference price
- `same_region` — core locality indicator
- `sme` — small/medium enterprise indicator
- `ntrabajadores` — bidder employee count (log-transform: `log(1 + ntrabajadores)`)

**Tender-level (compute and merge):**
- `n_bidders` — number of competing bidders
- `n_local_bidders` — count of same-region bidders in the tender
- `share_local_bidders` — fraction of bidders that are local
- `has_local_bidder` — indicator for whether any local bidder competed
- `local_is_cheapest` — indicator for whether the cheapest bid is from a local firm
- `log_monto_estimado` — log of estimated value
- `monto_utm` — value in UTM (to see if behavior differs by contract size)

**Buyer-level (compute and merge):**
- `buyer_n_tenders` — total number of tenders by this buyer (volume measure)
- `buyer_avg_n_bidders` — average competition level for this buyer
- `buyer_local_share_pre` — pre-reform share of contracts awarded to local firms (baseline preference)
- `buyer_sector` — sector (Health, Education, etc.)
- `buyer_region` — buyer's region

### Step 3: Summarize and Visualize

Create a script `code/analysis/choice_function/03_summarize_choice.py`.

1. **Distribution of local preference coefficients** across buyers:
   - Histogram of buyer-level `beta_j2` (same_region coefficient), separately for licitaciones and compra agil, and for pre/post.
   - Report mean, median, SD, interquartile range.
   - Flag buyers with statistically significant positive local preference vs. those with no preference or negative preference.

2. **Classify buyers** into types:
   - "Price-focused": `beta_j2` near zero or negative — these buyers select primarily on price.
   - "Local-preferring": `beta_j2` significantly positive — these buyers favor local firms beyond what price alone would predict.
   - Report the share of each type, by sector, region, and procurement mechanism.

3. **How did the reform change local preference?**
   - Compare the distribution of `beta_j2` pre vs. post for licitaciones (where the reform should not apply directly) vs. compra agil (where it may).
   - Test whether the mean/median local preference shifted.

4. **Correlates of local preference:**
   - Regress buyer-level `beta_j2` on buyer characteristics: sector dummies, region dummies, buyer_n_tenders, buyer_avg_n_bidders.
   - Which sectors/regions show strongest local preference?

5. **Output tables and figures** to `{OUTPUT_ROOT}/choice_function/tables/` and `{OUTPUT_ROOT}/choice_function/figures/`.

---

## Implementation Notes

### Region normalization
The `region` column (bidder's region from SII) uses roman numeral format like `"XV REGION ARICA Y PARINACOTA"`, while `region_buyer` uses `"Region de Arica y Parinacota"`. Both need normalization. See the `_REGION_ALIASES` dict and `_norm_region()` function in `code/analysis/bids/01_build_bid_sample.py` for the complete mapping. The `same_region` column is already pre-computed in the combined file and is reliable — use it directly.

### Handling multiple line items
A single tender can have multiple line items. The `tender_id` column identifies the tender, but rows may be at the line-item level. For the choice model, you likely want to collapse to **one row per bidder per tender** by summing `monto_oferta` across line items, or by working at the line-item level and clustering SEs at the tender level. Check whether `is_selected` varies within (tender_id, rut_bidder_raw) — if so, work at line-item level.

### Missing data patterns
- `same_region` requires a successful SII match. Missing for ~20-30% of rows. Use complete cases for the choice model.
- `monto_oferta` may be 0 or missing for some bids (especially in licitaciones where not all bidders have a valid "Valor Total Ofertado"). Drop these.
- `tramoventas` / `sme` will be NA for firms not in SII. Include a missing indicator or estimate models on the subsample with non-missing SII data.

### Computational considerations
The combined file has ~10M+ rows. For buyer-level estimation:
- First filter to tenders with `is_selected` information and 2+ bidders.
- This will reduce the sample substantially.
- For the buyer-level regressions, loop over buyers with >= 20 tenders. Expect ~500–2000 buyers depending on the subsample.
- Use `pyfixest` for FE estimation (already used in the codebase) or `statsmodels` for logit.

### Packages available
The codebase already uses: `pandas`, `numpy`, `pyarrow`, `pyfixest`, `statsmodels`, `matplotlib`, `scipy`. Install any additional packages as needed (e.g., `linearmodels` for panel models).

---

## File Structure

```
code/analysis/choice_function/
    __init__.py                          (empty)
    01_build_choice_sample.py            (data prep)
    02_estimate_choice_functions.py       (estimation)
    03_summarize_choice.py               (tables, figures, classification)

output/choice_function/
    samples/
        choice_sample_full.parquet
        choice_sample_licitaciones_pre.parquet
        choice_sample_licitaciones_post.parquet
        choice_sample_compra_agil_pre.parquet
        choice_sample_compra_agil_post.parquet
    diagnostics/
        sample_funnel.csv
        variable_coverage.csv
        buyer_sample_depth.csv
        buyer_depth_summary.csv
        within_tender_variation.csv
        outcome_crosstabs.csv
        price_distributions.csv
        temporal_coverage.csv
    estimates/
        pooled_results.csv
        buyer_level_coefficients.parquet   (one row per buyer with beta_j estimates)
    tables/
        pooled_lpm_results.tex
        buyer_preference_distribution.tex
        preference_by_sector.tex
    figures/
        hist_local_preference_licitaciones.png
        hist_local_preference_compra_agil.png
        hist_local_preference_pre_vs_post.png
        scatter_local_pref_vs_competition.png
        coefplot_pooled.png
```

---

## Summary of What We Want to Learn

1. **On average**, do buyers prefer local bidders after controlling for price? How much?
2. **Which buyers** have the strongest local preference? Which are purely price-driven?
3. **Did the December 2024 reform** change these preferences? Did it legitimize local preference in compra agil (where the new law explicitly allows it)?
4. **What predicts** local preference? Sector, region, buyer size, competition levels?
5. **Is local preference efficient?** Do local-preferring buyers pay more (higher bid ratios) for the same goods?
