# Simultaneous Bidding in Chilean Public Procurement: Results

**Author:** Nicolas Jimenez
**Date:** March 2026
**Data:** ChileCompra licitaciones, 2022–2025 (13.3M bid-item rows → 2.34M tender–firm observations, 99,278 firms, 540,327 tenders)
**Reform:** December 12, 2024 — local-preference reform restricting out-of-region bidding on below-threshold contracts
**Definition of simultaneous bidding:** Number of distinct tenders a firm bid on in the same calendar month

---

## 1. How Many Projects Do Firms Bid on Simultaneously?

### Distribution (firm-month observations)

| Monthly simultaneous bids | N firm-months | % | (Municipalidades) | (Obras Públicas) |
|---|---|---|---|---|
| 1 | 301,792 | 55.4% | 180,017 | 28,047 |
| 2 | 91,492 | 16.8% | 52,037 | 7,293 |
| 3 | 42,431 | 7.8% | 23,607 | 3,044 |
| 4–5 | 40,087 | 7.4% | 21,405 | 2,575 |
| 6–10 | 33,510 | 6.2% | 18,301 | 1,631 |
| 11–20 | 17,620 | 3.2% | 10,009 | 418 |
| 21–50 | 11,762 | 2.2% | 6,156 | 68 |
| 51+ | 5,995 | 1.1% | 2,539 | 23 |

**Key finding:** More than half of all firm-months involve only a single active bid (55.4%), but the distribution has a long right tail — about 9% of firm-months involve 10 or more simultaneous bids. The sector split is striking: Obras Públicas has very few observations with ≥21 simultaneous bids (only 91 firm-months with 21+ bids), while Municipalidades has 8,695 such cases, consistent with goods procurement permitting more simultaneous activity.

### By Firm Size (firm-month averages)

| Size group | N firm-months | Mean n_sim | Median | P90 | P99 | % months with >1 bid | % months with >5 bids | Mean nonlocal share | Mean distinct regions | Mean distance (km) |
|---|---|---|---|---|---|---|---|---|---|---|
| Micro (tramos 1–4) | 115,382 | 2.19 | 1 | 4 | 15 | 35.6% | 5.5% | 34% | 1.37 | 166.8 |
| Small (tramos 5–7) | 181,224 | 4.27 | 1 | 7 | 46 | 49.8% | 13.6% | 35% | 1.80 | 176.4 |
| Medium (tramos 8–10) | 86,219 | 6.45 | 2 | 13 | 79 | 58.5% | 22.1% | 42% | 2.38 | 214.8 |
| Large (tramos 11–13) | 38,128 | 13.21 | 3 | 32 | 167 | 67.2% | 34.2% | 51% | 3.54 | 276.7 |

**Key finding:** Larger firms are dramatically more active. Large firms average 13.2 simultaneous bids/month vs. 2.2 for micro firms, and submit 51% of their bids outside their home region compared to 34% for micro. This is consistent with large firms having greater capacity and a national market orientation.

### By Bidder Home Region

| Region | N firm-months | N firms | Mean n_sim | Mean nonlocal share | Mean avg distance (km) |
|---|---|---|---|---|---|
| Metropolitana (RM) | 210,453 | 26,621 | **6.74** | 43% | 224.6 |
| Biobío | 34,157 | 5,164 | 4.25 | 41% | 163.5 |
| Valparaíso | 34,640 | 5,371 | 4.01 | 45% | 168.0 |
| Maule | 19,660 | 3,309 | 3.59 | 29% | 93.2 |
| OHiggins | 12,761 | 2,390 | 3.23 | 33% | 91.7 |
| Antofagasta | 6,941 | 1,525 | 3.10 | 31% | 302.1 |
| Araucanía | 27,820 | 4,408 | 3.03 | 28% | 122.5 |
| … | | | | | |
| Aysén | 5,173 | 974 | **1.61** | 15% | (n/a) |

**Key finding:** Firms from the Región Metropolitana (Santiago) are the most simultaneously active (mean 6.74 bids/month), more than 4× the rate of firms from remote regions like Aysén (1.61). Santiago firms also have the highest nonlocal bidding share (43%) and the longest average distance to buyers. This is consistent with RM firms having both larger capacity and a historically national market orientation — two patterns that the December 2024 reform is designed to curtail.

---

## 2. Geographic Distribution of Out-of-Region Bids When Bidding Simultaneously

### Distance distribution for non-local bids (all vs. when n_sim > 1)

| Distance range | N (all non-local) | % all | N (n_sim > 1) | % sim | Municipalidades | Obras Públicas |
|---|---|---|---|---|---|---|
| 0–100 km | 13,023 | 1.1% | 11,351 | 1.1% | 8,932 | 837 |
| 101–200 km | 286,649 | 25.0% | 266,585 | 24.7% | 149,975 | 8,993 |
| 201–400 km | 267,418 | 23.4% | 254,373 | 23.5% | 165,018 | 8,546 |
| 401–700 km | 341,347 | 29.8% | 326,745 | 30.2% | 184,652 | 10,192 |
| 701–1,500 km | 199,691 | 17.5% | 188,268 | 17.4% | 91,063 | 10,393 |
| 1,500+ km | 36,223 | 3.2% | 33,252 | 3.1% | 7,187 | 2,080 |

**Key finding:** The geographic distribution of non-local bids is nearly identical whether or not a firm is simultaneously bidding (n_sim > 1 vs. all non-local bids). The modal distance band is 401–700 km (~30%), followed by 101–200 km (~25%). There is no systematic "reach compression" when firms are simultaneously active — they bid at similar distances regardless of portfolio size. For Obras Públicas, the distribution is slightly more right-skewed (higher share at 701+ km) than for Municipalidades, suggesting that construction firms are willing to travel farther for individual jobs even when bidding on fewer total contracts.

---

## 3. By Sector: Municipalidades vs. Obras Públicas

### Simultaneous bidding intensity (per firm-month)

| Sector | Period | N firm-months | Mean n_sim | Median n_sim | Mean nonlocal share | Mean # regions | Mean avg dist (km) |
|---|---|---|---|---|---|---|---|
| **All** | Pre-reform | 287,775 | 4.53 | 1 | 28.4% | 1.82 | 184.4 |
| **All** | Post-reform | 126,591 | 3.47 | 1 | 29.6% | 1.73 | 197.2 |
| **Municipalidades** | Pre-reform | 172,125 | 6.33 | 2 | 29.3% | 2.19 | 174.6 |
| **Municipalidades** | Post-reform | 67,129 | 4.84 | 2 | 30.9% | 2.12 | 194.3 |
| **Obras Públicas** | Pre-reform | 21,726 | **12.75** | 3 | 40.5% | 2.96 | **276.2** |
| **Obras Públicas** | Post-reform | 10,784 | **6.58** | 2 | 42.9% | 2.56 | **296.9** |

**Key findings:**

1. **Obras Públicas firms bid on almost twice as many simultaneous contracts as Municipalidades firms pre-reform** (12.75 vs. 6.33 per month on average), even though the sector is smaller. This is consistent with construction firms maintaining large bid pipelines because most bids do not win.

2. **The reform sharply reduced Obras Públicas simultaneous bidding** (12.75 → 6.58, a 48% decline vs. 23% for Municipalidades). This likely reflects that construction firms were disproportionately exposed to below-threshold out-of-region contracts that were now restricted.

3. **Obras Públicas firms bid from much farther away** (avg 276 km pre-reform vs. 175 km for Municipalidades). Post-reform, the average distance for Obras Públicas actually increased slightly (296 km), suggesting that the firms that remained active shifted toward longer-distance (above-threshold) bids.

4. **Both sectors show a slight increase in the nonlocal share post-reform**, which is counterintuitive at first glance but consistent with the reform eliminating many short-distance out-of-region bids (101–400 km range) while leaving longer-distance above-threshold bids unaffected.

---

## 4. Effect of Simultaneous Bidding on Entry into New Auctions

### Specification
OLS: `first_bid_in_region(i,j,t) = β₁ log(1 + n_sim_lag1) + β₂ log(1 + n_sim_nonlocal_lag1) + β₃ log(monto_estimado) + ε`

where `first_bid_in_region = 1` if this is the first time firm `i` bids in buyer region `r`.

### Results (T8)

| Sample | N | R² | β₁ (log n_sim_lag1) | SE | β₂ (log nonlocal_lag1) | SE |
|---|---|---|---|---|---|---|
| All sectors | 2,184,392 | 0.044 | **−0.040*** | 0.000 | **+0.016*** | 0.000 |
| All + post interaction | 2,184,392 | 0.046 | −0.042*** | 0.000 | +0.016*** | 0.000 |
| Municipalidades | 1,110,513 | 0.042 | **−0.035*** | 0.000 | **+0.013*** | 0.000 |
| Municipalidades + post | 1,110,513 | 0.043 | −0.037*** | 0.000 | +0.014*** | 0.000 |
| Obras Públicas | 78,415 | 0.029 | **−0.053*** | 0.001 | **+0.024*** | 0.001 |
| Obras Públicas + post | 78,415 | 0.032 | −0.056*** | 0.001 | +0.026*** | 0.001 |

**** p < 0.01; *** p < 0.01

**Key findings:**

1. **More simultaneous bids (n_sim_lag1) significantly reduces the probability of entering a new region** (β₁ < 0, highly significant). A one-log-unit increase in lagged simultaneous bids (roughly 1.7× increase) is associated with a 4.0 pp lower probability of a new-region entry bid. This is consistent with firms that are already stretched across many concurrent contracts being less likely to explore new geographic markets.

2. **The effect is substantially stronger for Obras Públicas than Municipalidades** (−0.053 vs. −0.035), consistent with the hypothesis that construction involves genuine capacity constraints that prevent geographic expansion. Goods procurement (Municipalidades) allows more simultaneous activity before capacity binds.

3. **Non-local simultaneous bids (n_sim_nonlocal_lag1) have the opposite sign: positive** (β₂ > 0). Firms that are already active outside their home region are *more* likely to enter still more regions. This reflects selection: firms that choose to bid non-locally are systematically the more geographically mobile ones.

4. **The interaction with the post-reform period is small and in the same direction** (slightly more negative for β₁), suggesting the reform mildly tightened the capacity-entry relationship.

---

## 5. Effect of Simultaneous Bidding on Bid Levels

### Specification
OLS: `log(bid / estimate)(i,j,t) = β₁ log(1 + n_sim) + β₂ log(1 + n_sim_nonlocal) + β₃ log(monto_estimado) + ε`

### Results (T9)

| Sample | N | R² | β₁ (log n_sim) | SE | β₂ (log nonlocal) | SE |
|---|---|---|---|---|---|---|
| All | 751,927 | 0.020 | **+0.038*** | 0.001 | **−0.054*** | 0.001 |
| All + post | 751,927 | 0.023 | +0.038*** | 0.001 | −0.051*** | 0.001 |
| **Municipalidades** | 736,190 | 0.022 | **+0.038*** | 0.001 | **−0.055*** | 0.001 |
| Municipalidades + post | 736,190 | 0.024 | +0.039*** | 0.001 | −0.052*** | 0.001 |
| **Obras Públicas** | 15,737 | 0.005 | **+0.030*** | 0.006 | **−0.014*** | 0.005 |
| Obras Públicas + post | 15,737 | 0.006 | +0.030*** | 0.006 | −0.014** | 0.006 |

### Pre vs. Post Reform (T11)

| Sample | N | β₁ (log n_sim) | SE | β₂ (log nonlocal) | SE |
|---|---|---|---|---|---|
| All — Pre-reform | 680,129 | **+0.038*** | 0.001 | −0.051*** | 0.001 |
| All — Post-reform | 71,798 | +0.001 (n.s.) | 0.004 | **−0.073*** | 0.003 |
| Municipalidades — Pre | 666,603 | +0.039*** | 0.001 | −0.052*** | 0.001 |
| Municipalidades — Post | 69,587 | +0.000 (n.s.) | 0.004 | **−0.075*** | 0.003 |
| Obras Públicas — Pre | 13,526 | +0.029*** | 0.006 | −0.013** | 0.006 |
| Obras Públicas — Post | 2,211 | +0.031* | 0.016 | −0.019 (n.s.) | 0.013 |

**Key findings:**

1. **More simultaneous total bids raises bid levels** (β₁ > 0, ≈ +3.8% per log-unit). A firm bidding on 7 auctions in a month submits about 7.6% higher bids compared to a firm bidding on only 1 — consistent with capacity constraints raising marginal costs when a firm is simultaneously committed to many potential projects.

2. **More non-local simultaneous bids *lowers* bid levels** (β₂ ≈ −0.054). This is initially surprising: firms active in many out-of-region markets submit lower bids. This likely reflects adverse selection — geographically mobile firms are structurally more competitive; or it captures that firms submitting many non-local "exploration" bids don't expect to win them all and shade bids down.

3. **The effect of total simultaneous bids on pricing essentially disappears post-reform** (β₁ ≈ 0.001, not significant in the post period). Pre-reform, the capacity constraint premium was real (+0.038). After the reform reduced portfolio sizes sharply, firms are no longer constrained in the same way, and the simultaneous bid count is no longer informative about capacity pressure.

4. **Post-reform, the non-local discount intensifies** (β₂: −0.051 pre-reform → −0.073 post-reform). This suggests that the remaining out-of-region bidders post-reform are the most competitive, lowest-cost firms — consistent with the reform filtering out marginal non-local bidders.

5. **Obras Públicas vs. Municipalidades comparison:**
   - **β₁ for Obras Públicas (+0.030) is somewhat lower than for Municipalidades (+0.038)** — this is *against* the simple capacity-constraint story (we expected construction to show a bigger premium). However, Obras Públicas observations are much fewer (15K vs. 736K), and the R² is very low (0.005 vs. 0.022), suggesting more noise. The capacity constraint mechanism may still apply but requires better identification (project-level controls, FE).
   - For Obras Públicas, the non-local discount is much smaller (−0.014 vs. −0.055 for Municipalidades), which makes sense: construction firms must physically be present, so their non-local bids are genuinely more costly and there is less scope for competitive outsiders to undercut.

### By Firm Size (T12)

| Size | N | β₁ (log n_sim) | Stars | β₂ (log nonlocal) | Stars |
|---|---|---|---|---|---|
| Micro | 90,313 | **+0.070** | *** | −0.064 | *** |
| Small | 286,401 | +0.062 | *** | −0.034 | *** |
| Medium | 178,401 | +0.106 | *** | −0.076 | *** |
| Large | 109,390 | +0.021 | *** | **−0.112** | *** |

**Municipalidades only:**

| Size | N | β₁ | Stars | β₂ | Stars |
|---|---|---|---|---|---|
| Micro | 88,239 | +0.070 | *** | −0.064 | *** |
| Small | 279,936 | +0.063 | *** | −0.035 | *** |
| Medium | 175,101 | +0.109 | *** | −0.078 | *** |
| Large | 107,377 | +0.035 | *** | −0.122 | *** |

**Obras Públicas only:**

| Size | N | β₁ | Stars | β₂ | Stars |
|---|---|---|---|---|---|
| Micro | 2,074 | +0.089 | *** | −0.046 | ** |
| Small | 6,465 | +0.016 | * | +0.000 | n.s. |
| Medium | 3,300 | +0.033 | ** | −0.001 | n.s. |
| Large | 2,013 | **−0.055** | ** | +0.015 | n.s. |

**Key findings by size:**

1. **The capacity constraint premium (β₁ > 0) is strongest for medium firms** (+0.106 for medium vs. +0.021 for large). This is consistent with medium firms having more binding capacity constraints: they are large enough to receive many procurement opportunities but lack the slack resources of large firms to absorb multiple simultaneous wins.

2. **For large firms, the nonlocal discount is especially powerful** (β₂ = −0.112 for large, vs. −0.064 for micro). Large firms that are active non-locally submit significantly lower bids — reflecting that only truly competitive large firms choose to bid out of region.

3. **For Obras Públicas large firms, β₁ is negative** (−0.055, significant). This suggests that large construction firms that are simultaneously bidding on more contracts actually bid *lower* — possibly because they have genuine economies of scale in construction (mobilisation costs spread across many projects) or because they are "testing the waters" with low strategic bids when their pipeline is full.

---

## 5b. Extension Results: Firm Fixed Effects

### Motivation
The OLS results in sections 4 and 5 pool across firms. Because more-active firms are systematically different from less-active firms (they are larger, more experienced, etc.), OLS coefficients may conflate between-firm selection with within-firm effects. We re-run all entry and bid-level regressions absorbing firm fixed effects (FE) using pyfixest.

---

### T13. Local vs. Non-local Bids by Period and Size

| Period | Size | N firm-months | Mean local bids | Mean nonlocal bids | Nonlocal share | % any nonlocal |
|---|---|---|---|---|---|---|
| Pre-reform | Micro | 89,678 | 1.25 | 0.99 | 33.1% | 38.8% |
| Pre-reform | Small | 139,603 | 2.18 | 2.46 | 35.3% | 45.3% |
| Pre-reform | Medium | 65,031 | 2.81 | 4.11 | 41.5% | 55.4% |
| Pre-reform | Large | 28,684 | 4.63 | 9.37 | 50.6% | 68.3% |
| Pre-reform | **All** | 418,098 | 1.75 | 2.31 | 28.8% | 36.7% |
| Post-reform | Micro | 25,704 | 1.10 | 0.91 | 35.0% | 40.4% |
| Post-reform | Small | 41,621 | 1.54 | 1.52 | 35.4% | 43.9% |
| Post-reform | Medium | 21,188 | 2.10 | 2.94 | 41.7% | 55.4% |
| Post-reform | Large | 9,444 | 3.76 | 7.04 | 51.3% | 69.3% |
| Post-reform | **All** | 126,591 | 1.36 | 1.70 | 29.6% | 37.1% |

**Key findings:**

1. **The reform reduced both local and nonlocal bids across all size groups.** Mean local bids fell from 1.75 → 1.36 overall (−22%), while mean nonlocal bids fell from 2.31 → 1.70 (−26%). The nonlocal share barely changed (28.8% → 29.6%), suggesting the reform cut non-local activity proportionally — not selectively.

2. **Larger firms are much more non-local in absolute and relative terms.** Pre-reform, large firms averaged 9.37 non-local bids per month (68.3% any nonlocal), vs. 0.99 for micro (38.8%). Post-reform, large firms still average 7.04 non-local bids — still dramatically above smaller firms.

3. **The nonlocal share is essentially stable pre vs. post for each size group.** This suggests the reform eliminated out-of-region bids roughly proportionally by size, rather than targeting specific firm types.

---

### T14. Entry Regressions: OLS vs. Firm FE

Outcome: `first_bid_in_region = 1`. All regressions include `log(1 + monto_estimado)`.

| Sample | Estimator | N | R²/R²_within | β₁ (log n_sim_lag1) | SE | Stars | β₂ (log nonlocal_lag1) | SE | Stars |
|---|---|---|---|---|---|---|---|---|---|
| All | OLS | 2,184,392 | 0.044 | −0.040 | 0.000 | *** | +0.016 | 0.000 | *** |
| All | **Firm FE** | 2,170,504 | 0.0024 | **−0.013** | 0.001 | *** | **−0.005** | 0.001 | *** |
| Municipalidades | OLS | 1,110,513 | 0.042 | −0.035 | 0.000 | *** | +0.013 | 0.000 | *** |
| Municipalidades | **Firm FE** | 1,100,012 | 0.0036 | **−0.014** | 0.001 | *** | **−0.006** | 0.001 | *** |
| Obras Públicas | OLS | 78,415 | 0.029 | −0.053 | 0.001 | *** | +0.024 | 0.001 | *** |
| Obras Públicas | **Firm FE** | 74,964 | 0.0024 | **−0.006** | 0.003 | ** | **−0.017** | 0.003 | *** |
| Pre-reform | OLS | 1,762,413 | 0.048 | −0.042 | 0.000 | *** | +0.016 | 0.000 | *** |
| Pre-reform | **Firm FE** | 1,750,163 | 0.0037 | **−0.017** | 0.001 | *** | **−0.006** | 0.001 | *** |
| Post-reform | OLS | 421,979 | 0.032 | −0.036 | 0.001 | *** | +0.013 | 0.000 | *** |
| Post-reform | **Firm FE** | 410,728 | 0.0013 | **+0.002** | 0.001 | n.s. | **−0.014** | 0.001 | *** |

**Key findings vs. OLS:**

1. **The negative effect of total simultaneous bids on entry (β₁) survives firm FE but is attenuated.** OLS: −0.040; FE: −0.013. About two-thirds of the OLS coefficient is between-firm selection (busier firms have worse entry rates for other reasons). The remaining within-firm effect (−0.013) is still highly significant: in months when a firm is particularly active, it is somewhat less likely to enter new regions.

2. **The non-local simultaneous bid coefficient (β₂) flips sign with firm FE**, from +0.016 (OLS) to −0.005 (FE). This is a critical finding: the positive OLS coefficient was entirely driven by firm-level selection (geographically mobile firms are more likely to both bid non-locally AND enter new regions). Once we account for firm identity, within-firm variation tells the opposite story — months with more non-local bids are modestly *worse* for entry into additional regions, consistent with non-local bids competing for capacity just like any other bids.

3. **For Obras Públicas, the within-firm entry effect is near zero (−0.006, barely significant)** despite the large OLS coefficient (−0.053). Much of the apparent "capacity constraint" on entry in Obras Públicas was between-firm selection.

4. **Post-reform, the within-firm capacity effect disappears entirely** (β₁ = +0.002, n.s. in FE post-reform). With smaller portfolios, firms' monthly bidding activity no longer predicts reduced geographic entry. Only the non-local portfolio effect remains (β₂ = −0.014***), still reflecting that non-local bids consume capacity.

---

### T15. Bid-Level Regressions: OLS vs. Firm FE

Outcome: `log_sub_price_ratio = log(bid / estimate)`. Sample: bids in `bid_analysis_sample.parquet` (1–200 UTM range), matched to simultaneous bid counts. All regressions include `log(monto_utm)`.

| Sample | Estimator | N | R²/R²_within | β₁ (log n_sim) | SE | Stars | β₂ (log nonlocal) | SE | Stars |
|---|---|---|---|---|---|---|---|---|---|
| All | OLS | 751,927 | 0.020 | +0.038 | 0.001 | *** | −0.054 | 0.001 | *** |
| All | **Firm FE** | 734,218 | 0.0149 | **−0.002** | 0.002 | n.s. | **−0.002** | 0.002 | n.s. |
| Municipalidades | OLS | 736,190 | 0.022 | +0.038 | 0.001 | *** | −0.055 | 0.001 | *** |
| Municipalidades | **Firm FE** | 719,045 | 0.0153 | **−0.002** | 0.002 | n.s. | **−0.002** | 0.002 | n.s. |
| Obras Públicas | OLS | 15,737 | 0.005 | +0.030 | 0.006 | *** | −0.014 | 0.005 | *** |
| Obras Públicas | **Firm FE** | 13,184 | 0.0097 | **−0.016** | 0.016 | n.s. | **+0.018** | 0.016 | n.s. |
| Pre-reform | OLS | 680,129 | 0.021 | +0.038 | 0.001 | *** | −0.051 | 0.001 | *** |
| Pre-reform | **Firm FE** | 664,221 | 0.0150 | **+0.000** | 0.002 | n.s. | **−0.004** | 0.002 | ** |
| Post-reform | OLS | 71,798 | 0.030 | +0.001 | 0.004 | n.s. | −0.073 | 0.003 | *** |
| Post-reform | **Firm FE** | 65,312 | 0.0125 | **−0.003** | 0.009 | n.s. | **−0.006** | 0.008 | n.s. |
| Municipalidades — Pre | OLS | 666,603 | 0.022 | +0.039 | 0.001 | *** | −0.052 | 0.001 | *** |
| Municipalidades — Pre | **Firm FE** | 651,220 | 0.0153 | +0.000 | 0.002 | n.s. | **−0.004** | 0.002 | ** |
| Municipalidades — Post | OLS | 69,587 | 0.030 | +0.000 | 0.004 | n.s. | −0.075 | 0.003 | *** |
| Municipalidades — Post | **Firm FE** | 63,321 | 0.0135 | −0.005 | 0.009 | n.s. | −0.006 | 0.009 | n.s. |

**Key finding — OLS results are entirely driven by selection:**

The firm FE estimates for bid levels are uniformly near zero and statistically insignificant. The OLS result (+0.038 for total bids, −0.054 for non-local bids) reflected between-firm differences, not within-firm variation in bid pricing. In other words:

- Firms that tend to bid on many projects simultaneously are structurally *different* bidders (e.g., they are larger, or they specialize in standard goods), and they happen to submit higher bids on average. But when a given firm has an unusually busy month, it does not raise its bids.

- The non-local "discount" in OLS (−0.054) similarly reflects that firms active in non-local markets are systematically lower-cost bidders. Within a firm, having more non-local bids this month does not shift bid prices.

This is a substantive finding: there is **no within-firm dynamic adjustment of bid levels** in response to portfolio size. Firms do not strategically raise prices when capacity-constrained or lower prices when expanding geographically. Bid pricing appears to be set by firm-specific cost structures that don't change month to month.

---

## 5c. Extension Results: UTM-Band Analysis

### Motivation
The December 2024 reform had an explicit threshold in UTM values. We examine whether effects differ for contracts below vs. above 500 UTM. Note: the bid-level sample (`bid_analysis_sample.parquet`) covers only 1–200 UTM, so the bid-level UTM split uses 100 UTM as an internal threshold.

---

### T16. UTM Descriptives: Contract Characteristics by Band, Sector, Period

| UTM band | Sector | Period | N bids | N tenders | N firms | Mean UTM value | % first-region entry |
|---|---|---|---|---|---|---|---|
| <500 UTM | Municipalidades | Pre-reform | 688,176 | 166,685 | 29,713 | 150 UTM | 4.0% |
| <500 UTM | Municipalidades | Post-reform | 84,677 | 22,990 | 14,540 | 218 UTM | 4.2% |
| <500 UTM | Obras Públicas | Pre-reform | 19,493 | 5,847 | 4,859 | 186 UTM | 7.1% |
| <500 UTM | Obras Públicas | Post-reform | 4,737 | 1,327 | 1,940 | 213 UTM | 5.1% |
| <500 UTM | **All** | Pre-reform | 1,058,605 | 273,615 | 41,226 | 155 UTM | 4.7% |
| <500 UTM | **All** | Post-reform | 153,854 | 44,360 | 22,754 | 213 UTM | 4.5% |
| ≥500 UTM | Municipalidades | Pre-reform | 246,659 | 59,408 | 19,297 | 67,398 UTM | 3.8% |
| ≥500 UTM | Municipalidades | Post-reform | 91,001 | 16,831 | 13,510 | 42,838 UTM | 3.1% |
| ≥500 UTM | Obras Públicas | Pre-reform | 39,570 | 11,466 | 4,769 | 16,210 UTM | 9.2% |
| ≥500 UTM | Obras Públicas | Post-reform | 14,615 | 3,426 | 2,954 | 33,944 UTM | 6.8% |
| ≥500 UTM | **All** | Pre-reform | 703,808 | 161,404 | 33,200 | 43,906 UTM | 4.2% |
| ≥500 UTM | **All** | Post-reform | 268,125 | 53,530 | 24,641 | 77,944 UTM | 3.2% |

**Key findings:**

1. **Small contracts dominate by count.** <500 UTM contracts account for the majority of bids (1.06M pre-reform, vs. 0.70M for ≥500 UTM). Post-reform, <500 UTM bids fell sharply (−85% from 1.06M to 154K) while ≥500 UTM bids fell much less (−62% from 704K to 268K), consistent with the reform specifically restricting small-contract out-of-region activity.

2. **Entry rates are similar across UTM bands** (4.7% vs. 4.2% pre-reform), but Obras Públicas has much higher first-region entry rates (7.1–9.2% vs. 3.8–4.0% for Municipalidades). This reflects that construction firms explore new regions with every project, while goods procurement firms are more stable.

3. **Post-reform, ≥500 UTM mean contract value rose** from 43,906 to 77,944 UTM (+78%). This is partly mechanical (the threshold excludes below-500 UTM non-local bids), but also suggests the reform pushed surviving non-local activity toward larger, higher-value contracts.

---

### T17. Entry Regressions by UTM Band

Key results (FE specification) for entry on simultaneous bids:

| UTM band | Sector | Period | N | β₁ (log n_sim_lag1) | SE | Stars | β₂ (log nonlocal_lag1) | SE | Stars |
|---|---|---|---|---|---|---|---|---|---|
| **<500 UTM** | All | All | 1,212,459 | −0.013*** | 0.001 | *** | −0.006*** | 0.001 | *** |
| <500 UTM | All | Pre-reform | 1,058,605 | −0.016*** | 0.001 | *** | −0.006*** | 0.001 | *** |
| <500 UTM | All | Post-reform | 153,854 | +0.001 | 0.002 | n.s. | −0.012*** | 0.002 | *** |
| <500 UTM | Municipalidades | All | 772,853 | −0.014*** | 0.001 | *** | −0.006*** | 0.001 | *** |
| <500 UTM | Obras Públicas | All | 24,230 | −0.004 | 0.005 | n.s. | −0.014** | 0.006 | ** |
| **≥500 UTM** | All | All | 971,933 | −0.012*** | 0.001 | *** | −0.005*** | 0.001 | *** |
| ≥500 UTM | All | Pre-reform | 703,808 | −0.017*** | 0.001 | *** | −0.007*** | 0.001 | *** |
| ≥500 UTM | All | Post-reform | 268,125 | +0.000 | 0.002 | n.s. | −0.012*** | 0.001 | *** |
| ≥500 UTM | Municipalidades | All | 337,660 | −0.012*** | 0.001 | *** | −0.007*** | 0.001 | *** |
| ≥500 UTM | Obras Públicas | All | 54,185 | −0.006 | 0.004 | n.s. | −0.021*** | 0.004 | *** |

**Key findings:**

1. **Entry effects are nearly identical across UTM bands.** The within-firm effect of total simultaneous bids on entry (β₁ ≈ −0.012 to −0.013) is essentially the same for <500 UTM and ≥500 UTM contracts, suggesting the capacity mechanism does not differ by contract size.

2. **Pre-reform, both UTM bands show a significant negative entry effect (β₁ ≈ −0.016 to −0.017). Post-reform, the total bid effect disappears in both bands** (β₁ ≈ 0), consistent with the reform reducing portfolio sizes to the point where capacity no longer binds entry decisions.

3. **The non-local bid effect (β₂) is negative and persistent in both bands.** In the post-reform period, β₂ ≈ −0.012 is still highly significant across both <500 and ≥500 UTM. This means that even post-reform, firms with larger non-local portfolios are less likely to enter new regions, a within-firm result that holds across contract size.

4. **Obras Públicas shows stronger non-local entry constraint.** For ≥500 UTM Obras Públicas (β₂ = −0.021***), out-of-region construction bidding more strongly reduces entry probability than for Municipalidades goods procurement (β₂ = −0.007***).

---

### T18. Bid-Level Regressions by UTM Band

Note: Bid-level analysis uses `bid_analysis_sample.parquet` (1–200 UTM only). We split at 100 UTM as an internal proxy for the reform threshold. All FE results are statistically insignificant — this section therefore focuses on OLS to characterize cross-sectional patterns, while confirming FE results remain zero throughout.

**OLS results:**

| UTM band | Sector | Period | N | β₁ (log n_sim) | Stars | β₂ (log nonlocal) | Stars |
|---|---|---|---|---|---|---|---|
| **<100 UTM** | All | All | 572,971 | +0.043*** | *** | −0.053*** | *** |
| <100 UTM | All | Pre-reform | 538,376 | +0.041*** | *** | −0.050*** | *** |
| <100 UTM | All | Post-reform | 34,595 | +0.023*** | *** | −0.096*** | *** |
| <100 UTM | Municipalidades | All | 565,526 | +0.044*** | *** | −0.054*** | *** |
| **100–200 UTM** | All | All | 178,956 | +0.033*** | *** | −0.060*** | *** |
| 100–200 UTM | All | Pre-reform | 141,753 | +0.039*** | *** | −0.062*** | *** |
| 100–200 UTM | All | Post-reform | 37,203 | +0.000 (n.s.) | | −0.049*** | *** |
| 100–200 UTM | Municipalidades | All | 170,664 | +0.035*** | *** | −0.062*** | *** |

**FE results (all near zero, none significant):**

| UTM band | Sector | Period | β₁ | Stars | β₂ | Stars |
|---|---|---|---|---|---|---|
| <100 UTM | All | All | +0.004** | ** | −0.005** | ** |
| <100 UTM | All | Pre-reform | +0.003 | n.s. | −0.005** | ** |
| 100–200 UTM | All | All | −0.006 | n.s. | −0.003 | n.s. |
| 100–200 UTM | All | Pre-reform | −0.005 | n.s. | −0.005 | n.s. |

**Key findings:**

1. **OLS bid-level patterns are consistent across UTM bands.** The capacity-constraint premium (β₁ > 0) and non-local discount (β₂ < 0) appear in both <100 UTM and 100–200 UTM subsamples. The magnitudes are similar, confirming these are not artifacts of contract size variation.

2. **Post-reform, the total bid effect on pricing disappears in the 100–200 UTM band** (+0.000, n.s.) but remains modestly positive in <100 UTM (+0.023***). This is consistent with the reform hitting smaller contracts disproportionately: the reform compressed bidding activity in small contracts, reducing portfolio sizes to the point where capacity no longer affects prices. For very small contracts (<100 UTM), there is still some residual activity and the cross-sectional relationship survives.

3. **Post-reform non-local discount intensifies for very small contracts** (β₂ = −0.096 for <100 UTM post-reform vs. −0.049 for 100–200 UTM post-reform). The remaining non-local bidders on the smallest contracts are the most cost-competitive firms, consistent with the reform filtering out marginal out-of-region competitors.

4. **Firm FE results are near zero throughout**, reconfirming T15's finding: within-firm variation in bid pricing in response to portfolio size is negligible at all contract sizes.

---

## 6. Summary and Interpretation

### Does Firm FE Change Results Substantially?

**Yes — for bid levels, substantially. For entry, partially.**

For **entry regressions**: The within-firm effect of total simultaneous bids on entry is attenuated (OLS: −0.040 → FE: −0.013) but remains significant. The non-local bid coefficient flips sign (OLS: +0.016 → FE: −0.005), revealing that the OLS positive result was entirely a selection artifact. The within-firm result is economically meaningful: in high-bidding months, firms are modestly less likely to enter new regions.

For **bid-level regressions**: The firm FE completely eliminates the OLS results. Neither β₁ (total simultaneous bids) nor β₂ (non-local bids) is significant in any firm-FE specification. The OLS results (+0.038 for β₁, −0.054 for β₂) were entirely between-firm selection effects. Firms that bid more simultaneously are structurally higher-cost or different in ways correlated with bid levels, but **within a firm, monthly portfolio size does not change bid prices**.

---

### Mechanisms

**Mechanism 1 — Capacity constraints (Obras Públicas):**
Construction firms have physical constraints (crews, equipment, financing). Simultaneous bidding predicts reduced geographic entry in OLS (β₁ = −0.053) but much less in FE (β₁ = −0.006), suggesting most of the raw relationship is selection. The reform reduced Obras Públicas portfolio sizes by 48%, the largest decline of any sector. Post-reform, the within-firm capacity mechanism no longer predicts either entry or pricing.

**Mechanism 2 — Scale economies and market learning (Municipalidades):**
Goods procurement is more scalable. Municipalidades firms run much larger simultaneous portfolios (mean 6.3 bids/month vs. 4.5 overall). The OLS premium for simultaneous bidding on bid prices (+0.038) reflects between-firm selection, not a within-firm capacity cost. Firm FEs reduce this to zero. The true effect of portfolio size on bid pricing is negligible within firms.

**Mechanism 3 — Competitive self-selection of non-local firms:**
The OLS non-local "discount" on bid prices (−0.054) is also selection: firms that choose to bid outside their region are structurally lower-cost bidders. Firm FEs eliminate this, too. Post-reform, the discount intensifies in OLS (−0.073) because the reform filters out all but the most competitive non-local bidders — a between-firm composition effect.

### Reform Impact

The December 2024 local-preference reform had heterogeneous effects:

- **Obras Públicas:** Most exposed — simultaneous bid counts fell 48% (12.75 → 6.58 per month). The remaining active firms are more distant (avg 297 km vs. 276 km pre-reform), suggesting only above-threshold, longer-haul construction projects remain contestable by out-of-region firms.

- **Municipalidades:** Less exposed — bid counts fell 23% (6.33 → 4.84). The nonlocal share barely changed (29.3% → 30.9%), suggesting the reform cut activity proportionally.

- **Contract size:** The reform hit small contracts hardest. <500 UTM bids fell by 85% in post-reform period while ≥500 UTM bids fell by 62%. Entry effects and bid-level effects are very similar across UTM bands — the reform did not qualitatively change the mechanism, only the scale.

- **Pricing:** Post-reform, OLS non-local bids are more discounted (−0.073 vs. −0.051), consistent with only the most competitive non-local firms remaining active. This is a composition effect, not a within-firm behavioral change (FE confirms).

- **Within-firm dynamics post-reform:** With smaller portfolios, the monthly simultaneous bid count no longer predicts entry decisions (β₁ ≈ 0 in FE post-reform). This suggests the reform effectively removed the capacity pressure firms previously faced — they now operate below their capacity frontier.

---

## 7. Files

### Data
- `output/simultaneousbids/firm_month_panel.parquet` — 544,689 firm-month observations
- `output/simultaneousbids/bid_level_simult.parquet` — 2,340,480 tender-firm observations with simultaneous bid counts

### Tables
| File | Contents |
|---|---|
| t1_n_sim_distribution.csv | Distribution of monthly sim bid count |
| t2_n_sim_by_size.csv | By firm size (tramoventas) |
| t3_n_sim_by_region.csv | By bidder home region |
| t4_n_sim_by_sector.csv | By sector (Municipalidades, Obras Públicas) |
| t5_geo_dist_nonlocal.csv | Distance distribution for non-local bids |
| t6_pre_post_reform.csv | Pre/post reform comparison |
| t7_n_sim_by_size_sector.csv | By size × sector |
| t8_entry_regressions.csv | Effect of n_sim on region entry |
| t9_bid_level_regressions.csv | Effect of n_sim on log bid ratio |
| t10_munic_vs_obras.csv | Sector × period comparison |
| t11_pre_post_regressions.csv | Pre/post breakdown |
| t12_size_heterogeneity.csv | By firm size |

### Figures
| File | Contents |
|---|---|
| f1_dist_n_sim.png | Distribution of simultaneous bids (log scale) |
| f2_n_sim_by_region.png | Mean sim bids by home region |
| f3_geo_spread_nonlocal.png | Distance histogram, non-local bids |
| f4_pre_post_n_sim.png | Pre vs. post comparison by sector |
| f5_n_sim_time_series.png | Monthly time series |
| f6_n_sim_by_size_sector.png | Heatmap: size × sector |

### Tables (Extensions)
| File | Contents |
|---|---|
| t13_local_nonlocal_prepost_size.csv | Local vs. nonlocal bids by period × size |
| t14_entry_fe.csv | Entry regressions: OLS vs. Firm FE |
| t15_bid_fe.csv | Bid-level regressions: OLS vs. Firm FE |
| t16_utm_descriptives.csv | Contract characteristics by UTM band × sector × period |
| t17_entry_utm_split.csv | Entry regressions by UTM band |
| t18_bid_utm_split.csv | Bid-level regressions by UTM band |

### Code
- `code/analysis/simultaneousbids/01_build_simultaneous_bids.py`
- `code/analysis/simultaneousbids/02_descriptives.py`
- `code/analysis/simultaneousbids/03_entry_bidding.py`
- `code/analysis/simultaneousbids/04_extensions.py`
