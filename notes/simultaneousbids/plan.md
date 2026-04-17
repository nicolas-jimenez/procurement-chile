# Simultaneous Bidding in Chilean Public Procurement

**Author:** Nicolas Jimenez
**Date:** March 2026
**Status:** Analysis in progress

---

## 1. Motivation and Research Questions

When a firm bids on a procurement auction, it may already be committed to — or competing in — other auctions at the same time. This "simultaneous bidding" behavior matters for two reasons:

1. **Capacity constraints**: Firms have finite managerial attention, capital, and labour. Bidding on many projects at once may increase the shadow cost of winning any one of them, affecting markups.
2. **Learning and market reach**: Firms that regularly bid outside their home region may face systematically different cost structures or competitive pressures when doing so alongside local bids.

The December 12, 2024 local-preference reform in Chile provides a natural experiment: it reshapes where firms can profitably bid, and should therefore alter simultaneous bidding portfolios — especially for firms that previously exported bids across regions.

**Research questions:**

1. How many projects do firms bid on simultaneously? (Definition: tenders published in the same calendar month for a given firm.)
2. For firms that bid outside their registered region, what is the geographic distribution of their simultaneous bids?
3. How does simultaneous bidding vary by firm size (tramoventas tramo) and buyer region?
4. Does bidding on other projects concurrently affect:
   (a) Whether a firm enters a new auction?
   (b) How aggressively it bids in that auction (bid/estimate ratio)?
5. Do these effects differ across sectors — especially **Municipalidades** (goods-heavy, cost complementarities expected) vs. **Obras Públicas** (construction, backlog and capacity constraints expected)?
6. How did each of these patterns change after the December 2024 reform?

---

## 2. Data

### Source
- **Primary**: `data/clean/licitaciones_sii_merged.parquet` — 13.3 million bid-rows from formal ChileCompra licitaciones (2022–2025), merged with SII firm registry data.
- **Unit of observation**: one row = one firm's bid on one tender (tender × firm level, but with multiple items per tender collapsed to the firm–tender level where needed).

### Key Variables

| Variable | Description |
|---|---|
| `tender_id` | Unique auction identifier |
| `rut_bidder` | Firm tax ID (firm identifier) |
| `fecha_pub` | Auction publication date |
| `region_buyer` | Region of the purchasing agency |
| `region` | Bidder's registered home region (from SII) |
| `same_region` | 1 if bidder's region = buyer's region |
| `monto_oferta` | Nominal bid value (CLP) |
| `monto_estimado` | Estimated/reserve value (CLP) |
| `is_selected` | 1 if this bid won |
| `sector` | Buyer sector (Municipalidades, Obras Públicas, Salud, etc.) |
| `tramoventas` | SII sales-size tramo (1 = micro, 13 = large) |
| `ntrabajadores` | Number of workers (SII) |
| `n_oferentes` | Number of bidders on tender |

### Firm-size Classification
We classify firms using `tramoventas`:
- **Micro/small**: tramos 1–7 (≤ UF 25,000 annual sales ≈ ~US$1M)
- **Medium**: tramos 8–10
- **Large**: tramos 11–13

### Simultaneity Definition
**Same-month definition**: firm $i$ is simultaneously bidding on $K$ auctions in month $m$ if it submitted bids on $K$ distinct tenders with `fecha_pub` in month $m$.

This is a mild approximation (we observe publication dates, not submission or closing dates). Since typical licitaciones remain open 10–30 days after publication, month-level simultaneity captures the key overlap.

### Reform Indicator
The December 12, 2024 local-preference reform creates a clean pre/post partition:
- **Pre-reform**: `fecha_pub` ≤ November 2024
- **Post-reform**: `fecha_pub` ≥ January 2025 (December 2024 is transition)

---

## 3. Empirical Strategy

### 3.1 Descriptive Analysis

**Step 1: Firm–month panel of simultaneous bids**

For each (firm, year-month) pair, compute:
- `n_sim`: total distinct tenders bid on in that month
- `n_sim_local`: tenders where buyer region = firm's home region
- `n_sim_nonlocal`: tenders outside home region
- `share_nonlocal`: `n_sim_nonlocal / n_sim`
- Geographic spread metrics (number of distinct buyer regions, distance distribution)

**Step 2: Distributional analysis**
- Distribution of `n_sim` overall and by sector
- By firm size (tramoventas tramo)
- By firm home region
- Pre vs. post reform

**Step 3: Geographic distribution of out-of-region bids, conditional on simultaneous bidding**
- For non-local bids, the distribution of distance to buyer region centroid
- Heat map: when a firm bids non-locally and simultaneously, which regions does it target?

### 3.2 Effect on Entry

**Outcome**: Whether firm $i$ enters auction $j$ (i.e., submits a bid).

Since we observe only bids (not the full set of firms that *could* have bid), we proxy entry by looking at the extensive margin within a firm's activity history:

**Specification — new market entry:**
$$\text{NewRegion}_{ijt} = \alpha_i + \gamma_{rt} + \beta_1 n\_sim_{i,t-1} + \beta_2 n\_sim\_nonlocal_{i,t-1} + X_{it}'\delta + \varepsilon_{ijt}$$

where $\text{NewRegion}_{ijt} = 1$ if firm $i$ bids in buyer region $r$ for the first time at time $t$.

**Alternative specification — number of auctions entered:**
$$n\_entered_{i,t} = \alpha_i + \gamma_t + \beta_1 n\_sim_{i,t-1} + X_{it}'\delta + \varepsilon_{it}$$

using the firm-month panel, lagged simultaneous bid count as the key regressor.

**Heterogeneity**: Interact with `small` (firm size), `same_region` (local vs. non-local), and sector dummies (Municipalidades vs. Obras Públicas).

**Pre/post reform**: Include `Post × n_sim` interaction to see whether the relationship changed post-reform. Under the hypothesis that Municipalidades has cost synergies, we expect: more simultaneous Municipalidades bids → more entry (positive $\beta$). For Obras Públicas (capacity constraints), we expect the opposite (negative $\beta$, or at least smaller positive).

### 3.3 Effect on Bid Levels

**Outcome**: log bid ratio = $\log(\text{monto\_oferta} / \text{monto\_estimado})$

**Specification:**
$$\log(b_{ijt}/v_{jt}) = \alpha_i + \gamma_{rt} + \beta_1 n\_sim_{i,t} + \beta_2 n\_sim\_nonlocal_{i,t} + \beta_3 \log n\_oferentes_{jt} + X_{jt}'\delta + \varepsilon_{ijt}$$

**Interpretation:**
- If $\beta_1 > 0$: Firms bid higher (less aggressively) when simultaneously active in many auctions → capacity constraints raise marginal costs
- If $\beta_1 < 0$: More simultaneous bidding → lower bids (scale economies / better information)

**Sector hypothesis:**
- **Municipalidades** (goods procurement): simultaneous bidding may allow cost spreading → expect $\beta_1 \leq 0$ (lower bids when simultaneously active)
- **Obras Públicas** (construction): binding labour/equipment capacity → expect $\beta_1 > 0$ (higher bids under capacity pressure)

**Pre/post reform interaction:**
$$\beta_1^{pre}, \beta_1^{post}$$ — does reform alter the simultaneous-bid/markup relationship?

### 3.4 Identification Concerns

The main threat is that unobserved firm-period quality shocks affect both the number of bids and bid levels simultaneously. We address this by:
1. **Firm fixed effects** absorb permanent heterogeneity
2. **Month × region FE** absorb market-level demand shocks
3. **Lagged instruments**: use $n\_sim_{t-1}$ as predictor for entry at time $t$ (past simultaneous bidding as a proxy for firm workload)
4. **Sector-by-quarter FE** for bid-level regressions

---

## 4. Expected Results and Sector Hypotheses

### Municipalidades (goods, services)
- Goods procurement involves lower capacity constraints (catalogue items, standard services)
- Firms can win multiple contracts simultaneously with low marginal cost
- **Expected**: High simultaneous bid counts, especially for medium/large firms
- **Expected**: Positive effect of simultaneous bidding on entry (scale economies)
- **Expected**: Negative or zero effect on bid ratios (cost synergies)
- **Post-reform**: Non-local firms should reduce Municipalidades bids outside their region → simultaneous bid portfolios become more concentrated

### Obras Públicas (construction)
- Construction requires physical presence, equipment, and crews on-site
- Winning multiple construction contracts simultaneously creates genuine capacity strain
- **Expected**: Lower simultaneous bid counts; firms spread bids more carefully
- **Expected**: Negative or insignificant effect on entry (capacity limits)
- **Expected**: Positive effect on bid ratios (higher markup under capacity pressure)
- **Post-reform**: Local-preference rules should reduce out-of-region construction bids sharply

---

## 5. Output Files

### Notes
- `notes/simultaneousbids/plan.md` — this document
- `notes/simultaneousbids/results.md` — full results write-up

### Code
- `code/analysis/simultaneousbids/01_build_simultaneous_bids.py` — data construction
- `code/analysis/simultaneousbids/02_descriptives.py` — descriptive tables and figures
- `code/analysis/simultaneousbids/03_entry_bidding.py` — regressions

### Output
- `output/simultaneousbids/tables/` — all summary tables (CSV)
- `output/simultaneousbids/figures/` — all figures (PNG)

---

## 6. Timeline

| Step | Script | Status |
|---|---|---|
| Data construction | 01_build_simultaneous_bids.py | ☐ |
| Descriptives | 02_descriptives.py | ☐ |
| Regressions | 03_entry_bidding.py | ☐ |
| Results write-up | results.md | ☐ |
