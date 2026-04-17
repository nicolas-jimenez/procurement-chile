# Empirical Strategy: Testing the Spatial Auction Model Predictions

## 0. Data Sources and Construction

### 0.1. ChileCompra Data

The primary dataset comes from ChileCompra's public transaction records. For each procurement, observe:

- **Project value** (in UTM): determines which policy tier applies (below 100, 100–500, above 500).
- **Purchasing agency**: identifies region of demand and agency type (regionally deconcentrated or not).
- **Sector/product classification**: allows restricting to construction or other target sectors.
- **Bidder identities and bids**: number of bidders $n_{ir}$, winning bid $p_i$, and ideally all submitted bids.
- **Winner identity and RUT**: links to firm registry data; determines whether winner is local.
- **Date**: allows constructing pre/post periods around the December 12, 2024 implementation date.

From this, construct:

- **Local firm indicator**: firm $i$ is local in region $r$ if its registered domicilio principal (from SII records or ChileCompra registry) is in region $r$.
- **Pre-reform non-local share**: for each region $\times$ sector cell, compute $s_{rk}^{pre} = \frac{\text{value awarded to non-local firms}}{\text{total value awarded}}$ using 2–3 years of pre-reform data.
- **Market thickness**: $Q_{rk}$ = number of projects per period in region $r$, sector $k$.
- **Potential local suppliers**: $N_{rk}^{pot}$ = number of distinct firms registered in region $r$ that have ever bid on sector $k$ projects anywhere in Chile, or that are registered in ChileCompra in the relevant rubros.

### 0.2. SII / Central Bank Employer-Employee Data

Matched employer-employee records from the Central Bank or SII (Servicio de Impuestos Internos) provide:

- Firm-level employment by region and sector (quarterly or annual).
- Worker-level wages, allowing construction of local sector employment and earnings measures.
- Firm entry/exit: first and last appearance in tax records.

### 0.3. MOP Data (for Construction)

If focusing on construction, the Ministerio de Obras Públicas provides:

- Project execution timelines (planned vs. actual completion).
- Cost overruns (final cost vs. awarded contract value).
- Quality assessments where available.

### 0.4. Sample Construction

Restrict to a balanced panel of region $\times$ sector $\times$ period cells. Define periods as quarters or semesters. The pre-reform period runs from roughly 2021–2024Q3; the post-reform period begins 2024Q4 or 2025Q1 (allowing for implementation lag). Drop the quarter immediately around December 2024 if there is ambiguity about which projects fall under the new rules.

---

## 1. Prediction 1: Heterogeneous Competition Effects

**Prediction:** The reform reduces the number of bidders more in regions where (a) non-local firms held a larger pre-reform market share, (b) the pool of potential local firms is small, and (c) demand is thin.

### 1.1. Main Specification: Treatment Intensity Event Study

$$n_{i,r,t} = \alpha_r + \gamma_t + \sum_{\tau \neq -1} \beta_\tau \cdot s_{rk}^{pre} \times \mathbf{1}[t = \tau] + X'_{i,r,t}\delta + \varepsilon_{i,r,t}$$

where:

- $n_{i,r,t}$: number of bidders on project $i$ in region $r$ at time $t$.
- $\alpha_r$: region fixed effects.
- $\gamma_t$: time (quarter) fixed effects.
- $s_{rk}^{pre}$: pre-reform non-local value share in region $r$, sector $k$ (continuous treatment intensity).
- $X_{i,r,t}$: project-level controls — log project value, agency type, product category.
- The omitted period $\tau = -1$ is the last pre-reform quarter.

**Interpretation of $\beta_\tau$:** The differential change in number of bidders for a region with 100% pre-reform non-local share relative to one with 0%, at time $\tau$ relative to the period just before the reform. We expect $\beta_\tau \approx 0$ for $\tau < 0$ (parallel pre-trends) and $\beta_\tau < 0$ for $\tau > 0$ if the policy reduces competition in high-exposure regions.

### 1.2. Heterogeneity by Market Characteristics

To test parts (b) and (c) of the prediction, augment with triple interactions:

$$n_{i,r,t} = \alpha_r + \gamma_t + \beta_1 \cdot s_{rk}^{pre} \times \text{Post}_t + \beta_2 \cdot s_{rk}^{pre} \times \text{Post}_t \times \text{ThinMarket}_r + X'_{i,r,t}\delta + \varepsilon_{i,r,t}$$

where $\text{ThinMarket}_r$ is an indicator (or continuous measure) for regions with below-median $Q_{rk}$ or below-median $N_{rk}^{pot}$.

**Prediction:** $\beta_2 < 0$. The competition decline is steeper in thin markets.

**Alternative continuous version:**

$$n_{i,r,t} = \alpha_r + \gamma_t + \beta_1 \cdot s_{rk}^{pre} \times \text{Post}_t + \beta_2 \cdot s_{rk}^{pre} \times \text{Post}_t \times \log Q_{rk} + \beta_3 \cdot s_{rk}^{pre} \times \text{Post}_t \times \log N_{rk}^{pot} + \varepsilon_{i,r,t}$$

**Prediction:** $\beta_2 > 0$ and $\beta_3 > 0$ — the competition decline is attenuated in thicker markets and in markets with more potential local entrants.

### 1.3. Threshold DiD (Sharpest Identification)

Restrict to projects near the 500 UTM cutoff. Below 500 UTM, the local preference applies (for deconcentrated agencies); above, it does not.

$$n_{i,r,t} = \alpha + \beta \cdot \text{Below500}_i \times \text{Post}_t + f(\text{Value}_i) + \alpha_r + \gamma_t + \varepsilon_{i,r,t}$$

where $f(\text{Value}_i)$ is a low-order polynomial or linear spline in project value, estimated separately on each side of the cutoff. Bandwidth: restrict to projects in, say, [300, 700] UTM.

**Prediction:** $\beta < 0$. Projects just below 500 UTM see fewer bidders post-reform relative to projects just above.

Repeat at the 100 UTM threshold for the Compra Ágil strong-form exclusion. The effect should be larger at 100 UTM since exclusion is complete.

---

## 2. Prediction 2: Cross-Market Spillovers

**Prediction:** In regions that are net exporters of firms (e.g., Santiago), the reform increases competition in above-threshold procurements, as capacity is redirected from peripheral markets.

### 2.1. Measuring Firm Export Intensity

For each region $r$, construct a pre-reform "firm export" measure:

$$\text{Export}_{r}^{pre} = \frac{\text{value of contracts won by region-}r\text{ firms in other regions}}{\text{total value won by region-}r\text{ firms}}$$

Santiago and other major urban centers will have high $\text{Export}_r^{pre}$.

### 2.2. Event Study on Above-Threshold Projects

Restrict to projects **above 500 UTM** (not directly subject to local preference) and estimate:

$$n_{i,r,t} = \alpha_r + \gamma_t + \sum_{\tau \neq -1} \beta_\tau \cdot \text{Export}_{r}^{pre} \times \mathbf{1}[t = \tau] + X'_{i,r,t}\delta + \varepsilon_{i,r,t}$$

**Prediction:** $\beta_\tau > 0$ for $\tau > 0$. Above-threshold projects in firm-exporting regions see more bidders post-reform, as firms that previously competed in peripheral below-threshold markets redirect capacity home.

**Why this is useful:** This is a direct test of the capacity constraint mechanism. If firms have ample capacity ($K$ is large), redirected firms simply reduce activity rather than re-entering their home market, and $\beta_\tau \approx 0$. A positive $\beta$ is evidence that capacity constraints bind and generate the cross-market linkages that motivate the multi-market model.

### 2.3. Firm-Level Evidence

Track individual firms that were active in peripheral markets pre-reform:

$$\text{HomeBids}_{i,t} = \alpha_i + \gamma_t + \beta \cdot \text{PeripheralExposure}_i \times \text{Post}_t + \varepsilon_{i,t}$$

where $\text{HomeBids}_{i,t}$ is the number of bids firm $i$ submits in its home region in period $t$, and $\text{PeripheralExposure}_i$ is the pre-reform share of firm $i$'s bids that were in below-threshold projects in non-home regions.

**Prediction:** $\beta > 0$. Firms with higher peripheral exposure increase home-market bidding post-reform.

---

## 3. Prediction 3: Local Entry Response

**Prediction:** In regions with large pools of potential local firms, new local firms enter the procurement market post-reform.

### 3.1. Measuring Entry

Define a firm as a "new local entrant" in region $r$ at time $t$ if it submits its first-ever bid on a below-threshold project in region $r$ at time $t$ and is domiciled in region $r$.

Outcome variable: $\text{NewEntrants}_{r,t}$ = count of new local entrants per region-period.

### 3.2. Specification

$$\text{NewEntrants}_{r,t} = \alpha_r + \gamma_t + \beta_1 \cdot \text{Post}_t \times \log N_{rk}^{pot} + \beta_2 \cdot \text{Post}_t \times s_{rk}^{pre} + X'_{r,t}\delta + \varepsilon_{r,t}$$

**Predictions:**

- $\beta_1 > 0$: More entry in regions with more potential local firms. These regions have the supply-side slack to respond to increased profit opportunities.
- $\beta_2 > 0$: More entry in regions where excluded non-local firms left larger rents. High pre-reform non-local share means more room for profitable entry.

### 3.3. Dynamics of Entry

Replace $\text{Post}_t$ with period dummies to trace out the speed of entry:

$$\text{NewEntrants}_{r,t} = \alpha_r + \gamma_t + \sum_{\tau > 0} \beta_\tau^{pot} \cdot \mathbf{1}[t=\tau] \times \log N_{rk}^{pot} + \sum_{\tau > 0} \beta_\tau^{s} \cdot \mathbf{1}[t=\tau] \times s_{rk}^{pre} + \varepsilon_{r,t}$$

This reveals whether entry is immediate or builds gradually, which speaks to the speed of market adjustment. If $\beta_\tau$ increases over time, entry takes time to materialize (firms need to learn about opportunities, prepare bid capabilities, etc.). If $\beta_\tau$ is large immediately and then stabilizes or declines, entry responds quickly to the profit opportunity and the market equilibrates fast.

---

## 4. Prediction 4: Heterogeneous Price Effects

**Prediction:** The reform increases procurement costs more in thin markets and may decrease costs in thick markets.

### 4.1. Main Specification

$$\log p_{i,r,t} = \alpha_r + \gamma_t + \beta_1 \cdot s_{rk}^{pre} \times \text{Post}_t + \beta_2 \cdot s_{rk}^{pre} \times \text{Post}_t \times \text{Thick}_r + X'_{i,r,t}\delta + \varepsilon_{i,r,t}$$

where $p_{i,r,t}$ is the winning bid (or winning bid / reserve price ratio) and $\text{Thick}_r$ is a measure of market thickness.

**Predictions:**

- $\beta_1 > 0$: On average in high-exposure regions, prices increase (reduced competition raises markups).
- $\beta_2 < 0$: The price increase is attenuated or reversed in thick markets where local entry compensates.

### 4.2. Threshold DiD for Prices

Same bandwidth around 500 UTM as in Section 1.3:

$$\log p_{i,r,t} = \alpha + \beta \cdot \text{Below500}_i \times \text{Post}_t + f(\text{Value}_i) + \alpha_r + \gamma_t + \varepsilon_{i,r,t}$$

**Prediction:** $\beta > 0$ overall, but interact with $\text{Thick}_r$ to show heterogeneity.

### 4.3. Decomposing the Price Effect

The model says the price effect has two components: a composition effect (different firms win) and a markup effect (same firms bid differently). To decompose:

**Step 1:** Estimate whether the identity of winners changes:

$$\text{LocalWin}_{i,r,t} = \alpha_r + \gamma_t + \beta \cdot s_{rk}^{pre} \times \text{Post}_t + \varepsilon_{i,r,t}$$

If $\beta > 0$, more local firms are winning, confirming the composition shift.

**Step 2:** Restrict to projects won by local firms both pre- and post-reform. Estimate the price equation on this subsample. If prices still rise, local firms are exercising increased markup (market power effect). If prices are flat, the aggregate price increase is entirely driven by composition (costlier local firms replacing cheaper non-local ones).

**Step 3 (if full bid data available):** Estimate bid functions directly. Compare the bid distribution of local firms in high-exposure vs. low-exposure regions, pre vs. post reform. An upward shift in local firm bids, conditional on project characteristics, is direct evidence of increased markups.

---

## 5. Prediction 5: Local Employment Effects

**Prediction:** Local employment in the procurement sector increases more in regions where local firms' win share rises most.

### 5.1. Main Specification

$$\log L_{r,k,t} = \alpha_r + \gamma_t + \beta \cdot s_{rk}^{pre} \times \text{Post}_t + X'_{r,t}\delta + \varepsilon_{r,k,t}$$

where $L_{r,k,t}$ is total employment in region $r$, sector $k$, period $t$ from the employer-employee data.

**Prediction:** $\beta > 0$. Regions with higher pre-reform non-local share see larger gains in local sector employment, as local firms win more contracts and hire more workers.

### 5.2. Worker-Level Analysis

At the worker level, examine hiring:

$$\text{Hired}_{j,r,t} = \alpha_r + \gamma_t + \beta \cdot s_{rk}^{pre} \times \text{Post}_t + X'_{j}\delta + \varepsilon_{j,r,t}$$

where $\text{Hired}_{j,r,t}$ indicates whether worker $j$ in region $r$ is newly hired in sector $k$ at time $t$.

### 5.3. Testing Whether Employment Gains Are Real or Reshuffled

A key concern: local employment gains in high-exposure regions may come at the expense of employment losses at non-local firms that lose contracts. To assess net effects:

**Approach A:** Estimate the employment effect separately for:
- Regions that are net "importers" of procurement firms (high $s_{rk}^{pre}$): expect employment gains.
- Regions that are net "exporters" (high $\text{Export}_r^{pre}$): may see employment losses if firms lose peripheral contracts without fully redirecting capacity.

Sum across all regions to get the net national employment effect.

**Approach B:** Firm-level employment analysis. For firms that were active in non-home markets pre-reform:

$$\log L_{i,t} = \alpha_i + \gamma_t + \beta \cdot \text{PeripheralRevShare}_i \times \text{Post}_t + \varepsilon_{i,t}$$

**Prediction:** $\beta < 0$ for non-local firms that lose access to peripheral markets (unless they fully redirect capacity). $\beta > 0$ for local firms in high-exposure regions.

---

## 6. Prediction 6: Bunching Around Thresholds

**Prediction:** Government agencies strategically size projects relative to the 100 UTM and 500 UTM thresholds.

### 6.1. Density Tests

For each threshold $\bar{v} \in \{100, 500\}$ UTM:

**Step 1:** Plot the density of project values in a window around $\bar{v}$, separately for pre-reform and post-reform periods.

**Step 2:** Apply the Cattaneo-Jansson-Ma (2020) manipulation test to the post-reform density at $\bar{v}$. The null hypothesis is continuity of the density at the threshold.

**Step 3:** Compare the CJM test statistic pre vs. post reform. Under the null of no strategic manipulation, both periods should show continuity. If the post-reform density shows a discontinuity but the pre-reform density does not, this is evidence of strategic project sizing.

### 6.2. Direction of Bunching

The direction of bunching is informative about agency preferences:

- **Bunching below $\bar{v}$:** Agencies prefer the local preference regime. They downsize projects to qualify for the preference, revealing that they value contracting with local firms (either due to genuine preference or due to political incentives).
- **Bunching above $\bar{v}$:** Agencies prefer to avoid the local preference. They upsize projects to escape the restriction, revealing that they prefer to maintain access to non-local (presumably cheaper or higher-quality) suppliers.

### 6.3. Heterogeneity in Bunching

Estimate bunching separately by:

- **Agency type:** Regionally deconcentrated agencies (directly subject to Art. 61) vs. central agencies. Only deconcentrated agencies should bunch.
- **Region:** Regions with many local firms (low cost of local preference) vs. few local firms (high cost). Agencies in thin-market regions have stronger incentives to bunch above the threshold.
- **Sector:** Sectors where local/non-local quality gaps are large should show more above-threshold bunching.

### 6.4. Implications for Other Designs

If bunching is substantial, the composition of projects near the thresholds changes post-reform, which complicates the threshold DiD in Sections 1.3 and 4.2. Two responses:

**Donut RDD:** Exclude a narrow window around the threshold (e.g., [480, 520] UTM) and compare projects in [300, 480] vs. [520, 700]. This sacrifices some statistical precision but avoids the manipulation region.

**Bunching as outcome:** Rather than treating bunching as a threat, estimate it as an outcome of interest. The magnitude of bunching quantifies agencies' willingness to pay to avoid (or obtain) local preference treatment, which is an informative moment for the structural model.

---

## 7. Additional Tests and Robustness

### 7.1. Agency Type Placebo

Article 61 applies to "gobiernos regionales y otros organismos públicos territorialmente desconcentrados." Central government agencies that are not regionally deconcentrated should be unaffected.

**Test:** Estimate the main competition and price specifications using central (non-deconcentrated) agencies as a placebo group. All $\beta$ coefficients should be zero (or at least much smaller) for this group.

$$n_{i,r,t} = \alpha_r + \gamma_t + \beta_1 \cdot s_{rk}^{pre} \times \text{Post}_t \times \text{Deconc}_a + \beta_2 \cdot s_{rk}^{pre} \times \text{Post}_t \times (1 - \text{Deconc}_a) + \varepsilon_{i,r,t}$$

**Prediction:** $|\beta_1| \gg |\beta_2| \approx 0$.

### 7.2. Guarantee Threshold as a Separate Natural Experiment

The reform also raises the guarantee threshold: seriedad guarantees are now required only above 5,000 UTM (previously no floor), and fiel cumplimiento guarantees only above 1,000 UTM. This is a distinct reform that reduces barriers to entry for small firms.

**Specification:** Around the 1,000 UTM threshold:

$$n_{i,r,t} = \alpha + \beta \cdot \text{Below1000}_i \times \text{Post}_t + f(\text{Value}_i) + \alpha_r + \gamma_t + \varepsilon_{i,r,t}$$

**Prediction:** $\beta > 0$. Below-threshold projects see more bidders post-reform because the guarantee requirement is removed, reducing entry costs especially for small firms.

This is a useful complementary analysis because (a) it identifies a different mechanism (entry cost reduction vs. local preference), (b) it provides an additional source of variation, and (c) the 1,000 UTM threshold is far enough from the 500 UTM local preference threshold that the two treatments do not confound each other in a narrow bandwidth design.

### 7.3. Pre-Trend Validation

For all event study specifications, the critical identification assumption is parallel pre-trends. Report:

- Plots of $\hat{\beta}_\tau$ for $\tau < 0$ with confidence intervals.
- Joint F-test of $\beta_\tau = 0$ for all $\tau < 0$.
- If pre-trends are present, apply Rambachan and Roth (2023) sensitivity analysis to bound the treatment effect under deviations from parallel trends.

### 7.4. Permutation / Randomization Inference

For the treatment intensity designs, the effective number of clusters is $R \times K$ (regions $\times$ sectors), which may be small. Standard cluster-robust inference may over-reject.

**Remedy:** Permutation inference. Randomly reassign $s_{rk}^{pre}$ across region-sector cells (keeping the time series structure intact) and re-estimate $\beta$. Repeat 1,000+ times. The p-value is the share of permuted $\hat{\beta}$'s that exceed the actual $\hat{\beta}$ in absolute value.

Alternative: wild cluster bootstrap at the region level (Cameron, Gelbach, and Miller 2008).

---

## 8. Summary: Mapping Predictions to Tests

| Prediction | Main Outcome | Key RHS Variation | Primary Design | Section |
|---|---|---|---|---|
| 1. Heterogeneous competition | # bidders | $s_{rk}^{pre} \times$ Post | Treatment intensity ES | 1 |
| 2. Cross-market spillovers | # bidders (above-threshold) | $\text{Export}_r^{pre} \times$ Post | Event study on untreated projects | 2 |
| 3. Local entry | New entrant count | $N_{rk}^{pot} \times$ Post | Region-level panel | 3 |
| 4. Heterogeneous prices | Winning bid | $s_{rk}^{pre} \times$ Post $\times$ Thick | Treatment intensity + threshold DiD | 4 |
| 5. Local employment | Sector employment | $s_{rk}^{pre} \times$ Post | Employer-employee panel | 5 |
| 6. Bunching | Project value density | Post indicator | Density tests at thresholds | 6 |

Each prediction has a clean reduced-form test. The results collectively discipline the structural model: if, for instance, you find no cross-market spillovers (Prediction 2), you can simplify the model by relaxing the capacity constraint. If you find large entry responses (Prediction 3), the pool of potential entrants $N_r^{pot}$ is an important parameter to estimate carefully. The reduced-form results tell you which mechanisms matter empirically and therefore which features of the model are worth estimating structurally.
