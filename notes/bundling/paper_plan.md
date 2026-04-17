# Paper Plan: Bundling and SME Outcomes in Public Procurement

Date: 2026-04-14

## 1. Title (working)

**"Product Variety, Category Breadth, and SME Outcomes: How Procurement Bundling Interacts with Local Preference Policies"**

Alternative: "Does Bundling Undermine SME Preference? Evidence from Chile's Compra Ágil Reform"

## 2. One-paragraph summary

We study how the composition of procurement bundles affects SME and local-firm outcomes, exploiting Chile's December 2024 Compra Ágil reform which shifted 30–100 UTM municipal tenders from competitive licitaciones to a simplified process with local/SME preference. The reform induced buyers to bundle more products per tender. We decompose bundle composition into two dimensions — product variety (N distinct 8-digit UNSPSC codes) and category breadth (N distinct 2-digit segments) — and trace their effects through three margins: who bids (extensive), the composition of bidders (intensive), and who wins. Product variety attracts large firms and repels SMEs from the bidder pool, but this effect is entirely a selection channel — it disappears when conditioning on bidder composition. Category breadth shrinks the total bidder pool but tilts it toward local SMEs, who are more likely to win even conditional on who bids. The reform's induced bundling is therefore complementary to, not in tension with, its SME preference goals.

## 3. Contribution

Three contributions to the procurement bundling literature:

1. **Bundling as multi-dimensional treatment.** Most empirical work treats bundling as a scalar (bundle/unbundle, or contract size). Following the spirit of Ridderstedt & Nilsson (2023), who decompose Swedish highway contracts into scale, spread, and task heterogeneity, we decompose goods-procurement bundles into product variety vs. category breadth and show they have opposite effects on SME outcomes. This is the first application of multi-dimensional bundling measurement in a goods-procurement (as opposed to infrastructure) setting.

2. **Selection vs. competitive advantage — a clean decomposition.** The literature emphasizes entry deterrence from bundling (Estache & Iimi 2011; Suzuki 2021) but does not cleanly separate whether bundling hurts small firms through who *shows up to bid* vs. who *wins conditional on bidding*. Our SME-share-of-bidders control provides a simple, transparent decomposition. We show the product variety channel is entirely selection; the category breadth channel operates through genuine competitive advantage for local generalist suppliers.

3. **Bundling × preferential policies.** To our knowledge, no prior work studies how procurement bundling interacts with SME/local preference policies. We show the interaction is complementary — cross-category bundling amplifies local SME advantage — which is policy-relevant for the many countries that combine simplified procurement with domestic preference (EU below-threshold procurement, US simplified acquisition, developing-country SME set-asides).

4. **Firm-level mechanism: the generalist advantage.** Using bid-level data with within-tender fixed effects, we show that generalist firms (those with broad pre-period scope) face a significant penalty when competing against specialists on the same tender. But this penalty shrinks by ~60% post-reform and nearly vanishes on diverse tenders — identifying the micro-foundation for the "general store" channel. This connects the tender-level finding (category breadth helps local SMEs) to an observable firm characteristic (scope).

## 4. Paper structure

### Section 1: Introduction (3–4 pages)
- Motivation: procurement bundling is pervasive but understudied in goods markets; SME/local preference is widespread; their interaction is unknown
- Preview of setting: Chile's Compra Ágil reform as a natural experiment that simultaneously activated local preference and induced bundling
- Preview of main results (3 bullet points)
- Contribution to literature (as above)
- Roadmap

### Section 2: Setting and Policy (3–4 pages)
- Chile's procurement system (ChileCompra/Mercado Público)
- The Compra Ágil mechanism and its local/SME preference feature
- Ley 21.634 (December 2024): threshold increase from 30 to 100 UTM
- What changed: procurement mode, bidder eligibility, buyer behavior
- The UNSPSC product classification system (8-digit codes → 2-digit segments)
- **Key fact: the reform induced bundling** — DiD evidence on n_product8 and n_segment2 (from the product_mix_note results)

### Section 3: Data (2–3 pages)
- ChileCompra universe: tenders, bids, line items (Jan 2022–Mar 2025)
- SII firm data: size classification, location, sector
- Sample construction: Municipalidades, Compra Ágil tenders, 1.2M tenders
- Product-mix construction: line-level → tender-level aggregation
  - N product codes (8-digit UNSPSC): product variety
  - N segments (2-digit UNSPSC): category breadth
  - Top-20 segment dummies (covering 79% of line items)
- Outcome variables: Pr(SME wins), Pr(local SME wins), Pr(large wins), share of SME/local/large bidders, N bidders by type
- Summary statistics table

### Section 4: The Reform Induced Bundling (2–3 pages)
- DiD on bundling outcomes: n_lines, n_product8, n_family4, n_segment2
- Control group: 0–30 UTM (already Compra Ágil) — pure mechanism-switch control
- Alternative: 100–200 UTM (licitaciones throughout) — stricter but different-mode control
- Event studies on bundling metrics (if we produce them)
- Key takeaway: treated tenders became more diverse in product composition after the reform, consistent with buyers consolidating previously separate purchases

### Section 5: Empirical Strategy for Cross-Sectional Analysis (2–3 pages)
- Specification: $y_{it} = \alpha_i + \gamma_t + \beta_1 \text{N\_product8}_{it} + \beta_2 \text{N\_segment2}_{it} + X'_{it}\delta + \varepsilon_{it}$
- Fixed effects: buyer (or region) + year-month
- Controls: log(estimated cost), N bidders, top-20 segment presence dummies
- SME share of bidders as an additional control to decompose selection vs. competitive advantage
- Clustering: buyer level
- Separate pre/post estimates (not interacted — we want to show stability, not a reform effect per se)
- Identification discussion:
  - Within-buyer variation in bundle composition
  - Segment dummies absorb which-categories-are-procured confound
  - SME share control absorbs who-shows-up confound
  - Remaining concern: within-buyer, within-segment-mix variation in product variety/breadth could still reflect tender-specific unobservables. Discuss what these would need to look like to overturn results.

### Section 6: Results (6–8 pages)

#### 6.1 Who wins? Baseline (Tables 1–3)
- Table 1: Pr(SME wins) — 4 columns (pre/post × with/without segment dummies)
- Table 2: Pr(local SME wins) — same structure
- Table 3: Pr(large firm wins) — same structure
- Key finding: product variety reduces SME win prob; category breadth increases it (especially for local SMEs after segment controls)

#### 6.2 Selection vs. competitive advantage (Tables 4–6)
- Same outcomes, now with SME share of bidders added
- Table 4: Pr(SME wins) with SME share control → product variety effect flips/disappears; R² jumps to ~0.49
- Table 5: Pr(local SME wins) with SME share control → n_segment2 effect survives at +0.05***
- Table 6: Pr(large wins) with SME share control → mirror image
- Discussion: the product variety channel is entirely selection; the category breadth channel is genuine competitive advantage

#### 6.3 The selection channel: bidder composition (Tables 7–9)
- Outcome = share of SME / local SME / large-firm bidders
- Table 7: Share SME bidders → product variety reduces it; category breadth increases it
- Table 8: Share local SME bidders → strong positive effect of n_segment2 (+0.04***)
- Table 9: Share large-firm bidders → mirror image
- These results directly confirm the selection mechanism

#### 6.4 The extensive margin: number of bidders (Tables 10–13)
- Outcome = N total / SME / large / local bidders
- Key finding: with segment dummies, category breadth *reduces* total bidders by ~1 per segment, but the reduction is proportionally larger for large firms and non-local firms
- Interpretation: diverse tenders are harder to bid on, which deters distant specialists more than local generalists → "general store" story

#### 6.5 Firm heterogeneity: generalists vs. specialists (Tables 14–15)
- Move to bid level (6.6M bids): unit = bid on a tender
- Firm scope = N distinct 2-digit UNSPSC segments the firm bid on in the pre-period (Jan 2022 – Nov 2024)
- Generalist = firm scope ≥ 10 segments (top quartile of 67K firms)
- Within-tender FE → identifies from cross-bidder variation on the same tender
- Table 14: Generalist premium (binary + continuous), pre vs. post, by tender diversity (1 seg / 2 seg / 3+ seg)
  - Key finding: generalist penalty shrinks by ~60% overall post-reform; on 3+ segment tenders, penalty nearly vanishes (−13.1pp → −0.9pp with continuous scope)
- Table 15 (optional): Pooled DiD — post × generalist interaction with tender FE
- Interpretation: reform-induced bundling made cross-category capability valuable; connects tender-level (n_segment2 helps local SMEs) to firm-level (generalists gain ground on diverse tenders)
- This is the micro-foundation for the "general store" story

### Section 7: Robustness (2–3 pages)
- Region FE instead of buyer FE (already done — results stable)
- Alternative bundling measures: diversity (1−HHI of product shares) — show it's absorbed by segment dummies
- Different segment-dummy cutoffs (top 10, top 30 instead of top 20)
- Including the 0–30 UTM band in the sample (currently restricted to treated band)
- Dropping tenders with very high n_product8 (outliers)
- Weighted regressions (by estimated cost)

### Section 8: Discussion and Conclusion (2–3 pages)
- Summary of findings in plain language
- Policy implications:
  - Bundling induced by simplified procurement does not undermine SME goals
  - Cross-category consolidation amplifies local SME advantage
  - The "tension" between efficiency (bundling) and equity (SME preference) is weaker than feared
  - Practical recommendation: agencies should monitor what gets bundled (which categories) rather than how much
- Connection to broader literature:
  - Ridderstedt & Nilsson: multi-dimensional bundling matters in goods as in infrastructure
  - Estache & Iimi / Suzuki: entry deterrence is real but operates through selection, not competitive disadvantage (at least in this goods-procurement context)
  - Wolfram et al.: our setting is complementary — they study monitoring × bundling; we study preference × bundling
- Limitations:
  - Cross-sectional identification — bundling is not randomly assigned within buyer
  - Goods procurement may differ from infrastructure/construction
  - Short post-reform window (Dec 2024–Mar 2025)
  - Cannot observe final prices/quality for Compra Ágil (no public bid data in the same format)
- Future work:
  - Cross-market spillovers (Q3 of the main project): does induced bundling in 30–100 UTM affect competition in 100–500 UTM?
  - Structural model: jointly estimate entry, bidding, and bundling to compute welfare
  - Longer post-reform panel as data accumulates

## 5. Tables and figures plan

### Main tables (in paper)
| # | Content | Status |
|---|---------|--------|
| 1 | Summary statistics: sample, outcomes, bundling measures | TODO |
| 2 | DiD: reform effect on bundling outcomes (n_product8, n_segment2, etc.) | DONE (bundling_did_comparison.csv) |
| 3 | Pr(SME wins) on bundle composition (4 cols: pre/post × seg dummies) | DONE |
| 4 | Pr(local SME wins) on bundle composition | DONE |
| 5 | Pr(SME wins) with SME share control (4 cols) | DONE |
| 6 | Pr(local SME wins) with SME share control | DONE |
| 7 | Share of SME bidders on bundle composition | DONE |
| 8 | Share of local SME bidders on bundle composition | DONE |
| 9 | N total bidders on bundle composition | DONE |
| 10 | N SME bidders, N large bidders, N local bidders | DONE |
| 11 | Generalist premium pre vs. post, by tender diversity (bid-level, within-tender FE) | DONE |

### Main figures (in paper)
| # | Content | Status |
|---|---------|--------|
| 1 | Event study: n_product8 and n_segment2 around reform | TODO |
| 2 | Binscatter: n_segment2 vs. Pr(local SME wins), pre vs. post | TODO |
| 3 | Coefficient plot: n_product8 and n_segment2 across all outcome families | TODO |

### Appendix tables
| # | Content | Status |
|---|---------|--------|
| A1 | Pr(large firm wins) baseline + with SME share | DONE |
| A2 | Share large-firm bidders | DONE |
| A3 | Region FE robustness for all key specs | DONE |
| A4 | Alternative bundling measure: diversity (1−HHI) | TODO |
| A5 | Different segment-dummy cutoffs | TODO |
| A6 | Full DiD results with both control groups | DONE (bundling_did_comparison.csv) |

## 6. What we have vs. what we need

### Already done
- Full data pipeline: clean ChileCompra + SII → DiD samples → product mix samples
- DiD on bundling outcomes (two control groups)
- Cross-sectional regressions: Pr(wins), share of bidders, N bidders — all with buyer FE, region FE, segment dummies, SME share controls
- LaTeX tables for all regression results
- Beamer presentation deck with lit review framing

### Still needed

#### Empirical (priority order)
1. **Summary statistics table** — sample sizes, means, SDs for all key variables, split pre/post and by band
2. **Event studies on bundling measures** — monthly coefficients for n_product8 and n_segment2 around the reform date (to show the bundling effect is sharp and coincides with the reform)
3. **Coefficient plot** — visual summary of n_product8 and n_segment2 effects across all outcome families (wins, shares, counts) in a single figure
4. **Binscatter** — n_segment2 vs. Pr(local SME wins) to visualize the key relationship
5. **Robustness: alternative segment-dummy cutoffs** — top 10, top 30 instead of 20
6. **Robustness: diversity (1−HHI)** — show it's absorbed by segment controls (we have this verbally but should formalize)
7. **Robustness: weighted regressions** — by estimated cost

#### Writing
1. Introduction draft
2. Setting section (can draw heavily from existing deck slides)
3. Data section (can draw from product_mix_note and agent_handoff)
4. Empirical strategy section
5. Results write-up (structured around the tables)
6. Robustness discussion
7. Conclusion

## 7. Timeline suggestion

| Week | Task |
|------|------|
| 1 | Summary stats table; event studies on bundling; coefficient plot; binscatter |
| 2 | Robustness regressions (alt segment cutoffs, HHI, weighted); organize all tables |
| 3 | Draft Sections 1–4 (intro, setting, data, empirical strategy) |
| 4 | Draft Sections 5–6 (results, robustness) |
| 5 | Draft Section 7 (discussion/conclusion); assemble full draft |
| 6 | Internal review; revise; circulate |

## 8. Target outlets

Given the contribution (empirical IO / public economics with policy relevance):
- **First tier:** AEJ: Economic Policy, Journal of Public Economics, RAND Journal of Economics
- **Field journals:** Journal of Industrial Economics, International Journal of Industrial Organization
- **If broadened to include structural model:** Econometrica, REStud (but this is a separate paper)
