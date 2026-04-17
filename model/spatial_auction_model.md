# A Spatial Auction Model with Entry and Local Preferences

## 1. Environment

### Regions and Geography

There are $R$ regions indexed by $r \in \{1, \dots, R\}$, with pairwise distances $d_{rs} \geq 0$ and $d_{rr} = 0$.

### Government Demand

Each period, region $r$ posts $Q_r \geq 1$ procurement projects, each with a publicly known reserve price $\bar{p}$. The vector $\mathbf{Q} = (Q_1, \dots, Q_R)$ captures heterogeneous demand intensity across regions. Regions with higher $Q_r$ (e.g., Santiago) can sustain thicker markets; regions with low $Q_r$ (e.g., remote areas) may struggle to support even a small number of active firms.

### Firms

There is a set of potential firms $\mathcal{F}$. Each firm $i \in \mathcal{F}$ has a home region $h_i \in \{1, \dots, R\}$. Let $N_r^{pot} = |\{i \in \mathcal{F} : h_i = r\}|$ denote the number of potential local firms in region $r$. Each firm has capacity $K_i \geq 1$: the maximum number of projects it can execute per period.

---

## 2. Timing

1. **Demand realization.** The vector $\mathbf{Q}$ is realized and publicly observed.
2. **Entry.** Firms simultaneously choose which markets to enter. Firm $i$ selects a set $M_i \subseteq \{1, \dots, R\}$, subject to the capacity constraint $|M_i| \leq K_i$. Entering market $r$ costs $\kappa_r^i$.
3. **Cost realization.** Each entrant draws a private cost for each project in each entered market.
4. **Bidding.** For each project, entrants submit sealed bids in a first-price auction. The lowest bidder wins and receives their bid as payment.

---

## 3. Cost Structure

Firm $i$'s cost for a project in region $r$ is:

$$c_{ir} = \underbrace{\bar{c}}\_{{\text{baseline}}} + \underbrace{\delta \cdot d_{h_i, r}}\_{\text{distance cost}} - \underbrace{\lambda \cdot \mathbf{1}[h_i = r]}\_{\text{local advantage}} + \underbrace{\epsilon_{ir}}\_{\text{private shock}}$$

where:

- $\bar{c} > 0$: common baseline project cost.
- $\delta > 0$: per-unit-distance cost (transport of materials, worker travel, logistics of remote supervision).
- $\lambda > 0$: local informational and relational advantage. Local firms benefit from knowledge of local regulations, established subcontractor networks, familiarity with terrain and climate, and relationships with local permitting authorities. This is conceptually distinct from the distance cost $\delta \cdot d$.
- $\epsilon_{ir} \sim F(\cdot)$ i.i.d. on $[0, \bar{\epsilon}]$, with density $f > 0$: idiosyncratic private cost shock.

**Key decomposition.** A non-local firm from region $s \neq r$ faces an expected cost disadvantage of $\delta \cdot d_{sr} + \lambda$ relative to a local firm. The first component is pure distance; the second captures non-distance local advantages.

---

## 4. Entry Costs

Firm $i$'s cost of entering market $r$ is:

$$\kappa_r^i = \kappa_0 + \kappa_d \cdot d_{h_i, r}$$

where $\kappa_0 > 0$ is the baseline cost of market participation (preparing bids, learning about available projects) and $\kappa_d > 0$ captures the additional cost of setting up operations at a distance. Local firms pay only $\kappa_0$.

---

## 5. Policy Environment

### Pre-Reform

All firms in $\mathcal{F}$ may enter any market $r \in \{1, \dots, R\}$.

### Post-Reform (Local Preference)

The reform introduces two tiers of local preference, defined by project size thresholds:

**Strong form (Compra Ágil, projects below $\underline{v}$ = 100 UTM):** Only firms with $h_i = r$ and EMT status may enter market $r$. Non-local firms are fully excluded.

**Weak form (projects between $\underline{v}$ and $\bar{v}$ = 500 UTM):** All firms may enter, but local firms receive a bid preference $\alpha > 0$. Non-local firm $i$ wins only if $b_i < b_j - \alpha$ for all local entrants $j$. Equivalently, local bids are evaluated as $b_j - \alpha$.

**No preference (projects above $\bar{v}$):** Status quo; all firms compete on equal terms.

For clarity, the theoretical analysis focuses on the strong-form case (full exclusion), which generates the starkest predictions. The weak-form case produces qualitatively similar but attenuated effects.

---

## 6. Equilibrium

### 6.1. Bidding Stage

Conditional on $n_r$ entrants in market $r$ with (potentially asymmetric) cost distributions, the bidding stage is a standard asymmetric first-price auction. The key properties are:

1. **Expected winning bid** $E[p_r \mid n_r]$ is decreasing in $n_r$: more entrants means more competition and lower procurement costs.
2. **Expected winning bid** is increasing in entrants' cost levels: if the pool of bidders has higher mean costs, the government pays more.

In the symmetric case with $n$ entrants drawing costs from $F(\cdot \mid \mu)$, the expected payment can be decomposed as:

$$E[p_r \mid n] = \underbrace{E[c_{(1:n)}]}\_{\text{efficient cost}} + \underbrace{\frac{1}{n} E\left[\frac{F(c_{(1:n)})}{f(c_{(1:n)})}\right]}\_{\text{markup}}$$

where $c_{(1:n)}$ is the minimum order statistic. The markup term is decreasing in $n$: market power falls with competition. Both terms matter for the policy analysis — the policy changes both the composition of the cost distribution (via the types of firms that enter) and the level of competition (via the number of entrants).

### 6.2. Entry Stage

Firm $i$'s expected profit from entering market $r$, given a total of $n_r$ entrants, is:

$$\pi_{ir}(n_r) = Q_r \cdot \Pi_{ir}^{bid}(n_r) - \kappa_r^i$$

where $\Pi_{ir}^{bid}(n_r)$ is the expected per-project bidding profit. The term $Q_r$ is key: more projects in a region means higher expected revenue from entry, and therefore more firms find it profitable to enter.

Firm $i$ chooses its entry set to solve:

$$\max_{M_i \subseteq \{1,\dots,R\},\ |M_i| \leq K_i} \sum_{r \in M_i} \pi_{ir}(n_r^e)$$

where $n_r^e$ is the (anticipated) equilibrium number of entrants in market $r$.

### 6.3. Free-Entry Equilibrium

An entry equilibrium is a vector $(n_1^*, \dots, n_R^*)$ such that, for each market $r$ and each entering firm $i$:

$$Q_r \cdot \Pi_{ir}^{bid}(n_r^*) \geq \kappa_r^i$$

with equality for the marginal entrant. This pins down $n_r^*$ as a function of $Q_r$, entry costs, and cost distributions.

Crucially, the capacity constraint $|M_i| \leq K_i$ links markets: firm $i$'s decision to enter market $r$ depends on its opportunities in all other markets. The equilibrium must be solved as a system across all $R$ markets simultaneously.

---

## 7. Effects of the Local Preference Policy

### 7.1. Direct Effect: Exclusion of Non-Local Firms

In market $r$, let:
- $n_r^{pre}$ = total pre-reform entrants (local + non-local)
- $n_r^{L}$ = local entrants among them
- $n_r^{NL} = n_r^{pre} - n_r^{L}$ = non-local entrants

The policy immediately reduces the effective bidding pool from $n_r^{pre}$ to $n_r^{L}$. The government pays more if $n_r^L < n_r^{pre}$ and the excluded non-local firms were cost-competitive.

### 7.2. Entry Response: Local Entry Induced by Reduced Competition

With non-local firms excluded, expected per-project profits for local firms in $r$ rise. This induces entry by potential local firms who previously found it unprofitable. The new post-reform equilibrium number of local entrants $\tilde{n}_r^L$ satisfies:

$$Q_r \cdot \Pi^{bid}(\tilde{n}_r^L) = \kappa_0$$

Whether competition recovers — whether $\tilde{n}_r^L$ approaches or exceeds $n_r^{pre}$ — depends on three key factors:

**(a) Pool of potential local firms ($N_r^{pot}$).** If $N_r^{pot}$ is large relative to the equilibrium number of entrants, new entry can fully replace excluded firms. If $N_r^{pot}$ is small (e.g., a remote region with few firms of the relevant type), entry cannot compensate and competition falls.

**(b) Demand volume ($Q_r$).** Higher $Q_r$ supports more entrants because each entrant expects more auction opportunities per period. Regions with thick demand can sustain more local firms even without non-local competition. Regions with thin, lumpy demand cannot.

**(c) Pre-reform cost advantage of non-local firms ($\delta \cdot d_{sr} + \lambda$).** If excluded non-local firms were from distant regions (high $d_{sr}$), they had high costs and low profits, so their exclusion creates modest rents for locals. If they were from nearby regions (low $d_{sr}$) and experienced (low effective $\lambda$), their exclusion creates large rents, stimulating more local entry — but also meaning the government loses access to genuinely low-cost bidders.

### 7.3. Cross-Market Reallocation via Capacity Constraints

This is where the capacity constraint $K_i$ generates cross-market spillovers, which are central to the multi-market contribution of this framework.

Consider a firm $i$ based in Santiago ($h_i = S$) that pre-reform entered both Santiago and a rural region $r$, using 2 of its $K$ capacity units. Post-reform:

1. Firm $i$ is excluded from $r$ and recovers one unit of capacity.
2. If there exists another market $s$ where $\pi_{is}(n_s^e) > 0$ and firm $i$ was previously capacity-constrained, firm $i$ now enters $s$.
3. If no such market exists, firm $i$ may redirect capacity to Santiago (e.g., taking on additional Santiago projects or entering above-threshold procurements).

**Effect on Santiago (and other origin regions):** Redirected capacity increases the number of entrants, intensifying competition and reducing procurement costs. The local preference policy in peripheral regions generates a positive competition spillover to urban markets.

**Formally,** the entry equilibrium must be solved as a system:

$$n_r^*\left(\{n_s^*\}_{s \neq r}\right) \quad \forall\ r \in \{1, \dots, R\}$$

The best response in each market depends on the number of entrants in all other markets through the capacity constraint. This is the sense in which studying all markets simultaneously — rather than one market at a time, as in Krasnokutskaya and Seim (2011) or Athey et al. — yields different and richer predictions.

### 7.4. Comparative Statics: When Does the Policy Reduce vs. Increase Procurement Costs?

#### The policy is more likely to *reduce* government procurement costs when:

1. **High $N_r^{pot}$ relative to $Q_r$.** Large pool of potential local firms relative to demand. Entry replaces or exceeds excluded non-local firms, maintaining or increasing competition.

2. **High $\lambda$.** Large local informational advantage means non-local firms were already high-cost. Their exclusion removes bidders who were unlikely to win or who bid high, and therefore has minimal impact on the winning bid distribution.

3. **High $\delta \cdot d_{sr}$.** Non-local entrants came from distant regions and had high costs. Same logic as above.

4. **Low $K$ (tight capacity).** Non-local firms' exit frees capacity that is redirected to their home markets, generating competition spillovers that benefit those markets.

#### The policy is more likely to *increase* government procurement costs when:

1. **Low $N_r^{pot}$ relative to $Q_r$.** Few potential local firms and thin demand. Cannot replace lost competition.

2. **Low $\lambda$ and low $\delta \cdot d_{sr}$.** Non-local firms were genuinely competitive — nearby, experienced, low-cost. Their exclusion removes the best bidders.

3. **High $K$ (ample capacity).** No meaningful cross-market reallocation; capacity freed by exclusion has nowhere productive to go.

4. **Lumpy, irregular demand ($Q_r$ volatile).** In bad demand periods, the local market cannot sustain enough firms to maintain competition.

---

## 8. Welfare

### 8.1. Government Procurement Welfare

The government's surplus from procurement in region $r$:

$$W_r^{gov} = Q_r \cdot \left(\bar{p} - E[p_r]\right)$$

where $E[p_r]$ is the expected winning bid.

### 8.2. Local Economic Benefit

The local economic benefit from procurement in region $r$:

$$W_r^{local} = Q_r \cdot \Pr[\text{local firm wins in } r] \cdot w_r$$

where $w_r$ captures local employment, wages, and multiplier effects from local firm activity. A project executed by a local firm generates more local economic impact than one executed by a non-local firm.

### 8.3. Total Welfare

$$W = \sum_{r=1}^{R} \left[W_r^{gov} + \phi \cdot W_r^{local}\right]$$

where $\phi \geq 0$ is the social weight on local economic activity (reflecting policy preferences for regional development, employment equity, etc.).

### 8.4. Welfare Effect of the Policy

The change in welfare is:

$$\Delta W = \sum_{r=1}^{R} Q_r \left[\underbrace{-(E[\tilde{p}_r] - E[p_r])}\_{\text{cost effect}\ (\leq 0\ \text{or}\ \geq 0)} + \underbrace{\phi \cdot \Delta\left(\Pr[\text{local wins}]_r \cdot w_r\right)}\_{\text{local benefit effect}\ (\geq 0)}\right]$$

**The sign of $\Delta W$ is ambiguous and varies across regions.** This is the core tension of the paper:

- In regions where local entry compensates for excluded non-local competition, the cost effect may be small or even positive (if local firms were actually cheaper), and the local benefit is large. The policy improves welfare.
- In regions with few potential local firms and previously competitive non-local entrants, the cost effect is large and negative, while the local benefit may be modest (few local firms to win anyway). The policy reduces welfare.

**Optimal policy** would set the local preference threshold (or bid preference $\alpha$) to vary across regions, being more generous in regions where the local benefit is high and the competition cost is low. The uniform threshold imposed by the Chilean reform is necessarily a blunt instrument, and the welfare analysis can quantify the cost of this uniformity.

---

## 9. Empirically Testable Predictions

The model generates region-level predictions that map directly to the event study designs:

### Prediction 1: Heterogeneous Competition Effects
The reform reduces the number of bidders more in regions where (a) non-local firms held a larger pre-reform market share, (b) the pool of potential local firms $N_r^{pot}$ is small, and (c) demand $Q_r$ is thin.

*Test:* Event study with treatment intensity = pre-reform non-local share, interacted with proxies for $N_r^{pot}$ and $Q_r$.

### Prediction 2: Cross-Market Spillovers
In regions that are net "exporters" of firms (e.g., Santiago), the reform increases competition in above-threshold procurements, as firms redirected from peripheral markets re-enter the home market.

*Test:* Compare number of bidders in above-threshold projects in Santiago-type regions, before vs. after reform.

### Prediction 3: Entry Response
In regions with large $N_r^{pot}$, new local firms enter the procurement market post-reform, partially or fully offsetting the loss of non-local competition.

*Test:* Track first-time local bidders in ChileCompra by region, pre vs. post reform.

### Prediction 4: Price Effects Depend on Market Thickness
The reform increases procurement costs more in thin markets (low $Q_r$, low $N_r^{pot}$) and may decrease costs in thick markets where local entry is strong.

*Test:* Heterogeneous price effects by pre-reform market thickness measures.

### Prediction 5: Local Employment Gains
Local employment in the procurement sector increases more in regions where local firms' win share rises most — which is where non-local firms were previously dominant.

*Test:* Combine ChileCompra data with Central Bank employer-employee data. Event study on local sector employment with treatment intensity = pre-reform non-local share.

### Prediction 6: Bunching Around Thresholds
Government agencies strategically size projects relative to the 100 and 500 UTM thresholds. The direction of bunching reveals agencies' revealed preferences over local vs. non-local suppliers.

*Test:* McCrary density tests / Cattaneo-Jansson-Ma manipulation tests at thresholds, pre vs. post reform.
