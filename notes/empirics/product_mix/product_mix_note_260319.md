# Buyer-Side Product Mix After the Compra Agil Expansion

Date: 2026-03-19

## Question

How did the December 12, 2024 policy change alter the buyer side of the market?

Main buyer-side questions:

1. Which kinds of items were previously bought through licitaciones in the treated value band and then moved into Compra Agil?
2. Did procuring entities change the composition of orders?
3. Is there evidence that buyers bundled more products into a single order and/or raised quantities per order?

## Plan

1. Build a harmonized buyer-side line-item sample for the same 1-200 UTM tender universe used in the DiD pipeline.
2. Standardize product codes to a common 8-digit code plus 2-digit, 4-digit, and 6-digit hierarchy buckets.
3. Collapse raw bidder-offer rows into buyer-requested product lines:
   - licitaciones: one row per `(Codigo, Correlativo)`
   - Compra Agil: one row per unique `(CodigoCotizacion, CodigoProducto, ProductoCotizado, CantidadSolicitada)`
4. Study broad product composition in the treated band before vs after the reform:
   - by buyer sector
   - by broad product buckets
   - by exact 8-digit product codes
5. Study buyer-side order structure:
   - number of lines per tender
   - number of distinct product codes per tender
   - number of distinct product families/segments per tender
   - quantity per line/order, with a quantity caveat for Compra Agil
6. Estimate tender-level DiD outcomes on bundling metrics under multiple control-group choices:
   - preferred: `30-100` UTM treated vs `100-200` UTM
   - alternative: `30-100` UTM treated vs `1-30` UTM
   - sensitivity: all three bands together

## Data and construction

New code:

- `code/analysis/product_mix/01_build_product_mix_sample.py`
- `code/analysis/product_mix/02_run_product_mix_analysis.py`

New outputs:

- `output/product_mix/samples/product_mix_lines.parquet`
- `output/product_mix/samples/product_mix_tenders.parquet`
- `output/product_mix/tables/*.csv`

Key fields:

- licitaciones product code: `CodigoProductoONU`
- Compra Agil product code: `CodigoProducto`
- licitaciones quantity: `Cantidad`
- Compra Agil quantity: `CantidadSolicitada`
- licitaciones quantity unit: `UnidadMedida`
- Compra Agil has no comparable quantity-unit field in the cleaned panel

Important caveat:

- Quantity comparisons are safest within exact product codes.
- Tender-level quantity evidence should be treated as suggestive, not fully unit-harmonized, because Compra Agil lacks a comparable unit-of-measure field.
- The 2-digit and 4-digit product-bucket CSVs use a representative product name from each code bucket. For specific items, the exact 8-digit product-code table is the cleanest object to quote.

## Broad results

### 1. The treated band almost fully switched mechanism on the buyer side

In the treated 30-100 UTM band, the CA share of tenders jumped sharply in every buyer sector.

Examples:

- Municipalidades: `3.8% -> 90.9%`
- Obras Publicas: `2.6% -> 92.9%`
- FFAA: `1.2% -> 92.5%`
- Gob. Central / Universidades: `6.9% -> 95.0%`
- Salud: `36.6% -> 97.5%`

Interpretation: the reform did not only reallocate a narrow subset of buyers. The switch from licitaciones to Compra Agil in the treated band is broad-based across procuring entities.

### 2. The product mix that moved is broad, but concentrated in routine goods and small works inputs

The strongest movers in exact 8-digit product codes include:

- `25101702` Vehiculos de policia
- `27113201` Conjuntos generales de herramientas
- `46171505` Llaves
- `60102304` Libros de literatura infantil
- `30102304` Perfiles de acero
- `31162404` Grapas de ferreteria
- `27112801` Brocas
- `53102102` Overol y sobretodo para hombre
- `39111810` Interruptor de lampara
- `39121311` Accesorios electricos
- `12352310` Siliconas
- `31211501` Pinturas al esmalte
- `31211604` Diluyentes para pinturas
- `43211507` Computadores de escritorio

Substantively, the migration is not dominated by one narrow category. It includes:

- maintenance and hardware supplies
- paint and construction materials
- electrical fittings
- tools
- IT equipment
- uniforms/apparel
- books and school materials
- some vehicle-related purchases

That is consistent with a broad buyer-side substitution from formal licitaciones toward quick small-purchase procurement for routine and semi-routine items.

## Bundling and order structure

### 3. Relative to control bands, treated-band tenders changed composition

Preferred tender-level DiD uses only the upper control band (`100-200` UTM), not the `1-30` UTM group.

Preferred tender-level DiD estimates:

- `n_lines`: `-0.185` (`se 0.141`, `p = 0.19`)
- `n_product8`: `+0.374` (`se 0.065`, `p < 0.001`)
- `n_family4`: `+0.234` (`se 0.032`, `p < 0.001`)
- `n_segment2`: `+0.163` (`se 0.019`, `p < 0.001`)
- `single_line`: `+0.009` (`se 0.007`, `p = 0.18`)

Interpretation:

- Against the `100-200` UTM control group, the clean result is not “more lines per order.”
- The stronger result is that treated-band tenders became more diverse in product composition: more distinct product codes, more 4-digit families, and more 2-digit segments inside the same tender.
- So the buyer-side effect is better described as `broader product mix within treated tenders`, rather than mechanically “more lines.”

This is consistent with the idea that buyers started packaging together items that previously would have been split across separate licitaciones.

### 3b. Using `1-30` UTM as the control changes the interpretation

If the control group is instead the lower band (`1-30` UTM), the estimates are:

- `n_lines`: `+0.679` (`se 0.071`, `p < 0.001`)
- `n_product8`: `+0.283` (`se 0.037`, `p < 0.001`)
- `n_family4`: `+0.128` (`se 0.020`, `p < 0.001`)
- `n_segment2`: `+0.055` (`se 0.012`, `p < 0.001`)
- `log(1 + mean quantity requested per line)`: `+0.370` (`se 0.055`, `p < 0.001`)

Interpretation:

- Against very small tenders (`1-30` UTM), treated tenders look more bundled in the literal sense of having more lines.
- Against somewhat larger tenders (`100-200` UTM), the cleaner effect is not more lines, but broader product scope inside the order.

So the control group matters economically:

- `1-30` UTM asks whether the newly eligible tenders start to resemble much smaller purchases.
- `100-200` UTM asks whether the newly eligible tenders start to resemble somewhat larger, still-formal tenders.

That is why the line-count result is sensitive, while the product-breadth and quantity results are much more robust.

### 4. Quantities per order also increased, with a unit caveat

Tender-level DiD on `log(1 + mean quantity requested per line)`:

- `+0.582` (`se 0.040`, `p < 0.001`)

This points in the same direction as the bundling results: treated-band orders became not only more multi-line, but also larger per requested line on average.

Because Compra Agil lacks a comparable quantity-unit field, this should be treated as suggestive evidence rather than a fully unit-harmonized quantity result.

## Specific quantity examples

For several high-migration products, Compra Agil post-reform quantities are comparable to or larger than pre-reform licitacion quantities, rather than obviously smaller:

- `27112801` Brocas:
  - CA mean quantity: `5.3 -> 6.4`
  - licitaciones pre mean quantity: `8.9`
- `30102304` Perfiles de acero:
  - CA mean quantity: `12.0 -> 23.4`
  - licitaciones pre mean quantity: `21.3`
- `31162404` Grapas de ferreteria:
  - CA mean quantity: `14.0 -> 30.3`
  - licitaciones pre mean quantity: `22.4`

These examples are consistent with buyers using Compra Agil not just for tiny fragmented requests, but often for reasonably sized line items.

## Bottom line

The buyer-side response to the reform looks like:

1. A very large mechanism shift in the treated band from licitaciones to Compra Agil.
2. A broad reallocation of routine goods, maintenance supplies, tools, materials, electrical goods, IT items, and apparel into Compra Agil.
3. A relative increase in product breadth inside treated-band orders:
   - more distinct products per order
   - more product-family breadth per order
   - more product-segment breadth per order
4. Suggestive evidence of larger requested quantities per line/order after the reform.

That pattern supports the interpretation that procuring entities adapted on the buyer side by consolidating purchases that were previously separated, not just by swapping one mechanism for another holding the contents of the order fixed.

## Files to inspect

- `output/product_mix/tables/sector_shift_treated_band.csv`
- `output/product_mix/tables/segment_shift_treated_band.csv`
- `output/product_mix/tables/family_shift_treated_band.csv`
- `output/product_mix/tables/product_code_shift_treated_band.csv`
- `output/product_mix/tables/bundling_cell_means_high_control.csv`
- `output/product_mix/tables/bundling_did_tender_high_control.csv`
- `output/product_mix/tables/bundling_cell_means_low_control.csv`
- `output/product_mix/tables/bundling_did_tender_low_control.csv`
- `output/product_mix/tables/bundling_did_comparison.csv`
- `output/product_mix/tables/bundling_cell_means_all_controls.csv`  (sensitivity)
- `output/product_mix/tables/bundling_did_tender_all_controls.csv`  (sensitivity)
- `output/product_mix/tables/top_product_quantity_examples.csv`
