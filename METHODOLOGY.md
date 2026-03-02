# Methodology

> How we measure whether Kenya's tax money since devolution has translated into better, more equal public services.

---

## 1. Data Sources

| Dataset | Source | Format | Coverage |
|---|---|---|---|
| Tax revenue (target & actual) | KRA Annual Reports / National Treasury | Historical + HTML | FY 2013/14 – 2023/24 |
| National budget (allocation & expenditure) | National Treasury Budget Review | Historical + PDF | 4 sectors × 11 FYs |
| Public debt | Central Bank of Kenya / World Bank | Historical + API | 11 FYs |
| County equitable share | Commission on Revenue Allocation | Historical | 47 counties × 11 FYs |
| County expenditure & absorption | Controller of Budget Reports | Historical + PDF | 47 counties × 11 FYs |
| County health & education indicators | KNBS (KDHS 2014, KDHS 2022) | Survey snapshots | 13 counties × 2 years |
| National outcome indicators | World Bank Open Data / KNBS Economic Survey | API + manual | 12 annual values |

### Known limitations

- **County indicator coverage is sparse.** Only 13 of 47 counties have KDHS snapshot data. The remaining 34 counties lack health/education indicator values. This restricts the funds-vs-outcomes analysis to a subset.
- **Uniform absorption rates.** The Controller of Budget publishes aggregate county absorption rates, not per-county breakdowns. We applied the aggregate rate to all counties, which understates variation.
- **World Bank API availability.** Some WB indicator values may be missing for recent years (2023–2024) due to reporting lag.
- **No county population series.** Per-capita calculations require county-level population, which we have not yet sourced (2019 Census is the primary candidate).

---

## 2. Data Pipeline

### 2.1 Validation (`pipeline/validate.py`)

Every raw CSV is checked against a [pandera](https://pandera.readthedocs.io/) schema that enforces:
- Expected column names and types
- Fiscal year format (`YYYY/YY`)
- Value range constraints (e.g., revenue > 0, absorption rate 0–200%)
- Non-null counts for key identifiers

A JSON quality report is written to `data/clean/data_quality_report.json`.

### 2.2 Cleaning (`pipeline/clean.py`)

1. **County name normalisation** — Maps all known aliases (e.g., "Nairobi City" → "Nairobi", "Taita Taveta" → "Taita-Taveta") using the canonical list in `config/counties.yaml`.
2. **Fiscal year alignment** — Converts formats like "2013/2014", "2014" (calendar year), and "2013 / 14" to the canonical "2013/14" label.
3. **Type casting** — Ensures numeric columns are `float64`, handling strings and missing values.
4. **Deduplication** — Drops exact duplicate rows.

### 2.3 Merging (`pipeline/merge.py`)

Produces two panel datasets:

- **`national_panel.csv`** — One row per fiscal year. Joins revenue, budget (pivoted by sector), debt, and World Bank indicators. WB calendar years are mapped to FYs (e.g., WB year 2014 → FY 2013/14).
- **`county_panel.csv`** — One row per county × fiscal year. Joins equitable share allocations, expenditure, and KDHS indicators (pivoted wide).

### 2.4 Derived Indicators (`pipeline/indicators.py`)

| Indicator | Formula | Purpose |
|---|---|---|
| Revenue YoY growth | $(R_t - R_{t-1}) / R_{t-1} \times 100$ | Track revenue trajectory |
| Revenue compliance gap | $(R_{actual} - R_{target}) / R_{target} \times 100$ | Measure KRA shortfall |
| Budget execution rate | $Expenditure_{actual} / Allocation \times 100$ | How much budget was spent |
| Debt-to-revenue ratio | $Debt_{stock} / Revenue_{actual}$ | Fiscal sustainability signal |
| County allocation growth | Per-county YoY % change | Track allocation trajectory |
| Absorption rate (derived) | $Expenditure / Allocation \times 100$ | County spending capacity |

---

## 3. Composite Service Delivery Score

### 3.1 Components

The composite score combines four county-level indicators:

| Component | Direction | Weight |
|---|---|---|
| Skilled birth attendance (%) | Higher is better | Equal (25%) |
| Primary completion rate (%) | Higher is better | Equal (25%) |
| Pupil-to-teacher ratio | Lower is better | Equal (25%) |
| Poverty headcount rate (%) | Lower is better | Equal (25%) |

### 3.2 Normalisation

Each component is min-max normalised to a 0–100 scale:

$$
x_{norm} = \frac{x - x_{min}}{x_{max} - x_{min}} \times 100
$$

For "lower is better" indicators, the scale is inverted: $x_{norm} = 100 - x_{norm}$.

If all values for a component are identical, a default value of 50 is assigned.

### 3.3 Aggregation

The composite score is the **equal-weight arithmetic mean** of all available normalised components:

$$
S = \frac{1}{n} \sum_{i=1}^{n} x_{i,norm}
$$

where $n$ is the number of components with non-null data for that county-year.

### 3.4 Rationale & Sensitivity

Equal weighting was chosen because:
1. No strong theoretical basis exists for prioritising one outcome over another in the Kenyan devolution context.
2. The data is sparse (13 counties × 2 years), making sophisticated weighting (PCA, expert judgement) unreliable.
3. Sensitivity analysis (planned for Sprint 4) will test alternative weightings.

### 3.5 Caveats

- The score is **descriptive, not causal**. A low score does not prove mismanagement; it may reflect structural disadvantages (arid counties, low baseline infrastructure).
- Scores are only computed for counties with at least one non-null component value. Counties without KDHS data receive no score.
- The normalisation window spans all counties and years pooled together, which means scores are relative to the observed range in the dataset, not to an absolute benchmark.

---

## 4. Analysis Approach

### 4.1 National (Chapter 1 of the story)

- **Time-series analysis** of revenue, budget, debt, and sector shares.
- **Compliance gaps** between targets and actuals.
- **Debt burden trajectory** — ratio analysis and growth decomposition.

### 4.2 County (Chapter 2 of the story)

- **Cross-sectional ranking** of allocations, absorption rates, and service delivery scores.
- **Funds-vs-outcomes scatter** — do counties that receive more deliver better services?
- **Coefficient of variation** over time — are inter-county gaps widening or narrowing?
- **Quadrant analysis** — classify counties as over-performers (good outcomes, low funds) or under-performers (poor outcomes, high funds).

### 4.3 Statistical Methods

- Pearson/Spearman correlations for funds-outcomes relationships.
- Year-over-year percentage change for trend analysis.
- Min-max normalisation for composite scoring.
- Coefficient of variation for inequality measurement.
- PCA considered for Sprint 4 if data density improves.

---

## 5. Reproducibility

All code, data pipeline, and configuration are version-controlled. To reproduce:

```bash
pip install -r requirements.txt
make pipeline    # validate → clean → merge → indicators
make test        # verify 67+ tests pass
jupyter lab notebooks/
```

Raw data is generated from historical constants embedded in the scrapers (no live API dependency for baseline data). Live enrichment is attempted but gracefully falls back to historical data.
