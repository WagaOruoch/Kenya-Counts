# Execution Sprints

Development is broken into 8 sprints. Each sprint delivers something testable before the next begins.

---

## Sprint 0 — Foundation (2–3 days)
**Goal:** Anyone can clone the repo and understand the project.

- [x] Scaffold the full folder structure from DIRECTORY.md
- [x] Create `requirements.txt` with pinned versions
- [x] Create `config/counties.yaml` — canonical list of 47 counties with known aliases
- [x] Create `config/sources.yaml` — every URL/API endpoint to scrape
- [x] Create `config/date_ranges.yaml` — fiscal years 2013/14 through 2024/25
- [x] Create `config/indicators.yaml` — every metric, its source, unit, and direction
- [x] Write `Makefile` with placeholder targets
- [x] Set up `.github/workflows/test.yml` — runs pytest on push
- [x] Set up `.github/workflows/scrape.yml` — scheduled monthly data refresh
- [x] Write `README.md`

**Deliverable:** A repo anyone can clone, understand, and contribute to.

---

## Sprint 1 — Scraping (5–7 days)
**Goal:** All raw data lands in `data/raw/` with a manifest.

- [x] Build `scraper/base.py` — BaseScraper with httpx, tenacity retry, rate limiting, logging, file caching
- [x] Build `scraper/extractors/pdf_extractor.py` — shared pdfplumber/camelot logic
- [x] Build scrapers one by one:
  1. `kra_revenue.py` — KRA annual revenue (easiest, well-structured)
  2. `world_bank_api.py` — health/education/poverty indicators via wbgapi (clean API)
  3. `treasury_budget.py` — national budget allocations (likely PDF extraction)
  4. `debt_cbk.py` — public debt stock and servicing costs
  5. `county_allocations.py` — CRA equitable share allocations
  6. `cob_expenditure.py` — Controller of Budget actual county spending (hardest)
  7. `knbs_indicators.py` — county-level indicators not available via World Bank
- [x] Generate `data/raw/manifest.json` after each scrape run
- [x] Write `tests/test_scrapers.py` — mock tests for each scraper (22/22 passing)

**Deliverable:** `make scrape` fills `data/raw/` with all datasets.

---

## Sprint 2 — Pipeline (3–4 days) ✅
**Goal:** Raw data becomes analysis-ready in `data/clean/`.

- [x] Build `pipeline/validate.py` — pandera schemas for each raw dataset, produces a data quality report
- [x] Build `pipeline/clean.py` — county name normalisation, fiscal year alignment, missing value handling
- [x] Build `pipeline/merge.py` — join national + county datasets on consistent keys
- [x] Build `pipeline/indicators.py` — per capita calculations, growth rates, composite score
- [x] Write `tests/test_cleaners.py` and `tests/test_merge.py`

**Deliverable:** `make pipeline` produces clean, joined datasets. Tests pass. *(67/67 passing)*

---

## Sprint 3 — Exploratory Analysis (4–5 days) ✅
**Goal:** Understand what the data actually says before telling the story.

- [x] `00_data_profile.ipynb` — shape, completeness, distributions, correlations
- [x] `01_eda_national.ipynb` — national revenue trends, budget vs expenditure, debt burden, sector spending
- [x] `02_eda_counties.ipynb` — equitable share per capita, county outcome distributions, outliers, clustering
- [x] Document surprises, gaps, and limitations
- [x] Finalise `METHODOLOGY.md` — lock in composite score method based on data availability

**Deliverable:** Three completed notebooks. Methodology finalised.

---

## Sprint 4 — Analysis (3–4 days)
**Goal:** Answer every sub-question in QUESTION.md with evidence.

- [ ] `03_analysis.ipynb` — structured answers to:
  - Chapter 1 (National): Revenue growth, allocation vs expenditure gaps, debt share, outcome-per-shilling
  - Chapter 2 (Counties): Funds vs outcomes scatter, over/underperformers, gap trends, composite ranking
- [ ] PCA or weighted index for composite score — run sensitivity analysis
- [ ] Statistical tests where appropriate (correlation, trend significance)

**Deliverable:** Every sub-question has a data-backed answer.

---

## Sprint 5 — Story & Visuals (4–5 days)
**Goal:** The four-act narrative from STORY.md becomes a visual reality.

- [ ] `04_story.ipynb` — one notebook, four acts:
  - Act 1: Revenue line chart (2013–2024) with annotated events
  - Act 2: Sankey diagram (KSh 100 flow), sector spending bars, outcome trends
  - Act 3: County choropleth map, funds-vs-outcomes scatter, outlier tables
  - Act 4: Summary findings, limitations, closing statement
- [ ] Apply STORY.md visualisation principles — insight-first titles, per-capita, intentional colour

**Deliverable:** A presentation-ready narrative notebook.

---

## Sprint 6 — Dashboard (3–4 days)
**Goal:** An interactive version anyone can explore.

- [ ] `dashboard/app.py` — Streamlit app with:
  - National overview page (revenue, budget, outcomes)
  - County explorer (select a county → allocation, spending, outcomes)
  - County comparison (pick 2–5 counties, compare side by side)
  - Interactive choropleth via streamlit-folium
  - Downloadable data tables

**Deliverable:** `make dashboard` launches a working Streamlit app.

---

## Sprint 7 — Polish & Ship (2–3 days)
**Goal:** The project is complete, reproducible, and public-ready.

- [ ] Auto-generate `report/kenya_counts.pdf` from `04_story.ipynb`
- [ ] Finalise `README.md` — project summary, key findings, how to reproduce
- [ ] Final review: all tests pass, all notebooks run end-to-end, dashboard works
- [ ] Verify all four success criteria from QUESTION.md are met

**Deliverable:** A complete, shippable project.

---

## Timeline

| Sprint | Focus | Duration | Cumulative |
|---|---|---|---|
| **0** | Foundation | 2–3 days | ~3 days |
| **1** | Scraping | 5–7 days | ~10 days |
| **2** | Pipeline | 3–4 days | ~14 days |
| **3** | Exploratory Analysis | 4–5 days | ~19 days |
| **4** | Analysis | 3–4 days | ~23 days |
| **5** | Story & Visuals | 4–5 days | ~28 days |
| **6** | Dashboard | 3–4 days | ~32 days |
| **7** | Polish & Ship | 2–3 days | ~35 days |

**Total: ~5 weeks** — Sprint 1 (scraping) is the highest-risk sprint. Budget extra time there.
