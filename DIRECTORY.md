Kenya Counts/
│
├── QUESTION.md                      # The question, context, why it matters
├── STORY.md                         # The narrative outline before any code
├── METHODOLOGY.md                   # Composite score definitions, weights, normalisation
├── TECHSTACK.md                     # Tools, libraries, and rationale for each layer
├── SPRINTS.md                       # Execution plan — 8 sprints, ~5 weeks
│
├── config/
│   ├── counties.yaml                # Canonical list of 47 counties + name      aliases
│   ├── indicators.yaml              # Each indicator: source, unit, direction (higher=better?)
│   ├── date_ranges.yaml             # Fiscal years covered
│   └── sources.yaml                 # URLs, API keys, scraper settings
│
├── data/
│   ├── raw/                         # Untouched scraped data
│   │   └── manifest.json            # When each file was scraped, from which URL
│   └── clean/                       # Filtered, normalised, analysis-ready
│
├── scraper/
│   ├── base.py                      # BaseScraper — retry, rate-limit, logging, caching
│   ├── sources/
│   │   ├── kra_revenue.py           # KRA tax collection data
│   │   ├── treasury_budget.py       # National budget vs expenditure
│   │   ├── cob_expenditure.py       # Controller of Budget — actual county spending
│   │   ├── knbs_indicators.py       # Health, education, infrastructure (KNBS)
│   │   ├── county_allocations.py    # CRA equitable share + county allocations
│   │   ├── debt_cbk.py              # CBK / Treasury — public debt stock & servicing
│   │   └── world_bank_api.py        # Fallback — cleaner access to same KNBS indicators
│   ├── extractors/
│   │   └── pdf_extractor.py         # Shared PDF table extraction (budget policy statements, CoB reports)
│   └── config.yaml                  # Legacy compat — points to config/sources.yaml
│
├── pipeline/
│   ├── validate.py                  # Schema checks, missing data reports, outlier flags
│   ├── clean.py                     # Standardise, normalise, fill gaps
│   ├── merge.py                     # Join national + county datasets
│   └── indicators.py               # Compute derived metrics (per capita, composite scores, growth rates)
│
├── notebooks/
│   ├── 00_data_profile.ipynb        # Shape, completeness, distributions — the trust layer
│   ├── 01_eda_national.ipynb        # Explore national trends
│   ├── 02_eda_counties.ipynb        # Explore county patterns
│   ├── 03_analysis.ipynb            # Answer the questions with evidence
│   └── 04_story.ipynb               # Visualisations that tell the story
│
├── tests/
│   ├── test_cleaners.py             # Unit tests — county name normaliser, date parsing
│   ├── test_merge.py                # Integration — merged dataset has 47 counties × N years
│   └── test_scrapers.py             # Scraper resilience — handles unexpected HTML / missing pages
│
├── dashboard/                       # Streamlit interactive dashboard
│   └── app.py
│
├── report/
│   └── kenya_counts.pdf             # Final written story + visuals (auto-generated)
│
├── .github/
│   └── workflows/
│       ├── scrape.yml               # Scheduled data refresh
│       └── test.yml                 # CI — run tests on every push
│
├── Makefile                         # make scrape | make clean | make analyse | make dashboard
├── README.md                        # The full story for anyone landing here
└── requirements.txt