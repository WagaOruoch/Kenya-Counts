# Kenya Counts — Task Runner
# Usage: make <target>
# Run `make help` to see all available targets.

.PHONY: help scrape validate clean merge indicators analyse dashboard report test all

PYTHON := python

help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

# ── Data Collection ──────────────────────────────────────────────

scrape: ## Run all scrapers → data/raw/
	$(PYTHON) -m scraper.sources.kra_revenue
	$(PYTHON) -m scraper.sources.treasury_budget
	$(PYTHON) -m scraper.sources.cob_expenditure
	$(PYTHON) -m scraper.sources.knbs_indicators
	$(PYTHON) -m scraper.sources.county_allocations
	$(PYTHON) -m scraper.sources.debt_cbk
	$(PYTHON) -m scraper.sources.world_bank_api

# ── Pipeline ─────────────────────────────────────────────────────

validate: ## Validate raw data schemas → print quality report
	$(PYTHON) -m pipeline.validate

clean: ## Clean and normalise raw data → data/clean/
	$(PYTHON) -m pipeline.clean

merge: ## Merge national + county datasets → data/clean/merged.parquet
	$(PYTHON) -m pipeline.merge

indicators: ## Compute derived metrics → data/clean/indicators.parquet
	$(PYTHON) -m pipeline.indicators

pipeline: validate clean merge indicators ## Run full pipeline (validate → clean → merge → indicators)

# ── Analysis ─────────────────────────────────────────────────────

analyse: ## Run all analysis notebooks via nbconvert
	jupyter nbconvert --execute --inplace notebooks/00_data_profile.ipynb
	jupyter nbconvert --execute --inplace notebooks/01_eda_national.ipynb
	jupyter nbconvert --execute --inplace notebooks/02_eda_counties.ipynb
	jupyter nbconvert --execute --inplace notebooks/03_analysis.ipynb
	jupyter nbconvert --execute --inplace notebooks/04_story.ipynb

# ── Dashboard ────────────────────────────────────────────────────

dashboard: ## Launch Streamlit dashboard
	streamlit run dashboard/app.py

# ── Report ───────────────────────────────────────────────────────

report: ## Generate PDF report from story notebook
	jupyter nbconvert --to pdf notebooks/04_story.ipynb --output-dir report/ --output kenya_counts.pdf

# ── Testing ──────────────────────────────────────────────────────

test: ## Run all tests
	pytest tests/ -v

# ── Full Run ─────────────────────────────────────────────────────

all: scrape pipeline analyse report ## Run everything end to end
