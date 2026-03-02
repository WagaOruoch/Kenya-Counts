# Kenya Counts

**"The Devolution Dividend — Has Kenya's Tax Money Worked for Its People?"**

*Eleven years of taxation. Forty-seven counties. One question: what did Kenyans actually get?*

---

## What This Is

A data-driven investigation into whether Kenya's tax revenue — collected since devolution began in 2013 — has translated into meaningfully better and more equally distributed public services for ordinary citizens.

This project scrapes public data from government sources, cleans and merges it into analysis-ready datasets, and tells the story through notebooks, an interactive dashboard, and a written report.

## The Question

> Has Kenya's tax money — collected since devolution in 2013 — translated into meaningfully better and more equally distributed public services for ordinary citizens?

Read the full question framing: [QUESTION.md](QUESTION.md)

## The Story

The project tells its story in four acts:

1. **The Promise** — What devolution was supposed to deliver
2. **The Ledger** — Following the money at the national level
3. **The Divide** — How 47 counties experienced devolution differently
4. **The Verdict** — What the data says, what it can't say, and what should happen next

Read the full narrative outline: [STORY.md](STORY.md)

## Project Structure

See [DIRECTORY.md](DIRECTORY.md) for the full annotated folder structure.

Key layers:
- **`scraper/`** — Collects data from KRA, National Treasury, Controller of Budget, KNBS, CRA, CBK, and the World Bank API
- **`pipeline/`** — Validates, cleans, merges, and computes derived indicators
- **`notebooks/`** — EDA, analysis, and story visualisations
- **`dashboard/`** — Interactive Streamlit app for exploring the data
- **`report/`** — Auto-generated PDF of the final story

## Quick Start

```bash
# Clone the repository
git clone https://github.com/placeholder/kenya-counts.git
cd kenya-counts

# Create a virtual environment
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
.venv\Scripts\activate     # Windows

# Install dependencies
pip install -r requirements.txt

# Run the full pipeline
make scrape      # Collect data from all sources
make pipeline    # Validate → clean → merge → compute indicators
make analyse     # Execute all notebooks
make dashboard   # Launch the Streamlit dashboard
```

## Data Sources

| Source | What It Provides |
|---|---|
| Kenya Revenue Authority (KRA) | Annual tax revenue collections |
| National Treasury | Budget allocations by sector, expenditure reports |
| Controller of Budget (CoB) | Actual county government expenditure |
| Commission on Revenue Allocation (CRA) | Equitable share allocations per county |
| Central Bank of Kenya (CBK) | Public debt stock and servicing costs |
| Kenya National Bureau of Statistics (KNBS) | Health, education, infrastructure, poverty indicators |
| World Bank Open Data | Same indicators as KNBS with cleaner API access |

## Tech Stack

See [TECHSTACK.md](TECHSTACK.md) for the full rationale.

**Core:** Python · httpx · Polars · Jupyter · Plotly · GeoPandas · Streamlit

## Methodology

The composite service delivery score and all analytical choices are documented in [METHODOLOGY.md](METHODOLOGY.md).

## Execution Plan

Development is organised into 8 sprints over ~5 weeks. See [SPRINTS.md](SPRINTS.md) for the full breakdown.

## Contributing

1. Start by reading [QUESTION.md](QUESTION.md) and [STORY.md](STORY.md) — every line of code serves the story
2. Check the sprint plan in [SPRINTS.md](SPRINTS.md) for what's currently being worked on
3. Open an issue or PR against the relevant sprint

## License

This project is released for public benefit. Data sourced from public government records.

---

*"The numbers do not lie, but they do not tell the whole story either."*
