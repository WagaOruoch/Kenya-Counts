# Tech Stack

## Scraping

| Tool | Role | Why |
|---|---|---|
| **`httpx`** | HTTP client | Async support, cleaner API than `requests`, handles retries natively with `tenacity` |
| **`tenacity`** | Retry logic | Decorator-based retries with exponential backoff — essential for flaky government sites |
| **`selectolax`** or **`beautifulsoup4`** | HTML parsing | `selectolax` is 10–20x faster; fall back to `bs4` only if you need its leniency with broken markup |
| **`pdfplumber`** | PDF table extraction | Best accuracy for the structured tables in Treasury/CoB budget PDFs |
| **`camelot-py`** | PDF fallback | Handles multi-page tables and lattice-based PDFs that `pdfplumber` struggles with |
| **`wbgapi`** | World Bank API | Official Python client — clean access to the same indicators KNBS publishes, with proper pagination |

## Pipeline & Data

| Tool | Role | Why |
|---|---|---|
| **`polars`** | DataFrames | 5–50x faster than `pandas` for the size of data you'll have; lazy evaluation catches schema errors early. If you're more comfortable with `pandas`, it works fine too — the datasets aren't huge. |
| **`pandera`** | Schema validation | Define expected schemas for each dataset in `validate.py` — catches breakage the moment a scraper returns unexpected data |
| **`pyyaml`** | Config loading | For the `config/` YAML files |

## Analytics & Notebooks

| Tool | Role | Why |
|---|---|---|
| **`jupyter`** | Notebooks | Standard for the 5 notebook pipeline |
| **`matplotlib` + `seaborn`** | Static plotting | Reliable, publication-quality, full control |
| **`plotly`** | Interactive charts | For the Sankey diagram (Act 2) and any chart that benefits from hover/zoom |
| **`geopandas` + `mapclassify`** | Choropleth maps | Kenya's 47-county shapefile + classification for the Act 3 map |
| **`scikit-learn`** | PCA / clustering | For the composite score (if you go the PCA route) and identifying county clusters |

## Dashboard

| Tool | Role | Why |
|---|---|---|
| **`streamlit`** | Web dashboard | Low friction, Python-native, handles `plotly` and `geopandas` natively |
| **`streamlit-folium`** | Interactive maps | Embeds Leaflet maps in Streamlit — better interactivity than static `geopandas` plots for the county choropleth |

## Report Generation

| Tool | Role | Why |
|---|---|---|
| **`nbconvert`** | Notebook → HTML/PDF | Auto-generates the report from `04_story.ipynb`, keeping the PDF in sync with the analysis |
| **`weasyprint`** | HTML → PDF | If you want finer control over the final PDF layout than `nbconvert` alone provides |

## Testing & CI

| Tool | Role | Why |
|---|---|---|
| **`pytest`** | Test runner | Industry standard, simple, handles the three test files cleanly |
| **`pytest-httpx`** | Mock HTTP | Mock scraper responses without hitting live government sites during CI |
| **GitHub Actions** | CI/CD | Runs tests on push, scheduled scraping on cron |

## Task Runner

| Tool | Role | Why |
|---|---|---|
| **`make`** (via `Makefile`) | Orchestration | `make scrape`, `make clean`, `make analyse`, `make dashboard` — one command per stage, language-agnostic, works everywhere |

---

## What to Avoid

- **Scrapy** — overkill for 7 targeted government sources; `httpx` + `tenacity` is lighter and more debuggable.
- **Airflow / Prefect** — the pipeline is linear and runs on a cron schedule; a `Makefile` + GitHub Actions is sufficient.
- **Dash (Plotly)** — more flexible than Streamlit but far more boilerplate; not worth it unless you need callback-heavy interactivity.
- **Heavy ORMs / databases** — the data fits comfortably in CSVs/Parquet files. A database adds complexity with no benefit at this scale.
