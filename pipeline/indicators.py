"""
Derived Indicators — compute per-capita, growth, gaps, and composite scores.

Operates on the merged panels produced by pipeline/merge.py:
  - data/clean/national_panel.csv
  - data/clean/county_panel.csv

Outputs enriched versions and a standalone indicators table:
  - data/clean/national_enriched.csv
  - data/clean/county_enriched.csv
  - data/clean/composite_scores.csv

Usage:
    python -m pipeline.indicators
    # or: make indicators
"""

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

logger = logging.getLogger("kenya_counts.pipeline.indicators")

ROOT_DIR = Path(__file__).resolve().parent.parent
CONFIG_DIR = ROOT_DIR / "config"
DATA_CLEAN = ROOT_DIR / "data" / "clean"


# ── Helpers ──────────────────────────────────────────────────────

def _fy_sort_key(fy: str) -> int:
    try:
        return int(str(fy).split("/")[0])
    except (ValueError, IndexError):
        return 0


def load_indicator_config() -> dict:
    """Load indicators.yaml."""
    with open(CONFIG_DIR / "indicators.yaml", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _read(name: str) -> pd.DataFrame | None:
    path = DATA_CLEAN / f"{name}.csv"
    if not path.exists():
        logger.warning("File missing: %s", path)
        return None
    return pd.read_csv(path)


def pct_change_by_group(
    df: pd.DataFrame,
    value_col: str,
    sort_col: str = "fiscal_year",
    group_cols: list[str] | None = None,
    new_col: str | None = None,
) -> pd.DataFrame:
    """
    Compute year-over-year percentage change for a value column.
    Adds a new column `{value_col}_growth_pct` (or custom name).
    """
    out_col = new_col or f"{value_col}_growth_pct"
    df = df.sort_values(
        [*(group_cols or []), sort_col],
        key=lambda s: s.map(_fy_sort_key) if s.name == sort_col else s,
    )
    if group_cols:
        df[out_col] = df.groupby(group_cols)[value_col].pct_change() * 100
    else:
        df[out_col] = df[value_col].pct_change() * 100
    return df


def per_capita(
    df: pd.DataFrame,
    value_col: str,
    population_col: str = "wb_population_total",
    new_col: str | None = None,
    multiplier: float = 1.0,
) -> pd.DataFrame:
    """
    Divide a value column by population to derive per-capita figures.
    `multiplier` allows unit conversion (e.g. billions → actual KSh).
    """
    out_col = new_col or f"{value_col}_per_capita"
    if population_col in df.columns:
        df[out_col] = (df[value_col] * multiplier) / df[population_col]
    else:
        logger.warning(
            "Population column '%s' missing — skipping per_capita for '%s'",
            population_col,
            value_col,
        )
        df[out_col] = np.nan
    return df


# ── National Enrichment ──────────────────────────────────────────

def enrich_national(df: pd.DataFrame) -> pd.DataFrame:
    """Add growth rates, per-capita, and compliance gaps to national panel."""
    df = df.copy()

    # Revenue growth
    if "revenue_actual_bn" in df.columns:
        df = pct_change_by_group(df, "revenue_actual_bn")

    # Revenue compliance gap (actual vs target)
    if {"revenue_actual_bn", "revenue_target_bn"}.issubset(df.columns):
        df["revenue_compliance_gap_pct"] = (
            (df["revenue_actual_bn"] - df["revenue_target_bn"])
            / df["revenue_target_bn"]
            * 100
        )

    # Budget execution gap (total)
    alloc_col = "allocation_bn_total"
    exp_col = "expenditure_actual_bn_total"
    if {alloc_col, exp_col}.issubset(df.columns):
        df["budget_execution_rate_pct"] = df[exp_col] / df[alloc_col] * 100

    # Debt-to-revenue ratio
    if {"debt_stock_bn", "revenue_actual_bn"}.issubset(df.columns):
        df["debt_to_revenue_ratio"] = df["debt_stock_bn"] / df["revenue_actual_bn"]

    # Per capita: revenue
    if "revenue_actual_bn" in df.columns:
        df = per_capita(df, "revenue_actual_bn", multiplier=1e9)

    # Debt stock growth
    if "debt_stock_bn" in df.columns:
        df = pct_change_by_group(df, "debt_stock_bn")

    return df


# ── County Enrichment ────────────────────────────────────────────

def enrich_county(df: pd.DataFrame) -> pd.DataFrame:
    """Add per-capita allocation, absorption rates, and growth to county panel."""
    df = df.copy()

    # Re-compute absorption rate if both columns available
    if {"allocation_mn", "expenditure_mn"}.issubset(df.columns):
        mask = df["allocation_mn"] > 0
        df.loc[mask, "absorption_rate_derived"] = (
            df.loc[mask, "expenditure_mn"] / df.loc[mask, "allocation_mn"] * 100
        )

    # Year-over-year allocation growth per county
    if "allocation_mn" in df.columns:
        df = pct_change_by_group(
            df, "allocation_mn", group_cols=["county_code"]
        )

    return df


# ── Composite Service Delivery Score ─────────────────────────────

COMPOSITE_COMPONENTS = [
    # (column_name_in_county_panel, direction)
    # After pivot, KNBS indicator columns are named ind_{indicator_id}
    ("ind_skilled_birth_attendance", "higher_is_better"),
    ("ind_primary_completion", "higher_is_better"),
    ("ind_pupil_teacher_ratio", "lower_is_better"),
    ("ind_poverty_headcount", "lower_is_better"),
    # ind_hospital_beds_per_1000 if available at county level
]


def min_max_normalise(
    series: pd.Series, direction: str = "higher_is_better"
) -> pd.Series:
    """
    Min-max normalise to [0, 100]. If direction is 'lower_is_better',
    the scale is inverted (lower raw → higher score).
    """
    smin, smax = series.min(), series.max()
    if smax == smin:
        return pd.Series(50.0, index=series.index)
    normed = (series - smin) / (smax - smin) * 100
    if direction == "lower_is_better":
        normed = 100 - normed
    return normed


def compute_composite_score(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build a 0–100 composite service delivery score per county × FY.
    Equal-weight average of normalised component indicators.

    Returns a DataFrame with columns:
      fiscal_year, county_code, county_name, service_delivery_score,
      plus individual normalised component columns.
    """
    key_cols = ["fiscal_year", "county_code", "county_name"]
    avail = [c for c, _ in COMPOSITE_COMPONENTS if c in df.columns]

    if not avail:
        logger.warning("No composite components found in data — skipping score")
        return pd.DataFrame(columns=key_cols + ["service_delivery_score"])

    out = df[key_cols].copy()
    normed_cols = []

    for col in avail:
        direction = next(
            d for c, d in COMPOSITE_COMPONENTS if c == col
        )
        norm_col = f"{col}_norm"
        out[norm_col] = min_max_normalise(df[col], direction)
        normed_cols.append(norm_col)

    out["service_delivery_score"] = out[normed_cols].mean(axis=1).round(1)
    out["components_available"] = len(normed_cols)

    return out


# ── Save ─────────────────────────────────────────────────────────

def _save(df: pd.DataFrame, name: str) -> Path:
    DATA_CLEAN.mkdir(parents=True, exist_ok=True)
    path = DATA_CLEAN / f"{name}.csv"
    df.to_csv(path, index=False)
    logger.info("Saved %s — %d rows × %d cols", path.name, len(df), len(df.columns))
    return path


# ── Main ─────────────────────────────────────────────────────────

def compute_all() -> dict[str, pd.DataFrame]:
    results = {}

    # National
    national = _read("national_panel")
    if national is not None and len(national):
        enriched = enrich_national(national)
        _save(enriched, "national_enriched")
        results["national_enriched"] = enriched

    # County
    county = _read("county_panel")
    if county is not None and len(county):
        enriched = enrich_county(county)
        _save(enriched, "county_enriched")
        results["county_enriched"] = enriched

        # Composite score
        composite = compute_composite_score(enriched)
        if len(composite):
            _save(composite, "composite_scores")
            results["composite_scores"] = composite

    return results


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    )
    results = compute_all()
    for name, df in results.items():
        print(f"{name}: {df.shape[0]} rows × {df.shape[1]} columns")


if __name__ == "__main__":
    main()
