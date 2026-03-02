"""
Data Cleaning — Normalise, type-cast, and fill raw datasets.

Steps for every dataset:
1.  Strip whitespace from all string columns
2.  Normalise county names to canonical form (config/counties.yaml)
3.  Normalise fiscal-year labels to "YYYY/YY"
4.  Cast numeric columns to float
5.  Drop exact-duplicate rows
6.  Write clean CSV to data/clean/<source>/

Usage:
    python -m pipeline.clean          # clean all
    python -m pipeline.clean revenue  # clean one dataset
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

logger = logging.getLogger("kenya_counts.pipeline.clean")

ROOT_DIR = Path(__file__).resolve().parent.parent
CONFIG_DIR = ROOT_DIR / "config"
DATA_RAW = ROOT_DIR / "data" / "raw"
DATA_CLEAN = ROOT_DIR / "data" / "clean"


# ── County Name Normalisation ───────────────────────────────────

def load_county_lookup() -> dict[str, dict[str, str]]:
    """
    Build a lookup: lowered alias/name → {name, code}.
    """
    with open(CONFIG_DIR / "counties.yaml", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    lookup: dict[str, dict[str, str]] = {}
    for county in data["counties"]:
        canonical = county["name"]
        code = county["code"]
        entry = {"name": canonical, "code": code}

        # canonical name itself
        lookup[canonical.lower().strip()] = entry
        # each alias
        for alias in county.get("aliases", []):
            lookup[alias.lower().strip()] = entry

    return lookup


_COUNTY_LOOKUP: dict[str, dict[str, str]] | None = None


def county_lookup() -> dict[str, dict[str, str]]:
    """Cached singleton for the county lookup table."""
    global _COUNTY_LOOKUP
    if _COUNTY_LOOKUP is None:
        _COUNTY_LOOKUP = load_county_lookup()
    return _COUNTY_LOOKUP


def normalise_county_name(raw: str) -> str:
    """
    Map any known variant of a county name to its canonical form.
    Returns the original string (title-cased) if not found.
    """
    if pd.isna(raw):
        return raw
    key = str(raw).lower().strip()
    match = county_lookup().get(key)
    if match:
        return match["name"]
    # Try removing hyphens/extra spaces
    key_simplified = re.sub(r"[\s\-]+", " ", key).strip()
    for lk, entry in county_lookup().items():
        if re.sub(r"[\s\-]+", " ", lk) == key_simplified:
            return entry["name"]
    logger.warning("Unrecognised county name: '%s'", raw)
    return str(raw).strip().title()


def county_code_for(name: str) -> str | None:
    """Return the 3-digit county code for a canonical or alias name."""
    if pd.isna(name):
        return None
    key = str(name).lower().strip()
    match = county_lookup().get(key)
    return match["code"] if match else None


# ── Fiscal Year Normalisation ────────────────────────────────────

FY_FULL = re.compile(r"(\d{4})\s*/\s*(\d{2,4})")
FY_SINGLE = re.compile(r"^(\d{4})$")


def normalise_fiscal_year(raw: Any) -> str | None:
    """
    Convert fiscal year strings to a canonical "YYYY/YY" format.
      '2013/14'  → '2013/14'
      '2013/2014' → '2013/14'
      '2013'     → '2012/13'  (calendar year → approx FY starting prior July)
    Returns None for unparseable values.
    """
    if pd.isna(raw):
        return None
    s = str(raw).strip()

    m = FY_FULL.match(s)
    if m:
        start_year = int(m.group(1))
        end_part = m.group(2)
        if len(end_part) == 4:
            end_part = end_part[2:]
        return f"{start_year}/{end_part}"

    m = FY_SINGLE.match(s)
    if m:
        cal_year = int(m.group(1))
        # A calendar year like 2014 maps to FY 2013/14
        start = cal_year - 1
        end = str(cal_year)[2:]
        return f"{start}/{end}"

    logger.warning("Cannot parse fiscal year: '%s'", raw)
    return s


# ── Generic Cleaning Helpers ─────────────────────────────────────

def strip_strings(df: pd.DataFrame) -> pd.DataFrame:
    """Strip leading/trailing whitespace from all string columns."""
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].str.strip()
    return df


def drop_exact_dupes(df: pd.DataFrame) -> pd.DataFrame:
    """Drop fully duplicated rows, log how many removed."""
    before = len(df)
    df = df.drop_duplicates()
    removed = before - len(df)
    if removed:
        logger.info("  Dropped %d exact duplicate rows", removed)
    return df


def coerce_numeric(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Try to cast columns to float, coercing errors to NaN."""
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


# ── Per-dataset Cleaners ─────────────────────────────────────────

def clean_revenue(df: pd.DataFrame) -> pd.DataFrame:
    df = strip_strings(df)
    df["fiscal_year"] = df["fiscal_year"].apply(normalise_fiscal_year)
    df = coerce_numeric(df, ["revenue_target_bn", "revenue_actual_bn"])
    return drop_exact_dupes(df)


def clean_wb_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = strip_strings(df)
    df = coerce_numeric(df, ["year", "value"])
    if "year" in df.columns:
        df["year"] = df["year"].astype("Int64")  # nullable int
    return drop_exact_dupes(df)


def clean_budget(df: pd.DataFrame) -> pd.DataFrame:
    df = strip_strings(df)
    df["fiscal_year"] = df["fiscal_year"].apply(normalise_fiscal_year)
    if "sector" in df.columns:
        df["sector"] = df["sector"].str.lower()
    df = coerce_numeric(df, ["allocation_bn", "expenditure_actual_bn"])
    return drop_exact_dupes(df)


def clean_debt(df: pd.DataFrame) -> pd.DataFrame:
    df = strip_strings(df)
    df["fiscal_year"] = df["fiscal_year"].apply(normalise_fiscal_year)
    df = coerce_numeric(df, ["debt_stock_bn", "debt_service_bn", "debt_to_gdp_pct"])
    return drop_exact_dupes(df)


def clean_equitable_share(df: pd.DataFrame) -> pd.DataFrame:
    df = strip_strings(df)
    df["fiscal_year"] = df["fiscal_year"].apply(normalise_fiscal_year)
    if "county_name" in df.columns:
        df["county_name"] = df["county_name"].apply(normalise_county_name)
    if "county_code" in df.columns:
        # Re-derive codes from normalised names to ensure consistency
        df["county_code"] = df["county_name"].apply(county_code_for)
    df = coerce_numeric(df, ["allocation_mn"])
    return drop_exact_dupes(df)


def clean_county_expenditure(df: pd.DataFrame) -> pd.DataFrame:
    df = strip_strings(df)
    df["fiscal_year"] = df["fiscal_year"].apply(normalise_fiscal_year)
    if "county_name" in df.columns:
        df["county_name"] = df["county_name"].apply(normalise_county_name)
    if "county_code" in df.columns:
        df["county_code"] = df["county_name"].apply(county_code_for)
    df = coerce_numeric(df, ["allocation_mn", "expenditure_mn", "absorption_rate_pct"])
    return drop_exact_dupes(df)


def clean_county_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = strip_strings(df)
    df["fiscal_year"] = df["fiscal_year"].apply(normalise_fiscal_year)
    if "county_name" in df.columns:
        df["county_name"] = df["county_name"].apply(normalise_county_name)
    if "county_code" in df.columns:
        df["county_code"] = df["county_name"].apply(county_code_for)
    df = coerce_numeric(df, ["value"])
    return drop_exact_dupes(df)


# ── Registry ─────────────────────────────────────────────────────

CLEANERS: dict[str, tuple[str, callable]] = {
    "revenue": (
        "kra_revenue/revenue_annual.csv",
        clean_revenue,
    ),
    "wb_indicators": (
        "world_bank_api/wb_indicators.csv",
        clean_wb_indicators,
    ),
    "budget": (
        "treasury_budget/budget_allocations.csv",
        clean_budget,
    ),
    "debt": (
        "debt_cbk/public_debt.csv",
        clean_debt,
    ),
    "equitable_share": (
        "county_allocations/equitable_share.csv",
        clean_equitable_share,
    ),
    "county_expenditure": (
        "cob_expenditure/county_expenditure.csv",
        clean_county_expenditure,
    ),
    "county_indicators": (
        "knbs_indicators/county_indicators.csv",
        clean_county_indicators,
    ),
}


def clean_dataset(name: str) -> pd.DataFrame | None:
    """
    Read a raw CSV, clean it, save to data/clean/, return the DataFrame.
    """
    if name not in CLEANERS:
        logger.error("Unknown dataset: %s", name)
        return None

    rel_path, cleaner_fn = CLEANERS[name]
    raw_path = DATA_RAW / rel_path
    if not raw_path.exists():
        logger.warning("Raw file not found: %s", raw_path)
        return None

    logger.info("Cleaning %s ...", name)
    df = pd.read_csv(raw_path)
    df = cleaner_fn(df)

    # Write
    out_dir = DATA_CLEAN / name
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / raw_path.name
    df.to_csv(out_path, index=False)
    logger.info("  → %s  (%d rows, %d cols)", out_path.relative_to(ROOT_DIR), len(df), len(df.columns))
    return df


def clean_all() -> dict[str, pd.DataFrame]:
    """Clean all known datasets. Returns {name: clean_df}."""
    results = {}
    for name in CLEANERS:
        df = clean_dataset(name)
        if df is not None:
            results[name] = df
    return results


# ── CLI ──────────────────────────────────────────────────────────

def main():
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    )

    args = sys.argv[1:]
    if args:
        for name in args:
            clean_dataset(name)
    else:
        clean_all()


if __name__ == "__main__":
    main()
