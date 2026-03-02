"""
Data Merge — Join national + county datasets into analysis-ready tables.

Produces three merged tables in data/clean/:
1.  national_panel.csv   — fiscal-year-level national indicators
2.  county_panel.csv     — county × fiscal-year panel
3.  county_wide.csv      — county × fiscal-year with indicators pivoted wide

Usage:
    python -m pipeline.merge
    # or: make merge
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger("kenya_counts.pipeline.merge")

ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_CLEAN = ROOT_DIR / "data" / "clean"


# ── Helpers ──────────────────────────────────────────────────────

def _read_clean(subdir: str, filename: str) -> pd.DataFrame | None:
    """Read a clean CSV. Returns None if file doesn't exist."""
    path = DATA_CLEAN / subdir / filename
    if not path.exists():
        logger.warning("Clean file missing: %s", path.relative_to(ROOT_DIR))
        return None
    return pd.read_csv(path)


def _fy_sort_key(fy: str) -> int:
    """
    Sort key for fiscal years like '2013/14'.
    Returns the 4-digit start year as int.
    """
    try:
        return int(str(fy).split("/")[0])
    except (ValueError, IndexError):
        return 0


# ── National Panel ───────────────────────────────────────────────

def build_national_panel() -> pd.DataFrame:
    """
    Combine revenue, budget, debt, and World Bank national indicators
    into a single fiscal-year-level table.
    """
    frames: list[pd.DataFrame] = []

    # Revenue
    rev = _read_clean("revenue", "revenue_annual.csv")
    if rev is not None:
        frames.append(rev[["fiscal_year", "revenue_target_bn", "revenue_actual_bn"]])

    # Budget
    bud = _read_clean("budget", "budget_allocations.csv")
    if bud is not None:
        # Pivot sectors into columns
        bud_wide = bud.pivot_table(
            index="fiscal_year",
            columns="sector",
            values=["allocation_bn", "expenditure_actual_bn"],
            aggfunc="first",
        )
        # Flatten multi-index columns
        bud_wide.columns = [f"{metric}_{sector}" for metric, sector in bud_wide.columns]
        bud_wide = bud_wide.reset_index()
        frames.append(bud_wide)

    # Debt
    debt = _read_clean("debt", "public_debt.csv")
    if debt is not None:
        frames.append(debt[["fiscal_year"] + [
            c for c in debt.columns
            if c != "fiscal_year" and c != "source_url"
        ]])

    # Merge all national frames on fiscal_year
    if not frames:
        logger.error("No national data available to merge")
        return pd.DataFrame()

    merged = frames[0]
    for f in frames[1:]:
        merged = merged.merge(f, on="fiscal_year", how="outer")

    # World Bank indicators (year-level, not FY-level — add separately)
    wb = _read_clean("wb_indicators", "wb_indicators.csv")
    if wb is not None:
        wb_wide = wb.pivot_table(
            index="year",
            columns="indicator_id",
            values="value",
            aggfunc="first",
        )
        wb_wide.columns = [f"wb_{c}" for c in wb_wide.columns]
        wb_wide = wb_wide.reset_index().rename(columns={"year": "wb_year"})
        # Map calendar year → fiscal year for the merge
        # FY 2013/14 covers Jul 2013 – Jun 2014.  Map WB year=2014 → FY 2013/14
        wb_wide["fiscal_year"] = wb_wide["wb_year"].apply(
            lambda y: f"{y - 1}/{str(y)[2:]}" if pd.notna(y) else None
        )
        wb_wide = wb_wide.drop(columns=["wb_year"])
        merged = merged.merge(wb_wide, on="fiscal_year", how="outer")

    # Sort by fiscal year
    merged = merged.sort_values(
        "fiscal_year", key=lambda s: s.map(_fy_sort_key)
    ).reset_index(drop=True)

    return merged


# ── County Panel ─────────────────────────────────────────────────

def build_county_panel() -> pd.DataFrame:
    """
    Combine equitable share allocations, county expenditure, and
    county-level indicators into a county × fiscal-year panel.
    """
    key_cols = ["fiscal_year", "county_code", "county_name"]

    # Allocations
    alloc = _read_clean("equitable_share", "equitable_share.csv")

    # Expenditure
    exp = _read_clean("county_expenditure", "county_expenditure.csv")

    # County indicators (long format → pivot wide)
    ind = _read_clean("county_indicators", "county_indicators.csv")

    # Start from whichever data we have
    if alloc is not None:
        panel = alloc[[c for c in alloc.columns if c != "source_url"]].copy()
    elif exp is not None:
        panel = exp.copy()
    else:
        panel = pd.DataFrame(columns=key_cols)

    # Merge expenditure
    if exp is not None and alloc is not None:
        exp_cols = [c for c in exp.columns if c not in ("allocation_mn", "source_url")]
        panel = panel.merge(
            exp[exp_cols],
            on=key_cols,
            how="outer",
        )
    elif exp is not None:
        panel = exp.copy()

    # Merge indicators (pivot wide first)
    if ind is not None and len(ind) > 0:
        ind_wide = ind.pivot_table(
            index=key_cols,
            columns="indicator_id",
            values="value",
            aggfunc="first",
        )
        ind_wide.columns = [f"ind_{c}" for c in ind_wide.columns]
        ind_wide = ind_wide.reset_index()
        panel = panel.merge(ind_wide, on=key_cols, how="outer")

    # Sort
    panel = panel.sort_values(
        ["fiscal_year", "county_code"],
        key=lambda s: s.map(_fy_sort_key) if s.name == "fiscal_year" else s,
    ).reset_index(drop=True)

    return panel


# ── Save ─────────────────────────────────────────────────────────

def save_merged(df: pd.DataFrame, name: str) -> Path:
    """Write a merged DataFrame to data/clean/{name}.csv."""
    DATA_CLEAN.mkdir(parents=True, exist_ok=True)
    path = DATA_CLEAN / f"{name}.csv"
    df.to_csv(path, index=False)
    logger.info("Saved %s — %d rows, %d cols", path.relative_to(ROOT_DIR), len(df), len(df.columns))
    return path


def merge_all() -> dict[str, pd.DataFrame]:
    """Build and save all merged datasets."""
    results = {}

    national = build_national_panel()
    if len(national):
        save_merged(national, "national_panel")
        results["national_panel"] = national

    county = build_county_panel()
    if len(county):
        save_merged(county, "county_panel")
        results["county_panel"] = county

    return results


# ── CLI ──────────────────────────────────────────────────────────

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    )
    results = merge_all()
    for name, df in results.items():
        print(f"{name}: {df.shape[0]} rows × {df.shape[1]} columns")


if __name__ == "__main__":
    main()
