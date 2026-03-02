"""
Data Validation — Schema checks for all raw datasets.

Uses pandera to define expected schemas for each CSV produced by scrapers.
Runs validation on all files in data/raw/ and produces a quality report.

Usage:
    python -m pipeline.validate
    # or: make validate
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pandera as pa
from pandera import Column, Check, DataFrameSchema

logger = logging.getLogger("kenya_counts.pipeline.validate")

ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_RAW = ROOT_DIR / "data" / "raw"
DATA_CLEAN = ROOT_DIR / "data" / "clean"


# ── Schema Definitions ──────────────────────────────────────────

FY_PATTERN = r"^\d{4}/\d{2}$"  # e.g. "2013/14"

revenue_schema = DataFrameSchema(
    {
        "fiscal_year": Column(str, Check.str_matches(FY_PATTERN)),
        "revenue_target_bn": Column(float, Check.greater_than(0), nullable=True),
        "revenue_actual_bn": Column(float, Check.greater_than(0), nullable=True),
    },
    name="kra_revenue",
    strict=False,  # allow extra columns (e.g. source_url)
)

wb_indicators_schema = DataFrameSchema(
    {
        "indicator_id": Column(str, Check.str_length(min_value=3)),
        "indicator_name": Column(str),
        "wb_code": Column(str, Check.str_matches(r"^[A-Z]{2}\.")),
        "year": Column(int, Check.in_range(2000, 2030)),
        "value": Column(float, nullable=True),
    },
    name="world_bank_api",
    strict=False,
)

budget_schema = DataFrameSchema(
    {
        "fiscal_year": Column(str, Check.str_matches(FY_PATTERN)),
        "sector": Column(str, Check.isin(["total", "education", "healthcare", "infrastructure"])),
        "allocation_bn": Column(float, Check.greater_than(0), nullable=True),
    },
    name="treasury_budget",
    strict=False,
)

debt_schema = DataFrameSchema(
    {
        "fiscal_year": Column(str, Check.str_matches(FY_PATTERN)),
        "debt_stock_bn": Column(float, Check.greater_than(0), nullable=True),
        "debt_service_bn": Column(float, Check.greater_than(0), nullable=True),
    },
    name="debt_cbk",
    strict=False,
)

equitable_share_schema = DataFrameSchema(
    {
        "fiscal_year": Column(str, Check.str_matches(FY_PATTERN)),
        "county_code": Column(str),
        "county_name": Column(str, Check.str_length(min_value=2)),
        "allocation_mn": Column(float, Check.greater_than(0), nullable=True),
    },
    name="county_allocations",
    strict=False,
)

county_expenditure_schema = DataFrameSchema(
    {
        "fiscal_year": Column(str, Check.str_matches(FY_PATTERN)),
        "county_code": Column(str),
        "county_name": Column(str, Check.str_length(min_value=2)),
        "allocation_mn": Column(float, Check.greater_than(0), nullable=True),
        "expenditure_mn": Column(float, Check.greater_than_or_equal_to(0), nullable=True),
        "absorption_rate_pct": Column(float, Check.in_range(0, 200), nullable=True),
    },
    name="cob_expenditure",
    strict=False,
)

county_indicators_schema = DataFrameSchema(
    {
        "fiscal_year": Column(str),
        "county_code": Column(str),
        "county_name": Column(str, Check.str_length(min_value=2)),
        "indicator_id": Column(str, Check.str_length(min_value=3)),
        "value": Column(float, nullable=True),
        "source": Column(str),
    },
    name="knbs_indicators",
    strict=False,
)


# ── File → Schema mapping ───────────────────────────────────────

SCHEMAS: dict[str, tuple[str, DataFrameSchema]] = {
    # (scraper_dir/filename) → (label, schema)
    "kra_revenue/revenue_annual.csv": ("KRA Revenue", revenue_schema),
    "world_bank_api/wb_indicators.csv": ("World Bank Indicators", wb_indicators_schema),
    "treasury_budget/budget_allocations.csv": ("Treasury Budget", budget_schema),
    "debt_cbk/public_debt.csv": ("CBK Debt", debt_schema),
    "county_allocations/equitable_share.csv": ("County Allocations", equitable_share_schema),
    "cob_expenditure/county_expenditure.csv": ("CoB Expenditure", county_expenditure_schema),
    "knbs_indicators/county_indicators.csv": ("KNBS Indicators", county_indicators_schema),
}


# ── Validation Logic ────────────────────────────────────────────

def validate_file(
    filepath: Path, schema: DataFrameSchema
) -> dict:
    """
    Validate a single CSV file against its schema.
    Returns a report dict with status, row/column counts, and any errors.
    """
    report = {
        "file": str(filepath.relative_to(ROOT_DIR)),
        "exists": filepath.exists(),
        "rows": 0,
        "columns": 0,
        "missing_pct": {},
        "schema_valid": False,
        "errors": [],
    }

    if not filepath.exists():
        report["errors"].append("File not found")
        return report

    try:
        df = pd.read_csv(filepath)
    except Exception as e:
        report["errors"].append(f"Failed to read CSV: {e}")
        return report

    report["rows"] = len(df)
    report["columns"] = len(df.columns)

    # Missing data percentage per column
    for col in df.columns:
        pct = df[col].isna().sum() / len(df) * 100 if len(df) > 0 else 0
        report["missing_pct"][col] = round(pct, 1)

    # Schema validation
    try:
        schema.validate(df, lazy=True)
        report["schema_valid"] = True
    except pa.errors.SchemaErrors as e:
        report["schema_valid"] = False
        for _, row in e.failure_cases.iterrows():
            report["errors"].append(
                f"Column '{row.get('column', '?')}': {row.get('check', '?')} "
                f"(index {row.get('index', '?')})"
            )
        # Cap error list to prevent huge reports
        if len(report["errors"]) > 20:
            report["errors"] = report["errors"][:20] + [
                f"... and {len(report['errors']) - 20} more errors"
            ]

    return report


def validate_all() -> list[dict]:
    """Validate all known raw data files. Returns list of report dicts."""
    reports = []
    for rel_path, (label, schema) in SCHEMAS.items():
        filepath = DATA_RAW / rel_path
        logger.info("Validating: %s (%s)", label, rel_path)
        report = validate_file(filepath, schema)
        report["label"] = label
        reports.append(report)

        status = "PASS" if report["schema_valid"] else "FAIL"
        if not report["exists"]:
            status = "MISSING"
        logger.info(
            "  %s — %d rows, %d cols, %s",
            status,
            report["rows"],
            report["columns"],
            f"{len(report['errors'])} errors" if report["errors"] else "no errors",
        )

    return reports


def print_quality_report(reports: list[dict]) -> None:
    """Print a human-readable quality report to stdout."""
    print("\n" + "=" * 70)
    print("DATA QUALITY REPORT")
    print("=" * 70)

    passed = sum(1 for r in reports if r["schema_valid"])
    total = len(reports)
    missing = sum(1 for r in reports if not r["exists"])

    print(f"\nFiles checked: {total}")
    print(f"  Passed:  {passed}")
    print(f"  Failed:  {total - passed - missing}")
    print(f"  Missing: {missing}")

    for r in reports:
        status = "PASS" if r["schema_valid"] else "FAIL"
        if not r["exists"]:
            status = "MISSING"
        print(f"\n{'─' * 50}")
        print(f"  {r.get('label', r['file'])} [{status}]")
        print(f"  File: {r['file']}")
        print(f"  Rows: {r['rows']}  Columns: {r['columns']}")

        if r["missing_pct"]:
            high_missing = {
                k: v for k, v in r["missing_pct"].items() if v > 0
            }
            if high_missing:
                print(f"  Missing data: {high_missing}")

        if r["errors"]:
            print(f"  Errors ({len(r['errors'])}):")
            for err in r["errors"][:5]:
                print(f"    - {err}")
            if len(r["errors"]) > 5:
                print(f"    ... and {len(r['errors']) - 5} more")

    print(f"\n{'=' * 70}\n")


def save_quality_report(reports: list[dict]) -> Path:
    """Save the quality report as JSON in data/clean/."""
    DATA_CLEAN.mkdir(parents=True, exist_ok=True)
    path = DATA_CLEAN / "data_quality_report.json"
    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "summary": {
            "total": len(reports),
            "passed": sum(1 for r in reports if r["schema_valid"]),
            "failed": sum(1 for r in reports if r["exists"] and not r["schema_valid"]),
            "missing": sum(1 for r in reports if not r["exists"]),
        },
        "datasets": reports,
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    logger.info("Quality report saved → %s", path)
    return path


# ── CLI ──────────────────────────────────────────────────────────

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    )
    reports = validate_all()
    print_quality_report(reports)
    save_quality_report(reports)

    # Exit with code 1 if any validation failed (useful for CI)
    if any(r["exists"] and not r["schema_valid"] for r in reports):
        sys.exit(1)


if __name__ == "__main__":
    main()
