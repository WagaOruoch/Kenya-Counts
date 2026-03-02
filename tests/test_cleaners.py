"""
Tests for pipeline/clean.py — county normalisation, FY parsing, per-dataset cleaners.
"""

import pandas as pd
import pytest

from pipeline.clean import (
    normalise_county_name,
    normalise_fiscal_year,
    county_code_for,
    strip_strings,
    drop_exact_dupes,
    coerce_numeric,
    clean_revenue,
    clean_wb_indicators,
    clean_budget,
    clean_debt,
    clean_equitable_share,
    clean_county_expenditure,
    clean_county_indicators,
)


# ── County Name Normalisation ───────────────────────────────────


class TestCountyNormalisation:

    def test_canonical_unchanged(self):
        assert normalise_county_name("Nairobi") == "Nairobi"

    def test_alias_resolved(self):
        assert normalise_county_name("Nairobi City") == "Nairobi"

    def test_hyphen_variant(self):
        assert normalise_county_name("Taita Taveta") == "Taita-Taveta"

    def test_case_insensitive(self):
        assert normalise_county_name("MOMBASA") == "Mombasa"
        assert normalise_county_name("nairobi county") == "Nairobi"

    def test_whitespace_stripped(self):
        assert normalise_county_name("  Meru  ") == "Meru"

    def test_muranga_apostrophe(self):
        assert normalise_county_name("Muranga") == "Murang'a"

    def test_unknown_returns_titlecased(self):
        result = normalise_county_name("nonexistent county")
        assert result == "Nonexistent County"

    def test_nan_passthrough(self):
        assert pd.isna(normalise_county_name(float("nan")))


class TestCountyCode:

    def test_known_county(self):
        assert county_code_for("Nairobi") == "047"
        assert county_code_for("Mombasa") == "001"

    def test_alias_county(self):
        assert county_code_for("Nairobi City") == "047"

    def test_unknown_returns_none(self):
        assert county_code_for("Atlantis") is None

    def test_nan_returns_none(self):
        assert county_code_for(float("nan")) is None


# ── Fiscal Year Normalisation ────────────────────────────────────


class TestFiscalYearNormalisation:

    def test_standard_fy(self):
        assert normalise_fiscal_year("2013/14") == "2013/14"

    def test_full_year_fy(self):
        assert normalise_fiscal_year("2013/2014") == "2013/14"

    def test_calendar_year(self):
        # Calendar 2014 → FY 2013/14
        assert normalise_fiscal_year("2014") == "2013/14"

    def test_with_spaces(self):
        assert normalise_fiscal_year("2019 / 20") == "2019/20"

    def test_nan_returns_none(self):
        assert normalise_fiscal_year(float("nan")) is None

    def test_integer_input(self):
        assert normalise_fiscal_year(2020) == "2019/20"


# ── Generic Helpers ──────────────────────────────────────────────


class TestHelpers:

    def test_strip_strings(self):
        df = pd.DataFrame({"a": ["  hello  ", "  world"], "b": [1, 2]})
        result = strip_strings(df)
        assert result["a"].tolist() == ["hello", "world"]
        assert result["b"].tolist() == [1, 2]

    def test_drop_exact_dupes(self):
        df = pd.DataFrame({"a": [1, 1, 2], "b": [3, 3, 4]})
        result = drop_exact_dupes(df)
        assert len(result) == 2

    def test_coerce_numeric(self):
        df = pd.DataFrame({"x": ["1.5", "abc", "3.0"], "y": ["a", "b", "c"]})
        result = coerce_numeric(df, ["x"])
        assert result["x"].tolist()[0] == 1.5
        assert pd.isna(result["x"].tolist()[1])
        assert result["x"].tolist()[2] == 3.0


# ── Per-dataset Cleaners ─────────────────────────────────────────


class TestRevenueCleaner:

    def test_basic_clean(self):
        df = pd.DataFrame({
            "fiscal_year": ["2013/14", "2014/15"],
            "revenue_target_bn": ["1200.5", "1350.0"],
            "revenue_actual_bn": ["1100.0", "1300.5"],
        })
        result = clean_revenue(df)
        assert result["fiscal_year"].tolist() == ["2013/14", "2014/15"]
        assert result["revenue_actual_bn"].dtype == float

    def test_deduplication(self):
        df = pd.DataFrame({
            "fiscal_year": ["2013/14", "2013/14"],
            "revenue_target_bn": [1200.5, 1200.5],
            "revenue_actual_bn": [1100.0, 1100.0],
        })
        result = clean_revenue(df)
        assert len(result) == 1


class TestBudgetCleaner:

    def test_sector_lowercased(self):
        df = pd.DataFrame({
            "fiscal_year": ["2013/14"],
            "sector": ["Healthcare"],
            "allocation_bn": [100.0],
            "expenditure_actual_bn": [90.0],
        })
        result = clean_budget(df)
        assert result["sector"].iloc[0] == "healthcare"


class TestEquitableShareCleaner:

    def test_county_name_normalised(self):
        df = pd.DataFrame({
            "fiscal_year": ["2013/14"],
            "county_code": ["047"],
            "county_name": ["Nairobi City"],
            "allocation_mn": [15000.0],
        })
        result = clean_equitable_share(df)
        assert result["county_name"].iloc[0] == "Nairobi"
        assert result["county_code"].iloc[0] == "047"


class TestCountyExpenditureCleaner:

    def test_county_normalised_and_numeric(self):
        df = pd.DataFrame({
            "fiscal_year": ["2017/18"],
            "county_code": ["001"],
            "county_name": ["MOMBASA"],
            "allocation_mn": ["5000"],
            "expenditure_mn": ["4200"],
            "absorption_rate_pct": ["84.0"],
        })
        result = clean_county_expenditure(df)
        assert result["county_name"].iloc[0] == "Mombasa"
        assert result["absorption_rate_pct"].dtype == float


class TestCountyIndicatorsCleaner:

    def test_county_and_value_normalised(self):
        df = pd.DataFrame({
            "fiscal_year": ["2014"],
            "county_code": ["047"],
            "county_name": ["Nairobi County"],
            "indicator_id": ["county_poverty_headcount"],
            "value": ["21.8"],
            "source": ["KIHBS 2015/16"],
        })
        result = clean_county_indicators(df)
        assert result["county_name"].iloc[0] == "Nairobi"
        # Calendar 2014 → FY 2013/14
        assert result["fiscal_year"].iloc[0] == "2013/14"
        assert result["value"].dtype == float
