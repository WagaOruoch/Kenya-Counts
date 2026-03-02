"""
Tests for pipeline/merge.py and pipeline/indicators.py.

Uses small synthetic DataFrames (not real files) to verify
merge logic, indicator calculations, and composite scoring.
"""

import numpy as np
import pandas as pd
import pytest

from pipeline.merge import (
    _fy_sort_key,
    build_national_panel,
    build_county_panel,
)
from pipeline.indicators import (
    pct_change_by_group,
    per_capita,
    min_max_normalise,
    compute_composite_score,
    enrich_national,
    enrich_county,
)


# ── Helpers ──────────────────────────────────────────────────────


class TestFYSortKey:

    def test_standard(self):
        assert _fy_sort_key("2013/14") == 2013

    def test_bad_value(self):
        assert _fy_sort_key("not-a-year") == 0


# ── Indicator Helpers ────────────────────────────────────────────


class TestPctChange:

    def test_simple_growth(self):
        df = pd.DataFrame({
            "fiscal_year": ["2013/14", "2014/15", "2015/16"],
            "value": [100.0, 120.0, 150.0],
        })
        result = pct_change_by_group(df, "value")
        assert "value_growth_pct" in result.columns
        assert pd.isna(result["value_growth_pct"].iloc[0])
        assert abs(result["value_growth_pct"].iloc[1] - 20.0) < 0.01
        assert abs(result["value_growth_pct"].iloc[2] - 25.0) < 0.01

    def test_grouped_growth(self):
        df = pd.DataFrame({
            "fiscal_year": ["2013/14", "2014/15", "2013/14", "2014/15"],
            "county": ["A", "A", "B", "B"],
            "value": [100.0, 150.0, 200.0, 210.0],
        })
        result = pct_change_by_group(df, "value", group_cols=["county"])
        a_growth = result[result["county"] == "A"]["value_growth_pct"].iloc[1]
        b_growth = result[result["county"] == "B"]["value_growth_pct"].iloc[1]
        assert abs(a_growth - 50.0) < 0.01
        assert abs(b_growth - 5.0) < 0.01


class TestPerCapita:

    def test_basic(self):
        df = pd.DataFrame({
            "revenue": [1000.0],
            "wb_population_total": [50.0],
        })
        result = per_capita(df, "revenue")
        assert abs(result["revenue_per_capita"].iloc[0] - 20.0) < 0.01

    def test_with_multiplier(self):
        df = pd.DataFrame({
            "revenue_bn": [2.0],
            "wb_population_total": [1e9],
        })
        result = per_capita(df, "revenue_bn", multiplier=1e9)
        assert abs(result["revenue_bn_per_capita"].iloc[0] - 2.0) < 0.01

    def test_missing_population_col(self):
        df = pd.DataFrame({"revenue": [100.0]})
        result = per_capita(df, "revenue")
        assert pd.isna(result["revenue_per_capita"].iloc[0])


class TestMinMaxNormalise:

    def test_higher_is_better(self):
        s = pd.Series([0.0, 50.0, 100.0])
        result = min_max_normalise(s, "higher_is_better")
        assert abs(result.iloc[0] - 0.0) < 0.01
        assert abs(result.iloc[2] - 100.0) < 0.01

    def test_lower_is_better(self):
        s = pd.Series([0.0, 50.0, 100.0])
        result = min_max_normalise(s, "lower_is_better")
        assert abs(result.iloc[0] - 100.0) < 0.01
        assert abs(result.iloc[2] - 0.0) < 0.01

    def test_constant_series(self):
        s = pd.Series([42.0, 42.0, 42.0])
        result = min_max_normalise(s, "higher_is_better")
        # All equal → default to 50
        assert (result == 50.0).all()


class TestCompositeScore:

    def test_score_computed(self):
        df = pd.DataFrame({
            "fiscal_year": ["2013/14", "2013/14"],
            "county_code": ["001", "002"],
            "county_name": ["Mombasa", "Kwale"],
            "ind_skilled_birth_attendance": [80.0, 40.0],
            "ind_primary_completion": [90.0, 60.0],
            "ind_pupil_teacher_ratio": [30.0, 60.0],
            "ind_poverty_headcount": [20.0, 50.0],
        })
        result = compute_composite_score(df)
        assert "service_delivery_score" in result.columns
        assert len(result) == 2
        # Mombasa should score higher (better on all indicators)
        assert result.iloc[0]["service_delivery_score"] > result.iloc[1]["service_delivery_score"]

    def test_no_components_returns_empty(self):
        df = pd.DataFrame({
            "fiscal_year": ["2013/14"],
            "county_code": ["001"],
            "county_name": ["Mombasa"],
        })
        result = compute_composite_score(df)
        assert "service_delivery_score" in result.columns
        assert len(result) == 0


# ── National Enrichment ──────────────────────────────────────────


class TestNationalEnrichment:

    def test_revenue_growth_and_gap(self):
        df = pd.DataFrame({
            "fiscal_year": ["2013/14", "2014/15"],
            "revenue_target_bn": [1000.0, 1200.0],
            "revenue_actual_bn": [900.0, 1100.0],
        })
        result = enrich_national(df)
        assert "revenue_actual_bn_growth_pct" in result.columns
        assert "revenue_compliance_gap_pct" in result.columns
        # Gap for 2013/14: (900 - 1000) / 1000 * 100 = -10%
        assert abs(result["revenue_compliance_gap_pct"].iloc[0] - (-10.0)) < 0.01

    def test_debt_to_revenue(self):
        df = pd.DataFrame({
            "fiscal_year": ["2020/21"],
            "debt_stock_bn": [8000.0],
            "revenue_actual_bn": [1600.0],
        })
        result = enrich_national(df)
        assert "debt_to_revenue_ratio" in result.columns
        assert abs(result["debt_to_revenue_ratio"].iloc[0] - 5.0) < 0.01


# ── County Enrichment ────────────────────────────────────────────


class TestCountyEnrichment:

    def test_absorption_rate_derived(self):
        df = pd.DataFrame({
            "fiscal_year": ["2017/18"],
            "county_code": ["001"],
            "county_name": ["Mombasa"],
            "allocation_mn": [5000.0],
            "expenditure_mn": [4000.0],
        })
        result = enrich_county(df)
        assert "absorption_rate_derived" in result.columns
        assert abs(result["absorption_rate_derived"].iloc[0] - 80.0) < 0.01

    def test_allocation_growth(self):
        df = pd.DataFrame({
            "fiscal_year": ["2016/17", "2017/18"],
            "county_code": ["001", "001"],
            "county_name": ["Mombasa", "Mombasa"],
            "allocation_mn": [4000.0, 5000.0],
            "expenditure_mn": [3500.0, 4200.0],
        })
        result = enrich_county(df)
        assert "allocation_mn_growth_pct" in result.columns
        assert abs(result["allocation_mn_growth_pct"].iloc[1] - 25.0) < 0.01


# ── Merge (integration-light) ────────────────────────────────────
# These test the merge functions with mocked clean files via monkeypatch.


class TestMergeNational:

    def test_merges_revenue_and_debt(self, tmp_path, monkeypatch):
        """Patch DATA_CLEAN to a tmp dir with synthetic clean files."""
        import pipeline.merge as merge_mod

        monkeypatch.setattr(merge_mod, "DATA_CLEAN", tmp_path)
        monkeypatch.setattr(merge_mod, "ROOT_DIR", tmp_path.parent)

        # Create revenue
        rev_dir = tmp_path / "revenue"
        rev_dir.mkdir()
        pd.DataFrame({
            "fiscal_year": ["2013/14", "2014/15"],
            "revenue_target_bn": [1000.0, 1200.0],
            "revenue_actual_bn": [900.0, 1100.0],
        }).to_csv(rev_dir / "revenue_annual.csv", index=False)

        # Create debt
        debt_dir = tmp_path / "debt"
        debt_dir.mkdir()
        pd.DataFrame({
            "fiscal_year": ["2013/14", "2014/15"],
            "debt_stock_bn": [2000.0, 2500.0],
            "debt_service_bn": [200.0, 300.0],
        }).to_csv(debt_dir / "public_debt.csv", index=False)

        result = merge_mod.build_national_panel()
        assert len(result) == 2
        assert "revenue_actual_bn" in result.columns
        assert "debt_stock_bn" in result.columns


class TestMergeCounty:

    def test_merges_alloc_and_expenditure(self, tmp_path, monkeypatch):
        import pipeline.merge as merge_mod

        monkeypatch.setattr(merge_mod, "DATA_CLEAN", tmp_path)
        monkeypatch.setattr(merge_mod, "ROOT_DIR", tmp_path.parent)

        key = {"fiscal_year": ["2017/18"], "county_code": ["001"], "county_name": ["Mombasa"]}

        # Allocations
        alloc_dir = tmp_path / "equitable_share"
        alloc_dir.mkdir()
        pd.DataFrame({**key, "allocation_mn": [5000.0]}).to_csv(
            alloc_dir / "equitable_share.csv", index=False
        )

        # Expenditure
        exp_dir = tmp_path / "county_expenditure"
        exp_dir.mkdir()
        pd.DataFrame({
            **key,
            "expenditure_mn": [4200.0],
            "absorption_rate_pct": [84.0],
        }).to_csv(exp_dir / "county_expenditure.csv", index=False)

        result = merge_mod.build_county_panel()
        assert len(result) == 1
        assert "allocation_mn" in result.columns
        assert "expenditure_mn" in result.columns
