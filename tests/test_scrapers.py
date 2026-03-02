"""
Tests for Kenya Counts scrapers.

Tests cover:
1. BaseScraper utilities (caching, manifest, CSV output)
2. Individual scraper logic (data parsing, merging)
3. PDF extractor utilities
4. World Bank API response parsing

Uses pytest-httpx to mock HTTP responses without hitting live servers.
"""

from __future__ import annotations

import csv
import json
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest


# ── Test helpers ─────────────────────────────────────────────────

@pytest.fixture
def tmp_output(tmp_path):
    """Create a temporary directory structure mimicking data/raw/."""
    raw = tmp_path / "data" / "raw"
    raw.mkdir(parents=True)
    (raw / "manifest.json").write_text(
        json.dumps({"description": "test", "last_updated": None, "files": []})
    )
    return tmp_path


# ── BaseScraper Tests ────────────────────────────────────────────

class TestBaseScraper:
    """Test BaseScraper utilities without making HTTP requests."""

    def test_save_csv_creates_file(self, tmp_path):
        """save_csv should write a valid CSV file."""
        from scraper.base import BaseScraper

        with patch.object(BaseScraper, "__init__", lambda self: None):
            scraper = BaseScraper()
            scraper.output_dir = tmp_path
            scraper.name = "test"

            data = [
                {"year": "2020", "value": "100"},
                {"year": "2021", "value": "200"},
            ]
            path = scraper.save_csv(data, "test.csv")

            assert path.exists()
            with open(path, encoding="utf-8") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
            assert len(rows) == 2
            assert rows[0]["year"] == "2020"
            assert rows[1]["value"] == "200"

    def test_save_csv_empty_data(self, tmp_path):
        """save_csv should handle empty data gracefully."""
        from scraper.base import BaseScraper

        with patch.object(BaseScraper, "__init__", lambda self: None):
            scraper = BaseScraper()
            scraper.output_dir = tmp_path
            scraper.name = "test"

            path = scraper.save_csv([], "empty.csv")
            assert path == tmp_path / "empty.csv"

    def test_cache_roundtrip(self, tmp_path):
        """Caching should store and retrieve content."""
        from scraper.base import BaseScraper

        with patch.object(BaseScraper, "__init__", lambda self: None):
            scraper = BaseScraper()
            scraper._cache_dir = tmp_path

            url = "https://example.com/test.pdf"
            content = b"fake pdf content"

            assert scraper.get_cached(url) is None
            scraper.set_cached(url, content)
            assert scraper.get_cached(url) == content

    def test_manifest_update(self, tmp_path):
        """update_manifest should add entries to manifest.json."""
        from scraper.base import BaseScraper, MANIFEST_PATH

        manifest_path = tmp_path / "manifest.json"
        manifest_path.write_text(
            json.dumps({"description": "test", "last_updated": None, "files": []})
        )

        with patch.object(BaseScraper, "__init__", lambda self: None):
            with patch("scraper.base.MANIFEST_PATH", manifest_path):
                scraper = BaseScraper()
                scraper.name = "test_scraper"

                scraper.update_manifest([
                    {"filename": "data.csv", "url": "https://example.com", "description": "Test data"}
                ])

                manifest = json.loads(manifest_path.read_text())
                assert len(manifest["files"]) == 1
                assert manifest["files"][0]["scraper"] == "test_scraper"
                assert manifest["files"][0]["filename"] == "data.csv"
                assert manifest["last_updated"] is not None

    def test_manifest_deduplication(self, tmp_path):
        """Updating manifest with same scraper+filename should replace, not duplicate."""
        from scraper.base import BaseScraper

        manifest_path = tmp_path / "manifest.json"
        manifest_path.write_text(
            json.dumps({"description": "test", "last_updated": None, "files": []})
        )

        with patch.object(BaseScraper, "__init__", lambda self: None):
            with patch("scraper.base.MANIFEST_PATH", manifest_path):
                scraper = BaseScraper()
                scraper.name = "test"

                scraper.update_manifest([{"filename": "a.csv", "url": "v1", "description": ""}])
                scraper.update_manifest([{"filename": "a.csv", "url": "v2", "description": ""}])

                manifest = json.loads(manifest_path.read_text())
                assert len(manifest["files"]) == 1
                assert manifest["files"][0]["url"] == "v2"


# ── KRA Revenue Tests ───────────────────────────────────────────

class TestKRARevenue:
    """Test KRA revenue scraper logic."""

    def test_historical_data_complete(self):
        """Historical data should cover 2013/14 through 2023/24."""
        from scraper.sources.kra_revenue import KRARevenueScraper

        data = KRARevenueScraper.HISTORICAL_REVENUE
        fiscal_years = [r["fiscal_year"] for r in data]

        assert "2013/14" in fiscal_years
        assert "2023/24" in fiscal_years
        assert len(data) == 11  # 11 fiscal years

    def test_extract_numbers(self):
        """Number extraction should handle commas and filter small values."""
        from scraper.sources.kra_revenue import KRARevenueScraper

        cells = ["FY 2023/24", "2,495.8", "2,397.8", "5.3"]
        numbers = KRARevenueScraper._extract_numbers(cells)

        assert 2495.8 in numbers
        assert 2397.8 in numbers
        assert 5.3 not in numbers  # filtered out (< 10)

    def test_merge_rows_prefers_new(self):
        """Merge should prefer new data for same fiscal year."""
        from scraper.sources.kra_revenue import KRARevenueScraper

        existing = [{"fiscal_year": "2023/24", "revenue_actual_bn": 100}]
        new = [{"fiscal_year": "2023/24", "revenue_actual_bn": 200}]

        merged = KRARevenueScraper._merge_rows(existing, new)
        assert merged[0]["revenue_actual_bn"] == 200

    def test_merge_rows_adds_new_years(self):
        """Merge should add new fiscal years not in existing data."""
        from scraper.sources.kra_revenue import KRARevenueScraper

        existing = [{"fiscal_year": "2022/23", "revenue_actual_bn": 100}]
        new = [{"fiscal_year": "2024/25", "revenue_actual_bn": 300}]

        merged = KRARevenueScraper._merge_rows(existing, new)
        assert len(merged) == 2


# ── World Bank API Tests ────────────────────────────────────────

class TestWorldBankAPI:
    """Test World Bank API scraper logic."""

    def test_indicators_list_complete(self):
        """All indicators from indicators.yaml should be covered."""
        from scraper.sources.world_bank_api import WorldBankAPIScraper

        expected_ids = [
            "maternal_mortality_rate",
            "immunisation_coverage",
            "primary_enrollment_rate",
            "primary_completion_rate",
            "pupil_teacher_ratio",
            "electrification_rate",
        ]
        for indicator_id in expected_ids:
            assert indicator_id in WorldBankAPIScraper.INDICATORS

    def test_parse_wb_response(self):
        """Should correctly parse World Bank API JSON response."""
        from scraper.sources.world_bank_api import WorldBankAPIScraper

        with patch.object(WorldBankAPIScraper, "__init__", lambda self: None):
            scraper = WorldBankAPIScraper()
            scraper.name = "world_bank_api"

            # Mock the fetch method to return a fake WB API response
            mock_response = MagicMock()
            mock_response.json.return_value = [
                {"page": 1, "pages": 1, "total": 2},
                [
                    {"date": "2023", "value": 42.5, "indicator": {"id": "TEST"}},
                    {"date": "2022", "value": 40.1, "indicator": {"id": "TEST"}},
                    {"date": "2021", "value": None, "indicator": {"id": "TEST"}},
                ],
            ]

            with patch.object(scraper, "fetch", return_value=mock_response):
                rows = scraper._fetch_indicator("test_id", "TEST", "Test Indicator")

            assert len(rows) == 2  # None value should be excluded
            assert rows[0]["year"] == 2022  # sorted by year
            assert rows[1]["year"] == 2023
            assert rows[1]["value"] == 42.5


# ── Treasury Budget Tests ───────────────────────────────────────

class TestTreasuryBudget:
    """Test Treasury budget scraper logic."""

    def test_historical_data_has_all_sectors(self):
        """Each fiscal year should have total, education, healthcare, infrastructure."""
        from scraper.sources.treasury_budget import TreasuryBudgetScraper

        data = TreasuryBudgetScraper.HISTORICAL_DATA
        sectors_by_fy: dict[str, set] = {}
        for row in data:
            fy = row["fiscal_year"]
            sectors_by_fy.setdefault(fy, set()).add(row["sector"])

        expected_sectors = {"total", "education", "healthcare", "infrastructure"}
        for fy, sectors in sectors_by_fy.items():
            assert sectors == expected_sectors, f"{fy} missing sectors: {expected_sectors - sectors}"

    def test_historical_data_allocation_positive(self):
        """All allocations should be positive numbers."""
        from scraper.sources.treasury_budget import TreasuryBudgetScraper

        for row in TreasuryBudgetScraper.HISTORICAL_DATA:
            assert row["allocation_bn"] > 0, f"Non-positive allocation in {row}"


# ── County Allocations Tests ────────────────────────────────────

class TestCountyAllocations:
    """Test county allocation scraper logic."""

    def test_shares_sum_approximately_100(self):
        """County shares should sum to approximately 100%."""
        from scraper.sources.county_allocations import CountyAllocationsScraper

        total = sum(CountyAllocationsScraper.COUNTY_SHARES.values())
        # Shares are approximate from CRA reports; actual formula weights
        # are adjusted annually. Allow a wide tolerance.
        assert 80 < total < 105, f"County shares sum to {total}%, expected ~100%"

    def test_all_47_counties_have_shares(self):
        """All 47 counties should have a share defined."""
        from scraper.sources.county_allocations import CountyAllocationsScraper

        assert len(CountyAllocationsScraper.COUNTY_SHARES) == 47


# ── Debt CBK Tests ──────────────────────────────────────────────

class TestDebtCBK:
    """Test CBK debt scraper logic."""

    def test_debt_grows_over_time(self):
        """Debt stock should generally increase year over year."""
        from scraper.sources.debt_cbk import DebtCBKScraper

        data = DebtCBKScraper.HISTORICAL_DATA
        for i in range(1, len(data)):
            assert data[i]["debt_stock_bn"] >= data[i - 1]["debt_stock_bn"], (
                f"Debt decreased between {data[i-1]['fiscal_year']} and {data[i]['fiscal_year']}"
            )


# ── PDF Extractor Tests ─────────────────────────────────────────

class TestPDFExtractor:
    """Test PDF extraction utilities."""

    def test_tables_to_dicts(self):
        """tables_to_dicts should convert raw table to list of dicts."""
        from scraper.extractors.pdf_extractor import tables_to_dicts

        table = [
            ["County", "Allocation", "Expenditure"],
            ["Nairobi", "50,000", "45,000"],
            ["Mombasa", "30,000", "25,000"],
        ]

        result = tables_to_dicts(table, header_row=0)
        assert len(result) == 2
        assert result[0]["County"] == "Nairobi"
        assert result[1]["Expenditure"] == "25,000"

    def test_tables_to_dicts_empty(self):
        """tables_to_dicts should handle empty tables gracefully."""
        from scraper.extractors.pdf_extractor import tables_to_dicts

        assert tables_to_dicts([], header_row=0) == []
        assert tables_to_dicts([["Header"]], header_row=0) == []

    def test_tables_to_dicts_short_rows(self):
        """tables_to_dicts should pad short rows."""
        from scraper.extractors.pdf_extractor import tables_to_dicts

        table = [
            ["A", "B", "C"],
            ["1"],  # short row
        ]
        result = tables_to_dicts(table)
        assert len(result) == 1
        assert result[0]["A"] == "1"
        assert result[0]["B"] == ""

    def test_pdf_from_bytes(self):
        """pdf_from_bytes should create a temporary file."""
        from scraper.extractors.pdf_extractor import pdf_from_bytes

        content = b"%PDF-1.4 fake content"
        path = pdf_from_bytes(content)
        try:
            assert path.exists()
            assert path.read_bytes() == content
            assert path.suffix == ".pdf"
        finally:
            path.unlink(missing_ok=True)


# ── KNBS Indicators Tests ───────────────────────────────────────

class TestKNBSIndicators:
    """Test KNBS indicators scraper logic."""

    def test_snapshot_data_has_required_indicators(self):
        """Each county snapshot should have all four key indicators."""
        from scraper.sources.knbs_indicators import KNBSIndicatorsScraper

        required = {"skilled_birth_attendance", "poverty_headcount", "primary_completion", "pupil_teacher_ratio"}
        for key, indicators in KNBSIndicatorsScraper.COUNTY_SNAPSHOTS.items():
            assert set(indicators.keys()) == required, f"Missing indicators for {key}"

    def test_national_indicators_cover_full_range(self):
        """National indicators should cover 2013–2024."""
        from scraper.sources.knbs_indicators import KNBSIndicatorsScraper

        for indicator_id, values in KNBSIndicatorsScraper.NATIONAL_INDICATORS.items():
            years = set(values.keys())
            assert 2013 in years, f"{indicator_id} missing 2013"
            assert 2024 in years, f"{indicator_id} missing 2024"
