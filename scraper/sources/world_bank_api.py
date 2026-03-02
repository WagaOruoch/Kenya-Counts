"""
World Bank API Scraper — clean access to Kenya's development indicators.

Uses the World Bank Open Data API (via httpx, not wbgapi) to fetch
national-level indicators defined in config/indicators.yaml.

This is the preferred source for indicators that KNBS also publishes,
because the API is stable, paginated, and returns structured JSON.

Output: data/raw/world_bank_api/wb_indicators.csv
Columns: indicator_id, indicator_name, year, value, source_url
"""

from __future__ import annotations

import logging

from scraper.base import BaseScraper, load_indicators, setup_logging

logger = logging.getLogger("kenya_counts.scraper.world_bank_api")

# World Bank API v2 base
WB_API_BASE = "https://api.worldbank.org/v2"
COUNTRY = "KE"
DATE_RANGE = "2013:2025"


class WorldBankAPIScraper(BaseScraper):
    name = "world_bank_api"

    # Indicators to fetch — id: (wb_code, human_name)
    # Sourced from config/indicators.yaml where source == world_bank_api
    INDICATORS = {
        "maternal_mortality_rate": ("SH.STA.MMRT", "Maternal Mortality Ratio"),
        "immunisation_coverage": ("SH.IMM.IDPT", "DPT3 Immunisation Coverage"),
        "primary_enrollment_rate": ("SE.PRM.NENR", "Primary School Net Enrollment Rate"),
        "primary_completion_rate": ("SE.PRM.CMPT.ZS", "Primary School Completion Rate"),
        "pupil_teacher_ratio": ("SE.PRM.ENRL.TC.ZS", "Pupil-to-Teacher Ratio (Primary)"),
        "electrification_rate": ("EG.ELC.ACCS.ZS", "Access to Electricity"),
        # Additional useful indicators
        "gdp_current": ("NY.GDP.MKTP.CN", "GDP (current LCU)"),
        "population": ("SP.POP.TOTL", "Total Population"),
        "gni_per_capita": ("NY.GNP.PCAP.CD", "GNI per Capita (USD)"),
        "life_expectancy": ("SP.DYN.LE00.IN", "Life Expectancy at Birth"),
        "infant_mortality": ("SP.DYN.IMRT.IN", "Infant Mortality Rate"),
        "tax_revenue_pct_gdp": ("GC.TAX.TOTL.GD.ZS", "Tax Revenue (% of GDP)"),
        "govt_expenditure_pct_gdp": ("GC.XPN.TOTL.GD.ZS", "Government Expenditure (% of GDP)"),
        "debt_total": ("GC.DOD.TOTL.CN", "Central Government Debt, Total (LCU)"),
    }

    def scrape(self) -> list[dict]:
        """
        Fetch each indicator from the World Bank API.
        Returns a flat list of {indicator_id, indicator_name, year, value, source_url}.
        """
        all_rows: list[dict] = []

        for indicator_id, (wb_code, name) in self.INDICATORS.items():
            try:
                rows = self._fetch_indicator(indicator_id, wb_code, name)
                all_rows.extend(rows)
                logger.info("  %s: %d data points", indicator_id, len(rows))
            except Exception as e:
                logger.warning("  Failed to fetch %s (%s): %s", indicator_id, wb_code, e)

        # Save
        self.save_csv(all_rows, "wb_indicators.csv")
        self.update_manifest([
            {
                "filename": "wb_indicators.csv",
                "url": f"{WB_API_BASE}/country/{COUNTRY}/indicator/",
                "description": f"World Bank indicators for Kenya ({len(self.INDICATORS)} indicators, {DATE_RANGE})",
            }
        ])

        return all_rows

    def _fetch_indicator(
        self, indicator_id: str, wb_code: str, name: str
    ) -> list[dict]:
        """Fetch a single indicator from the World Bank API."""
        url = (
            f"{WB_API_BASE}/country/{COUNTRY}/indicator/{wb_code}"
            f"?date={DATE_RANGE}&format=json&per_page=100"
        )

        response = self.fetch(url)
        data = response.json()

        # WB API returns [metadata, data_array]
        if not isinstance(data, list) or len(data) < 2:
            logger.warning("Unexpected API response for %s", wb_code)
            return []

        records = data[1]
        if not records:
            return []

        rows = []
        for record in records:
            value = record.get("value")
            if value is not None:
                rows.append({
                    "indicator_id": indicator_id,
                    "indicator_name": name,
                    "wb_code": wb_code,
                    "year": int(record["date"]),
                    "value": float(value),
                    "source_url": url,
                })

        return sorted(rows, key=lambda r: r["year"])


def main():
    setup_logging()
    scraper = WorldBankAPIScraper()
    scraper.run()


if __name__ == "__main__":
    main()
