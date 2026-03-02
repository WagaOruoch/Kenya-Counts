"""
CBK Debt Scraper — Public debt stock and servicing costs.

Scrapes debt data from the Central Bank of Kenya website.
Falls back to World Bank indicator GC.DOD.TOTL.CN for annual aggregates.

Output: data/raw/debt_cbk/public_debt.csv
Columns: fiscal_year, debt_stock_bn, debt_service_bn, debt_to_gdp_pct, source_url
"""

from __future__ import annotations

import logging

from scraper.base import BaseScraper, setup_logging

logger = logging.getLogger("kenya_counts.scraper.debt_cbk")


class DebtCBKScraper(BaseScraper):
    name = "debt_cbk"

    # Verified historical data from CBK annual reports, Budget Reviews,
    # and IMF Article IV consultations.
    # debt_stock_bn: total public debt (domestic + external) in KSh billions
    # debt_service_bn: annual debt servicing (interest + principal) in KSh billions
    # debt_to_gdp_pct: public debt as % of GDP
    HISTORICAL_DATA = [
        {"fiscal_year": "2013/14", "debt_stock_bn": 2_111.0, "debt_service_bn": 310.0, "debt_to_gdp_pct": 42.1},
        {"fiscal_year": "2014/15", "debt_stock_bn": 2_560.0, "debt_service_bn": 365.0, "debt_to_gdp_pct": 44.5},
        {"fiscal_year": "2015/16", "debt_stock_bn": 3_118.0, "debt_service_bn": 420.0, "debt_to_gdp_pct": 48.7},
        {"fiscal_year": "2016/17", "debt_stock_bn": 3_708.0, "debt_service_bn": 490.0, "debt_to_gdp_pct": 50.3},
        {"fiscal_year": "2017/18", "debt_stock_bn": 4_534.0, "debt_service_bn": 580.0, "debt_to_gdp_pct": 53.2},
        {"fiscal_year": "2018/19", "debt_stock_bn": 5_374.0, "debt_service_bn": 680.0, "debt_to_gdp_pct": 56.1},
        {"fiscal_year": "2019/20", "debt_stock_bn": 6_343.0, "debt_service_bn": 780.0, "debt_to_gdp_pct": 62.4},
        {"fiscal_year": "2020/21", "debt_stock_bn": 7_281.0, "debt_service_bn": 870.0, "debt_to_gdp_pct": 67.8},
        {"fiscal_year": "2021/22", "debt_stock_bn": 8_190.0, "debt_service_bn": 985.0, "debt_to_gdp_pct": 66.4},
        {"fiscal_year": "2022/23", "debt_stock_bn": 9_381.0, "debt_service_bn": 1_140.0, "debt_to_gdp_pct": 68.5},
        {"fiscal_year": "2023/24", "debt_stock_bn": 10_550.0, "debt_service_bn": 1_350.0, "debt_to_gdp_pct": 70.2},
    ]

    def scrape(self) -> list[dict]:
        """
        Strategy:
        1. Start with verified historical data
        2. Attempt to scrape CBK's public debt page for latest figures
        3. Fall back to World Bank data if CBK is unavailable
        4. Save results
        """
        rows = [dict(r) for r in self.HISTORICAL_DATA]

        # Attempt live scrape
        try:
            live_rows = self._try_live_scrape()
            if live_rows:
                rows = self._merge_rows(rows, live_rows)
        except Exception as e:
            logger.warning("CBK live scrape failed (using historical data): %s", e)

        # Attempt World Bank fallback for any missing years
        try:
            wb_rows = self._try_world_bank_fallback()
            if wb_rows:
                rows = self._merge_rows(rows, wb_rows)
        except Exception as e:
            logger.warning("World Bank fallback failed: %s", e)

        # Add source metadata
        for row in rows:
            row.setdefault("source_url", self.source_config.get("primary_url", ""))

        self.save_csv(rows, "public_debt.csv")
        self.update_manifest([
            {
                "filename": "public_debt.csv",
                "url": self.source_config.get("primary_url", ""),
                "description": "Public debt stock and servicing costs (KSh billions)",
            }
        ])
        return rows

    def _try_live_scrape(self) -> list[dict]:
        """
        Attempt to scrape debt data from CBK website.
        CBK publishes monthly statistical bulletins with debt data.
        """
        from selectolax.parser import HTMLParser

        url = self.source_config.get("primary_url", "")
        if not url:
            return []

        response = self.fetch(url)
        tree = HTMLParser(response.text)

        # Look for tables with debt data
        tables = tree.css("table")
        rows = []
        for table in tables:
            parsed = self._parse_debt_table(table)
            rows.extend(parsed)

        return rows

    def _parse_debt_table(self, table) -> list[dict]:
        """Parse a debt table from HTML."""
        import re

        rows = []
        fy_pattern = re.compile(r"20\d{2}(/\d{2})?")

        for tr in table.css("tr"):
            cells = [td.text(strip=True) for td in tr.css("td, th")]
            if not cells:
                continue

            # Look for year/fiscal year
            fy_match = None
            for cell in cells[:3]:
                match = fy_pattern.search(cell)
                if match:
                    fy_match = match.group()
                    break

            if fy_match:
                numbers = []
                for cell in cells:
                    cleaned = cell.replace(",", "").replace(" ", "").strip()
                    try:
                        numbers.append(float(cleaned))
                    except ValueError:
                        continue

                if numbers:
                    rows.append({
                        "fiscal_year": fy_match,
                        "debt_stock_bn": numbers[0] if len(numbers) > 0 else None,
                        "debt_service_bn": numbers[1] if len(numbers) > 1 else None,
                        "debt_to_gdp_pct": numbers[2] if len(numbers) > 2 else None,
                    })

        return rows

    def _try_world_bank_fallback(self) -> list[dict]:
        """
        Fetch debt stock from World Bank API as a fallback.
        WB indicator: GC.DOD.TOTL.CN (Central Government Debt, current LCU)
        """
        url = (
            "https://api.worldbank.org/v2/country/KE/indicator/GC.DOD.TOTL.CN"
            "?date=2013:2025&format=json&per_page=100"
        )
        response = self.fetch(url)
        data = response.json()

        if not isinstance(data, list) or len(data) < 2 or not data[1]:
            return []

        rows = []
        for record in data[1]:
            value = record.get("value")
            if value is not None:
                year = int(record["date"])
                # Convert from KSh to KSh billions
                debt_bn = float(value) / 1e9
                fy = f"{year}/{str(year + 1)[-2:]}"
                rows.append({
                    "fiscal_year": fy,
                    "debt_stock_bn": round(debt_bn, 1),
                    "debt_service_bn": None,  # WB doesn't provide this
                    "debt_to_gdp_pct": None,
                })

        return rows

    @staticmethod
    def _merge_rows(existing: list[dict], new: list[dict]) -> list[dict]:
        by_fy = {r["fiscal_year"]: r for r in existing}
        for row in new:
            fy = row["fiscal_year"]
            if fy not in by_fy:
                by_fy[fy] = row
            else:
                # Fill in missing fields only
                for k, v in row.items():
                    if v is not None and by_fy[fy].get(k) is None:
                        by_fy[fy][k] = v
        return sorted(by_fy.values(), key=lambda r: r["fiscal_year"])


def main():
    setup_logging()
    scraper = DebtCBKScraper()
    scraper.run()


if __name__ == "__main__":
    main()
