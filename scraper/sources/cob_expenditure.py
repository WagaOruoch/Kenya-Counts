"""
Controller of Budget (CoB) Expenditure Scraper.

Scrapes actual county government expenditure data from CoB's
Budget Implementation Review Reports (BIRRs). These are published
as PDFs — there is no API or structured data source.

This is the hardest scraper in the project. CoB reports vary in
format across years, and tables span multiple pages. The approach:
1. Find report PDF links on the CoB website
2. Download PDFs
3. Extract tables using pdf_extractor
4. Fall back to hard-coded summary data where PDF extraction fails

Output: data/raw/cob_expenditure/county_expenditure.csv
Columns: fiscal_year, county_code, county_name, allocation_mn,
         expenditure_mn, absorption_rate_pct, source_url
"""

from __future__ import annotations

import logging
import re
from pathlib import Path

from selectolax.parser import HTMLParser

from scraper.base import BaseScraper, load_counties, setup_logging
from scraper.extractors.pdf_extractor import (
    extract_tables,
    pdf_from_bytes,
    tables_to_dicts,
)

logger = logging.getLogger("kenya_counts.scraper.cob_expenditure")


class COBExpenditureScraper(BaseScraper):
    name = "cob_expenditure"

    # Summary-level county absorption rates from CoB annual reports.
    # absorption_rate = actual expenditure / allocation
    # These are the aggregate (all-county average) figures.
    # County-level detail should come from PDF extraction.
    AGGREGATE_ABSORPTION = {
        "2013/14": 0.68,  # first year of devolution — low absorption expected
        "2014/15": 0.74,
        "2015/16": 0.79,
        "2016/17": 0.81,
        "2017/18": 0.78,
        "2018/19": 0.80,
        "2019/20": 0.75,  # COVID impact
        "2020/21": 0.77,
        "2021/22": 0.82,
        "2022/23": 0.83,
        "2023/24": 0.80,
    }

    def scrape(self) -> list[dict]:
        """
        Strategy:
        1. Attempt to download and parse CoB BIRR PDFs
        2. If PDF extraction yields county-level data, use it
        3. Otherwise, estimate county expenditure from:
           - county_allocations data (what was allocated)
           - aggregate absorption rates (% typically spent)
        """
        counties = load_counties()
        rows: list[dict] = []

        # Attempt PDF scrape first
        try:
            pdf_rows = self._try_pdf_scrape()
            if pdf_rows:
                rows.extend(pdf_rows)
                logger.info("Extracted %d rows from CoB PDFs", len(pdf_rows))
        except Exception as e:
            logger.warning("CoB PDF scrape failed: %s", e)

        # If we don't have county-level data, generate estimates
        if not rows:
            logger.info("Generating expenditure estimates from absorption rates")
            rows = self._generate_estimates(counties)

        self.save_csv(rows, "county_expenditure.csv")
        self.update_manifest([
            {
                "filename": "county_expenditure.csv",
                "url": self.source_config.get("primary_url", ""),
                "description": "County government expenditure vs allocation (from CoB reports or estimated)",
            }
        ])
        return rows

    def _try_pdf_scrape(self) -> list[dict]:
        """
        Navigate CoB reports page, find BIRRs, download and extract.
        """
        url = self.source_config.get("primary_url", "")
        if not url:
            return []

        response = self.fetch(url)
        tree = HTMLParser(response.text)

        # Find links to annual BIRR PDFs
        pdf_links = []
        for a in tree.css("a[href]"):
            href = a.attributes.get("href", "")
            text = a.text(strip=True).lower()
            if href.endswith(".pdf") and any(
                kw in text
                for kw in ["annual", "implementation", "review", "county"]
            ):
                if not href.startswith("http"):
                    href = url.rstrip("/") + "/" + href.lstrip("/")
                pdf_links.append(href)

        logger.info("Found %d CoB report PDF links", len(pdf_links))

        all_rows: list[dict] = []
        for link in pdf_links[:5]:
            try:
                content = self.fetch_with_cache(link)
                pdf_path = pdf_from_bytes(content)
                try:
                    tables = extract_tables(pdf_path, pages="all")
                    for table in tables:
                        parsed = self._parse_expenditure_table(table)
                        all_rows.extend(parsed)
                finally:
                    pdf_path.unlink(missing_ok=True)
            except Exception as e:
                logger.warning("Failed to process %s: %s", link, e)

        return all_rows

    def _parse_expenditure_table(self, table: list[list[str]]) -> list[dict]:
        """
        Parse a county expenditure table from a CoB PDF.
        Look for county names and numeric columns (allocation, expenditure, absorption).
        """
        counties = load_counties()
        county_lookup = {}
        for c in counties:
            county_lookup[c["name"].lower()] = c
            for alias in c.get("aliases", []):
                county_lookup[alias.lower()] = c

        rows = []
        for row in table:
            if not row or len(row) < 3:
                continue

            # Try to match county name in first cell
            first_cell = row[0].strip().lower()
            matched = county_lookup.get(first_cell)

            if matched:
                numbers = []
                for cell in row[1:]:
                    cleaned = cell.replace(",", "").replace(" ", "").strip()
                    try:
                        numbers.append(float(cleaned))
                    except ValueError:
                        continue

                if len(numbers) >= 2:
                    allocation = numbers[0]
                    expenditure = numbers[1]
                    absorption = (expenditure / allocation * 100) if allocation > 0 else 0

                    rows.append({
                        "fiscal_year": "unknown",  # needs context from report title/headers
                        "county_code": matched["code"],
                        "county_name": matched["name"],
                        "allocation_mn": round(allocation, 1),
                        "expenditure_mn": round(expenditure, 1),
                        "absorption_rate_pct": round(absorption, 1),
                    })

        return rows

    def _generate_estimates(self, counties: list[dict]) -> list[dict]:
        """
        Generate estimated expenditure from allocation data + absorption rates.
        This is used when PDF extraction fails.
        """
        # Import county allocation shares
        from scraper.sources.county_allocations import CountyAllocationsScraper

        alloc_scraper = CountyAllocationsScraper()
        total_shares = alloc_scraper.TOTAL_EQUITABLE_SHARE
        county_shares = alloc_scraper.COUNTY_SHARES

        rows = []
        for fy, total_mn in total_shares.items():
            absorption = self.AGGREGATE_ABSORPTION.get(fy, 0.78)

            for county in counties:
                name = county["name"]
                code = county["code"]
                share_pct = county_shares.get(name, 1.0 / 47 * 100)
                allocation_mn = round(total_mn * share_pct / 100, 1)
                expenditure_mn = round(allocation_mn * absorption, 1)

                rows.append({
                    "fiscal_year": fy,
                    "county_code": code,
                    "county_name": name,
                    "allocation_mn": allocation_mn,
                    "expenditure_mn": expenditure_mn,
                    "absorption_rate_pct": round(absorption * 100, 1),
                    "source_url": "estimated from CoB aggregate absorption rates",
                })

        return rows


def main():
    setup_logging()
    scraper = COBExpenditureScraper()
    scraper.run()


if __name__ == "__main__":
    main()
