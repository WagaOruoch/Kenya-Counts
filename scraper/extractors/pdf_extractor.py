"""
PDF table extractor — shared utility for Kenya Counts scrapers.

Wraps pdfplumber (primary) and camelot (fallback) for extracting
tables from government budget PDFs, CoB reports, and KNBS publications.
"""

from __future__ import annotations

import logging
import tempfile
from pathlib import Path
from typing import Any

import pdfplumber

logger = logging.getLogger("kenya_counts.scraper.pdf")


def extract_tables_pdfplumber(
    pdf_path: str | Path,
    pages: list[int] | str = "all",
    table_settings: dict[str, Any] | None = None,
) -> list[list[list[str]]]:
    """
    Extract all tables from a PDF using pdfplumber.

    Args:
        pdf_path: Path to the PDF file.
        pages: List of 0-indexed page numbers, or "all".
        table_settings: Optional pdfplumber table extraction settings
                        (e.g. {"vertical_strategy": "text", "horizontal_strategy": "text"}).

    Returns:
        List of tables, where each table is a list of rows,
        and each row is a list of cell strings.
    """
    settings = table_settings or {}
    all_tables: list[list[list[str]]] = []

    with pdfplumber.open(pdf_path) as pdf:
        page_range = range(len(pdf.pages)) if pages == "all" else pages

        for page_num in page_range:
            if page_num >= len(pdf.pages):
                logger.warning("Page %d out of range (PDF has %d pages)", page_num, len(pdf.pages))
                continue

            page = pdf.pages[page_num]
            tables = page.extract_tables(table_settings=settings)

            for table in tables:
                # Clean: replace None with empty string, strip whitespace
                cleaned = []
                for row in table:
                    cleaned.append([
                        (cell.strip() if cell else "") for cell in row
                    ])
                all_tables.append(cleaned)
                logger.debug(
                    "Page %d: extracted table with %d rows", page_num, len(cleaned)
                )

    logger.info("Extracted %d tables from %s", len(all_tables), pdf_path)
    return all_tables


def extract_tables_camelot(
    pdf_path: str | Path,
    pages: str = "all",
    flavor: str = "lattice",
) -> list[list[list[str]]]:
    """
    Extract tables using camelot-py (fallback for complex layouts).

    Args:
        pdf_path: Path to the PDF file.
        pages: Page specification string (e.g. "1,2,3" or "all").
                Note: camelot uses 1-indexed pages.
        flavor: "lattice" (for bordered tables) or "stream" (for borderless).

    Returns:
        Same format as extract_tables_pdfplumber.
    """
    try:
        import camelot
    except ImportError:
        logger.error("camelot-py not installed. Install with: pip install camelot-py[cv]")
        raise

    tables = camelot.read_pdf(str(pdf_path), pages=pages, flavor=flavor)
    logger.info("Camelot extracted %d tables from %s", len(tables), pdf_path)

    result: list[list[list[str]]] = []
    for table in tables:
        df = table.df
        rows = df.values.tolist()
        # Convert all values to stripped strings
        cleaned = [[str(cell).strip() for cell in row] for row in rows]
        result.append(cleaned)

    return result


def extract_tables(
    pdf_path: str | Path,
    pages: list[int] | str = "all",
    fallback: bool = True,
    table_settings: dict[str, Any] | None = None,
) -> list[list[list[str]]]:
    """
    Extract tables from a PDF, trying pdfplumber first and camelot as fallback.

    Args:
        pdf_path: Path to the PDF file.
        pages: Page numbers (0-indexed for pdfplumber).
        fallback: If True, try camelot when pdfplumber finds no tables.
        table_settings: Optional pdfplumber settings.

    Returns:
        List of tables (list of rows of cell strings).
    """
    tables = extract_tables_pdfplumber(pdf_path, pages=pages, table_settings=table_settings)

    if not tables and fallback:
        logger.info("pdfplumber found no tables — trying camelot fallback")
        # Convert 0-indexed pages to camelot's 1-indexed format
        if isinstance(pages, list):
            camelot_pages = ",".join(str(p + 1) for p in pages)
        else:
            camelot_pages = pages
        tables = extract_tables_camelot(pdf_path, pages=camelot_pages)

    return tables


def extract_text(
    pdf_path: str | Path,
    pages: list[int] | str = "all",
) -> str:
    """
    Extract raw text from a PDF (useful for finding structure before table extraction).
    """
    text_parts: list[str] = []
    with pdfplumber.open(pdf_path) as pdf:
        page_range = range(len(pdf.pages)) if pages == "all" else pages
        for page_num in page_range:
            if page_num < len(pdf.pages):
                page_text = pdf.pages[page_num].extract_text() or ""
                text_parts.append(page_text)

    return "\n\n".join(text_parts)


def tables_to_dicts(
    table: list[list[str]],
    header_row: int = 0,
) -> list[dict[str, str]]:
    """
    Convert a raw table (list of rows) into a list of dicts
    using the specified row as column headers.
    """
    if not table or len(table) <= header_row:
        return []

    headers = table[header_row]
    rows = table[header_row + 1 :]

    result = []
    for row in rows:
        if len(row) < len(headers):
            row = row + [""] * (len(headers) - len(row))
        entry = {h: v for h, v in zip(headers, row) if h}
        if any(v.strip() for v in entry.values()):  # skip empty rows
            result.append(entry)

    return result


def pdf_from_bytes(content: bytes, suffix: str = ".pdf") -> Path:
    """
    Write bytes to a temporary file and return its path.
    Useful for processing PDFs fetched via HTTP.
    """
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
    tmp.write(content)
    tmp.close()
    return Path(tmp.name)
