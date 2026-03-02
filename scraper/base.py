"""
BaseScraper — shared foundation for all Kenya Counts scrapers.

Provides:
- httpx async client with configurable timeouts
- tenacity retry with exponential backoff
- Per-domain rate limiting
- Local file caching (avoid re-downloading during development)
- Manifest tracking (records what was scraped, when, from where)
- Consistent logging
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
import yaml
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

# ── Project paths ────────────────────────────────────────────────
ROOT_DIR = Path(__file__).resolve().parent.parent
CONFIG_DIR = ROOT_DIR / "config"
DATA_RAW_DIR = ROOT_DIR / "data" / "raw"
CACHE_DIR = DATA_RAW_DIR / ".cache"
MANIFEST_PATH = DATA_RAW_DIR / "manifest.json"

logger = logging.getLogger("kenya_counts.scraper")


def load_config() -> dict[str, Any]:
    """Load sources.yaml and settings."""
    with open(CONFIG_DIR / "sources.yaml", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_counties() -> list[dict[str, Any]]:
    """Load the canonical county list from counties.yaml."""
    with open(CONFIG_DIR / "counties.yaml", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return data["counties"]


def load_date_ranges() -> dict[str, Any]:
    """Load fiscal year definitions from date_ranges.yaml."""
    with open(CONFIG_DIR / "date_ranges.yaml", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_indicators() -> dict[str, Any]:
    """Load indicator definitions from indicators.yaml."""
    with open(CONFIG_DIR / "indicators.yaml", encoding="utf-8") as f:
        return yaml.safe_load(f)


class BaseScraper:
    """
    Base class for all scrapers. Handles HTTP requests, retries,
    rate limiting, caching, and manifest updates.

    Subclasses must implement:
        - name: str  (e.g. "kra_revenue")
        - scrape() -> list[dict]  (the main entry point)
    """

    name: str = "base"

    def __init__(self) -> None:
        config = load_config()
        self.settings = config.get("settings", {})
        self.sources = config.get("sources", {})
        self.source_config = self.sources.get(self.name, {})

        # HTTP client
        self.timeout = httpx.Timeout(
            self.settings.get("timeout_seconds", 30), connect=10.0
        )
        self.headers = {
            "User-Agent": self.settings.get(
                "user_agent",
                "KenyaCounts/1.0",
            )
        }
        self.client = httpx.Client(
            timeout=self.timeout, headers=self.headers, follow_redirects=True
        )

        # Rate limiting
        self._rate_limit = self.settings.get("rate_limit_seconds", 2)
        self._last_request_time: float = 0

        # Retry settings
        self._max_retries = self.settings.get("max_retries", 3)
        self._backoff_multiplier = self.settings.get("retry_backoff_multiplier", 2)

        # Cache
        self._cache_dir = ROOT_DIR / self.settings.get("cache_dir", "data/raw/.cache")
        self._cache_dir.mkdir(parents=True, exist_ok=True)

        # Output directory
        self.output_dir = DATA_RAW_DIR / self.name
        self.output_dir.mkdir(parents=True, exist_ok=True)

        logger.info("Initialised scraper: %s", self.name)

    # ── HTTP with rate limiting ──────────────────────────────────

    def _rate_limit_wait(self) -> None:
        """Enforce minimum delay between requests."""
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < self._rate_limit:
            sleep_time = self._rate_limit - elapsed
            logger.debug("Rate limiting: sleeping %.1fs", sleep_time)
            time.sleep(sleep_time)

    @retry(
        retry=retry_if_exception_type((httpx.HTTPStatusError, httpx.TransportError)),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=2, max=30),
        reraise=True,
    )
    def fetch(self, url: str, **kwargs: Any) -> httpx.Response:
        """
        GET a URL with rate limiting and retries.
        Raises httpx.HTTPStatusError on 4xx/5xx after retries exhausted.
        """
        self._rate_limit_wait()
        logger.info("Fetching: %s", url)
        response = self.client.get(url, **kwargs)
        self._last_request_time = time.monotonic()
        response.raise_for_status()
        return response

    @retry(
        retry=retry_if_exception_type((httpx.HTTPStatusError, httpx.TransportError)),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=2, max=30),
        reraise=True,
    )
    def fetch_bytes(self, url: str, **kwargs: Any) -> bytes:
        """GET a URL and return raw bytes (for PDF downloads)."""
        self._rate_limit_wait()
        logger.info("Downloading: %s", url)
        response = self.client.get(url, **kwargs)
        self._last_request_time = time.monotonic()
        response.raise_for_status()
        return response.content

    # ── Caching ──────────────────────────────────────────────────

    def _cache_key(self, url: str) -> str:
        return hashlib.sha256(url.encode()).hexdigest()[:16]

    def get_cached(self, url: str) -> bytes | None:
        """Return cached content for a URL, or None."""
        path = self._cache_dir / f"{self._cache_key(url)}.bin"
        if path.exists():
            logger.debug("Cache hit: %s", url)
            return path.read_bytes()
        return None

    def set_cached(self, url: str, content: bytes) -> None:
        """Store content in cache."""
        path = self._cache_dir / f"{self._cache_key(url)}.bin"
        path.write_bytes(content)
        logger.debug("Cached: %s → %s", url, path.name)

    def fetch_with_cache(self, url: str, **kwargs: Any) -> bytes:
        """Fetch a URL, using cache if available."""
        cached = self.get_cached(url)
        if cached is not None:
            return cached
        content = self.fetch_bytes(url, **kwargs)
        self.set_cached(url, content)
        return content

    # ── File output ──────────────────────────────────────────────

    def save_csv(self, data: list[dict], filename: str) -> Path:
        """Save a list of dicts as CSV in the scraper's output directory."""
        import csv

        path = self.output_dir / filename
        if not data:
            logger.warning("No data to save for %s", filename)
            return path

        fieldnames = list(data[0].keys())
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)

        logger.info("Saved %d rows → %s", len(data), path)
        return path

    def save_pdf(self, content: bytes, filename: str) -> Path:
        """Save raw PDF bytes to the scraper's output directory."""
        path = self.output_dir / filename
        path.write_bytes(content)
        logger.info("Saved PDF → %s (%d bytes)", path, len(content))
        return path

    # ── Manifest ─────────────────────────────────────────────────

    def update_manifest(self, files: list[dict[str, str]]) -> None:
        """
        Record scraped files in the manifest.
        Each entry in `files` should have: filename, url, description.
        """
        manifest = self._load_manifest()
        now = datetime.now(timezone.utc).isoformat()

        for f in files:
            entry = {
                "scraper": self.name,
                "filename": f.get("filename", ""),
                "url": f.get("url", ""),
                "description": f.get("description", ""),
                "scraped_at": now,
            }
            # Replace existing entry for same scraper+filename, or append
            manifest["files"] = [
                e
                for e in manifest["files"]
                if not (
                    e.get("scraper") == self.name
                    and e.get("filename") == entry["filename"]
                )
            ]
            manifest["files"].append(entry)

        manifest["last_updated"] = now
        self._save_manifest(manifest)

    def _load_manifest(self) -> dict:
        if MANIFEST_PATH.exists():
            with open(MANIFEST_PATH, encoding="utf-8") as f:
                return json.load(f)
        return {"description": "Manifest of all scraped data files.", "last_updated": None, "files": []}

    def _save_manifest(self, manifest: dict) -> None:
        with open(MANIFEST_PATH, "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2, ensure_ascii=False)

    # ── Main interface ───────────────────────────────────────────

    def scrape(self) -> list[dict]:
        """
        Override in subclass. Should:
        1. Fetch data from source(s)
        2. Parse into list of dicts (rows)
        3. Call self.save_csv() / self.save_pdf()
        4. Call self.update_manifest()
        5. Return the parsed rows
        """
        raise NotImplementedError(f"{self.name}.scrape() not implemented")

    def run(self) -> list[dict]:
        """Execute the scraper with logging."""
        logger.info("=" * 60)
        logger.info("Starting scraper: %s", self.name)
        logger.info("=" * 60)
        try:
            rows = self.scrape()
            logger.info("Scraper %s finished: %d rows", self.name, len(rows))
            return rows
        except Exception:
            logger.exception("Scraper %s failed", self.name)
            raise
        finally:
            self.client.close()

    def close(self) -> None:
        """Clean up HTTP client."""
        self.client.close()


def setup_logging(level: int = logging.INFO) -> None:
    """Configure logging for the scraper package."""
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
