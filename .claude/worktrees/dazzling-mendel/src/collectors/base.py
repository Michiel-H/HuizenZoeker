"""Base collector with rate limiting, retries, and error handling."""

from __future__ import annotations

import logging
import time
from abc import ABC, abstractmethod

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from src.config import MAX_RETRIES, REQUEST_DELAY_SEC, REQUEST_TIMEOUT_SEC, USER_AGENT
from src.models import RawListing

logger = logging.getLogger(__name__)


class BaseCollector(ABC):
    """Base class for all rental listing collectors."""

    SOURCE_NAME: str = ""
    BASE_URL: str = ""

    def __init__(self):
        self._last_request_time: float = 0
        self._client: httpx.Client | None = None

    @property
    def client(self) -> httpx.Client:
        if self._client is None:
            self._client = httpx.Client(
                timeout=REQUEST_TIMEOUT_SEC,
                headers={
                    "User-Agent": USER_AGENT,
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "nl-NL,nl;q=0.9,en;q=0.8",
                },
                follow_redirects=True,
            )
        return self._client

    def _rate_limit(self) -> None:
        """Enforce rate limiting between requests."""
        elapsed = time.time() - self._last_request_time
        if elapsed < REQUEST_DELAY_SEC:
            time.sleep(REQUEST_DELAY_SEC - elapsed)
        self._last_request_time = time.time()

    @retry(
        stop=stop_after_attempt(MAX_RETRIES),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type((httpx.HTTPError, httpx.TimeoutException)),
    )
    def fetch_page(self, url: str) -> str:
        """Fetch a page with rate limiting and retries."""
        self._rate_limit()
        logger.info(f"[{self.SOURCE_NAME}] Fetching: {url}")
        response = self.client.get(url)
        response.raise_for_status()
        return response.text

    @retry(
        stop=stop_after_attempt(MAX_RETRIES),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type((httpx.HTTPError, httpx.TimeoutException)),
    )
    def fetch_json(self, url: str) -> dict:
        """Fetch JSON data with rate limiting and retries."""
        self._rate_limit()
        logger.info(f"[{self.SOURCE_NAME}] Fetching JSON: {url}")
        response = self.client.get(url)
        response.raise_for_status()
        return response.json()

    @abstractmethod
    def collect(self) -> list[RawListing]:
        """Collect listings from the source. Must be implemented by subclasses."""
        ...

    def safe_collect(self) -> list[RawListing]:
        """Collect with error handling - never raises, returns empty on failure."""
        try:
            listings = self.collect()
            logger.info(f"[{self.SOURCE_NAME}] Collected {len(listings)} listings")
            return listings
        except Exception as e:
            logger.error(f"[{self.SOURCE_NAME}] Collection failed: {e}", exc_info=True)
            return []

    def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            self._client.close()
            self._client = None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
