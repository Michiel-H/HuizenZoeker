"""Collector for Kamernet.nl rental listings."""

from __future__ import annotations

import logging
import re

from bs4 import BeautifulSoup

from src.collectors.base import BaseCollector
from src.models import RawListing

logger = logging.getLogger(__name__)


class KamernetCollector(BaseCollector):
    SOURCE_NAME = "Kamernet"
    BASE_URL = "https://kamernet.nl"

    def collect(self) -> list[RawListing]:
        listings: list[RawListing] = []
        url = f"{self.BASE_URL}/for-rent/apartments-amsterdam"
        try:
            html = self.fetch_page(url)
            soup = BeautifulSoup(html, "lxml")
            items = soup.select(".tile-listing, [class*='listing-card'], [class*='result-item']")
            for item in items:
                try:
                    listing = self._parse_item(item)
                    if listing:
                        listings.append(listing)
                except Exception as e:
                    logger.debug(f"[Kamernet] Parse error: {e}")
        except Exception as e:
            logger.error(f"[Kamernet] Fetch error: {e}")
        return listings

    def _parse_item(self, item) -> RawListing | None:
        title_el = item.select_one("a[href*='/for-rent/'], a[class*='title']")
        if not title_el:
            title_el = item if item.name == "a" else None
        if not title_el:
            return None
        title = title_el.get_text(strip=True)
        if not title or len(title) < 3:
            return None
        href = title_el.get("href", "")
        url = href if href.startswith("http") else f"{self.BASE_URL}{href}"
        source_id = href.strip("/").split("/")[-1] if href else None

        price_raw = None
        price_el = item.select_one("[class*='price']")
        if price_el:
            m = re.search(r"€\s*([\d.,]+)", price_el.get_text(strip=True))
            if m:
                from src.normalizer.price import _parse_price_string
                price_raw = _parse_price_string(m.group(1))

        location = ""
        loc_el = item.select_one("[class*='location'], [class*='address']")
        if loc_el:
            location = loc_el.get_text(strip=True)

        area_m2 = None
        area_el = item.select_one("[class*='area'], [class*='size']")
        if area_el:
            m = re.search(r"(\d+)\s*m²", area_el.get_text())
            if m:
                area_m2 = float(m.group(1))

        return RawListing(
            source=self.SOURCE_NAME, source_id=source_id, url=url, title=title,
            raw_location_text=location, price_raw=price_raw, area_m2=area_m2,
        )
