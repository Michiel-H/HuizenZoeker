"""Collector for Huurwoningen.nl rental listings."""

from __future__ import annotations

import logging
import re

from bs4 import BeautifulSoup

from src.collectors.base import BaseCollector
from src.models import RawListing

logger = logging.getLogger(__name__)


class HuurwoningenCollector(BaseCollector):
    SOURCE_NAME = "Huurwoningen.nl"
    BASE_URL = "https://www.huurwoningen.nl"

    def collect(self) -> list[RawListing]:
        listings: list[RawListing] = []
        page = 1
        max_pages = 8

        while page <= max_pages:
            url = f"{self.BASE_URL}/in/amsterdam/"
            if page > 1:
                url += f"page-{page}/"
            try:
                html = self.fetch_page(url)
                soup = BeautifulSoup(html, "lxml")
                items = soup.select(".listing-search-item, [class*='search-item']")
                if not items:
                    break
                for item in items:
                    try:
                        listing = self._parse_item(item)
                        if listing:
                            listings.append(listing)
                    except Exception as e:
                        logger.debug(f"[Huurwoningen] Parse error: {e}")
                if not soup.select_one("a[rel='next'], a.next"):
                    break
                page += 1
            except Exception as e:
                logger.error(f"[Huurwoningen] Page fetch error: {e}")
                break
        return listings

    def _parse_item(self, item) -> RawListing | None:
        title_el = item.select_one("a[class*='title'], h2 a, h3 a")
        if not title_el:
            return None
        title = title_el.get_text(strip=True)
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
        loc_el = item.select_one("[class*='location'], [class*='subtitle']")
        if loc_el:
            location = loc_el.get_text(strip=True)

        area_m2 = None
        for feat in item.select("[class*='feature'], li"):
            m = re.search(r"(\d+)\s*m²", feat.get_text(strip=True))
            if m:
                area_m2 = float(m.group(1))
                break

        snippet = ""
        desc_el = item.select_one("[class*='description']")
        if desc_el:
            snippet = desc_el.get_text(strip=True)[:300]

        return RawListing(
            source=self.SOURCE_NAME, source_id=source_id, url=url, title=title,
            raw_location_text=location, price_raw=price_raw, area_m2=area_m2,
            description_snippet=snippet,
        )
