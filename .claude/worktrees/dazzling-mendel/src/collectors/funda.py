"""Collector for Funda.nl rental listings."""

from __future__ import annotations

import logging
import re

from bs4 import BeautifulSoup

from src.collectors.base import BaseCollector
from src.models import RawListing

logger = logging.getLogger(__name__)


class FundaCollector(BaseCollector):
    SOURCE_NAME = "Funda"
    BASE_URL = "https://www.funda.nl"

    def collect(self) -> list[RawListing]:
        listings: list[RawListing] = []
        page = 1
        max_pages = 10

        while page <= max_pages:
            url = f"{self.BASE_URL}/huur/amsterdam/0-2200/"
            if page > 1:
                url += f"p{page}/"
            try:
                html = self.fetch_page(url)
                soup = BeautifulSoup(html, "lxml")
                items = soup.select("[data-test-id='search-result-item'], div[class*='search-result']")
                if not items:
                    break
                for item in items:
                    try:
                        listing = self._parse_item(item)
                        if listing:
                            listings.append(listing)
                    except Exception as e:
                        logger.debug(f"[Funda] Parse error: {e}")
                if not soup.select_one("[rel='next'], [data-pagination-action='next']"):
                    break
                page += 1
            except Exception as e:
                logger.error(f"[Funda] Page fetch error: {e}")
                break
        return listings

    def _parse_item(self, item) -> RawListing | None:
        title_el = item.select_one("a[data-test-id='street-name-house-number'], a[href*='/huur/']")
        if not title_el:
            return None
        title = title_el.get_text(strip=True)
        href = title_el.get("href", "")
        url = f"{self.BASE_URL}{href}" if href.startswith("/") else href
        source_id = None
        id_match = re.search(r"/(\d+)-", href)
        if id_match:
            source_id = id_match.group(1)

        price_raw = None
        price_el = item.select_one("[data-test-id='price-rent'], [class*='price']")
        if price_el:
            m = re.search(r"€\s*([\d.,]+)", price_el.get_text(strip=True))
            if m:
                from src.normalizer.price import _parse_price_string
                price_raw = _parse_price_string(m.group(1))

        location = ""
        loc_el = item.select_one("[data-test-id='postal-code-city'], [class*='subtitle']")
        if loc_el:
            location = loc_el.get_text(strip=True)

        area_m2 = None
        area_el = item.select_one("[title*='m²'], span[class*='surface']")
        if area_el:
            m = re.search(r"(\d+)\s*m²", area_el.get_text())
            if m:
                area_m2 = float(m.group(1))

        bedrooms = None
        bed_el = item.select_one("[title*='slaapkamer']")
        if bed_el:
            m = re.search(r"(\d+)", bed_el.get_text())
            if m:
                bedrooms = int(m.group(1))

        return RawListing(
            source=self.SOURCE_NAME, source_id=source_id, url=url, title=title,
            raw_location_text=location, price_raw=price_raw, area_m2=area_m2,
            bedrooms=bedrooms,
        )
