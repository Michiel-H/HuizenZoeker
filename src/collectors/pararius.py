"""Collector for Pararius.nl rental listings."""

from __future__ import annotations

import logging
import re

from bs4 import BeautifulSoup

from src.collectors.base import BaseCollector
from src.models import RawListing

logger = logging.getLogger(__name__)


class ParariusCollector(BaseCollector):
    SOURCE_NAME = "Pararius"
    BASE_URL = "https://www.pararius.nl"

    def collect(self) -> list[RawListing]:
        listings: list[RawListing] = []
        page = 1
        max_pages = 10

        while page <= max_pages:
            url = f"{self.BASE_URL}/huurwoningen/amsterdam/0-2500/"
            if page > 1:
                url += f"page-{page}"

            try:
                html = self.fetch_page(url)
                soup = BeautifulSoup(html, "lxml")
                items = soup.select("li.search-list__item--listing, section.listing-search-item")
                if not items:
                    break

                for item in items:
                    try:
                        listing = self._parse_item(item)
                        if listing:
                            listings.append(listing)
                    except Exception as e:
                        logger.debug(f"[Pararius] Parse error: {e}")

                if not soup.select_one("a.pagination__link--next"):
                    break
                page += 1
            except Exception as e:
                logger.error(f"[Pararius] Page fetch error: {e}")
                break
        return listings

    def _parse_item(self, item) -> RawListing | None:
        title_el = item.select_one("a.listing-search-item__link--title, a[class*='title']")
        if not title_el:
            return None
        title = title_el.get_text(strip=True)
        href = title_el.get("href", "")
        url = f"{self.BASE_URL}{href}" if href.startswith("/") else href
        source_id = href.strip("/").split("/")[-1] if href else None

        price_raw = None
        price_el = item.select_one(".listing-search-item__price, [class*='price']")
        if price_el:
            m = re.search(r"€\s*([\d.,]+)", price_el.get_text(strip=True))
            if m:
                from src.normalizer.price import _parse_price_string
                price_raw = _parse_price_string(m.group(1))

        location = ""
        loc_el = item.select_one(".listing-search-item__sub-title, [class*='location']")
        if loc_el:
            location = loc_el.get_text(strip=True)

        area_m2 = None
        for feat in item.select(".illustrated-features__item, li"):
            m = re.search(r"(\d+)\s*m²", feat.get_text(strip=True))
            if m:
                area_m2 = float(m.group(1))
                break

        snippet = ""
        desc_el = item.select_one(".listing-search-item__description")
        if desc_el:
            snippet = desc_el.get_text(strip=True)[:300]

        service_costs = None
        includes_service = False
        cond_el = item.select_one(".listing-search-item__price-conditions")
        if cond_el:
            cond_text = cond_el.get_text(strip=True).lower()
            sc_match = re.search(r"servicekosten\s*€?\s*([\d.,]+)", cond_text)
            if sc_match:
                from src.normalizer.price import _parse_price_string
                service_costs = _parse_price_string(sc_match.group(1))
            if "incl" in cond_text and "service" in cond_text:
                includes_service = True

        return RawListing(
            source=self.SOURCE_NAME, source_id=source_id, url=url, title=title,
            raw_location_text=location, price_raw=price_raw,
            service_costs_raw=service_costs,
            price_includes_service_costs=includes_service,
            area_m2=area_m2, description_snippet=snippet,
        )
