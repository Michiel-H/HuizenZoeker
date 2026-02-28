"""Collector for Vesteda.com rental listings."""

from __future__ import annotations

import logging
import re

from bs4 import BeautifulSoup

from src.collectors.base import BaseCollector
from src.models import RawListing

logger = logging.getLogger(__name__)


class VestedaCollector(BaseCollector):
    SOURCE_NAME = "Vesteda"
    BASE_URL = "https://www.vesteda.com"

    def collect(self) -> list[RawListing]:
        listings: list[RawListing] = []
        html_url = f"{self.BASE_URL}/en/apartments/amsterdam/"
        try:
            try:
                data = self.fetch_json(f"{self.BASE_URL}/api/properties?city=amsterdam&maxRent=2500")
                items = data if isinstance(data, list) else data.get("results", [])
                for item in items:
                    listing = self._parse_api_item(item)
                    if listing:
                        listings.append(listing)
            except Exception:
                logger.debug("[Vesteda] API not available, trying HTML")

            if not listings:
                html = self.fetch_page(html_url)
                soup = BeautifulSoup(html, "lxml")
                for item in soup.select("[class*='property-card'], [class*='unit-card']"):
                    try:
                        listing = self._parse_html_item(item)
                        if listing:
                            listings.append(listing)
                    except Exception as e:
                        logger.debug(f"[Vesteda] Parse error: {e}")
        except Exception as e:
            logger.error(f"[Vesteda] Fetch error: {e}")
        return listings

    def _parse_api_item(self, item: dict) -> RawListing | None:
        title = item.get("name") or item.get("title") or item.get("address", "")
        if not title:
            return None
        url = item.get("url", "")
        if url and not url.startswith("http"):
            url = f"{self.BASE_URL}{url}"
        price_raw = item.get("rent") or item.get("price") or item.get("totalRent")
        area_m2 = item.get("surface") or item.get("area")
        bedrooms = item.get("bedrooms") or item.get("rooms")
        location = item.get("city", "Amsterdam")
        if item.get("neighborhood"):
            location = f"{item['neighborhood']}, {location}"
        source_id = str(item.get("id", "")) or None
        return RawListing(
            source=self.SOURCE_NAME, source_id=source_id, url=url, title=title,
            raw_location_text=location,
            price_raw=float(price_raw) if price_raw else None,
            area_m2=float(area_m2) if area_m2 else None,
            bedrooms=int(bedrooms) if bedrooms else None,
            price_includes_service_costs=True,
        )

    def _parse_html_item(self, item) -> RawListing | None:
        title_el = item.select_one("a[href], [class*='title']")
        if not title_el:
            return None
        title = title_el.get_text(strip=True)
        href = title_el.get("href", "")
        url = href if href.startswith("http") else f"{self.BASE_URL}{href}"
        source_id = href.strip("/").split("/")[-1] if href else None
        price_raw = None
        price_el = item.select_one("[class*='price'], [class*='rent']")
        if price_el:
            m = re.search(r"€\s*([\d.,]+)", price_el.get_text())
            if m:
                from src.normalizer.price import _parse_price_string
                price_raw = _parse_price_string(m.group(1))
        area_m2 = None
        for el in item.select("[class*='detail'], span, li"):
            m = re.search(r"(\d+)\s*m²", el.get_text(strip=True))
            if m:
                area_m2 = float(m.group(1))
                break
        return RawListing(
            source=self.SOURCE_NAME, source_id=source_id, url=url, title=title,
            raw_location_text="Amsterdam", price_raw=price_raw, area_m2=area_m2,
            price_includes_service_costs=True,
        )
