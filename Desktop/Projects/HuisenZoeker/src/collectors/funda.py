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
                
                # Find all anchor tags pointing to a rental detail page
                anchors = soup.select("a[href*='/detail/huur/']")
                if not anchors and page > 1:
                    break
                    
                processed_urls = set()
                
                for a in anchors:
                    href = a.get("href", "")
                    if href in processed_urls:
                        continue
                    processed_urls.add(href)
                    
                    try:
                        listing = self._parse_from_anchor(a)
                        if listing:
                            listings.append(listing)
                    except Exception as e:
                        logger.debug(f"[Funda] Parse error: {e}")
                        
                # Next page detection
                if not soup.select_one("a[aria-label*='Volgende'], [data-pagination-action='next']"):
                     # Sometimes Funda uses specific classes for pagination, fallback to breaking if few items
                     if len(processed_urls) < 10: 
                        break
                page += 1
            except Exception as e:
                logger.error(f"[Funda] Page fetch error: {e}")
                break
        return listings

    def _parse_from_anchor(self, a_tag) -> RawListing | None:
        href = a_tag.get("href", "")
        url = f"{self.BASE_URL}{href}" if href.startswith("/") else href
        
        # Source ID
        source_id = None
        id_match = re.search(r"/(\d+)-", href)
        if id_match:
            source_id = id_match.group(1)

        # Container fallback
        container = a_tag.find_parent("div", class_=re.compile(r"border-b|pb-[\d]|flex-col"))
        text_content = ""
        if container:
            text_content = container.get_text(separator=" ", strip=True)
        else:
            text_content = a_tag.parent.get_text(separator=" ", strip=True)

        # Title
        raw_title = a_tag.get_text(separator=" ", strip=True)
        title = re.sub(r'^(Nieuw|Uitgelicht|In prijs verlaagd|Blikvanger|Open huis|Top-listing)\s+', '', raw_title).strip()
        if not title:
            return None

        # Price
        price_raw = None
        m_price = re.search(r"€\s*([\d.,]+)", text_content)
        if m_price:
            from src.normalizer.price import _parse_price_string
            price_raw = _parse_price_string(m_price.group(1))

        # Location - Often the text immediately following the title or postcode
        location = ""
        m_loc = re.search(r"(\d{4}\s*[A-Z]{2})\s+([^€]+)", text_content)
        if m_loc:
            location = f"{m_loc.group(1)} {m_loc.group(2).strip()}"

        # Area
        area_m2 = None
        m_area = re.search(r"(\d+)\s*m²", text_content)
        if m_area:
            area_m2 = float(m_area.group(1))

        # Bedrooms
        bedrooms = None
        m_beds = re.search(r"(\d+)\s*(?:slaap)?kamer", text_content)
        if m_beds:
            bedrooms = int(m_beds.group(1))

        return RawListing(
            source=self.SOURCE_NAME, source_id=source_id, url=url, title=title,
            raw_location_text=location, price_raw=price_raw, area_m2=area_m2,
            bedrooms=bedrooms,
        )
