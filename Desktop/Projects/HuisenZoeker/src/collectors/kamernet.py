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
        
        # Kamernet split their search into rooms and apartments
        target_urls = [
            f"{self.BASE_URL}/huren/appartement-amsterdam",
            f"{self.BASE_URL}/huren/kamer-amsterdam"
        ]
        
        for url in target_urls:
            try:
                html = self.fetch_page(url)
                soup = BeautifulSoup(html, "lxml")
                
                # Find all anchor tags pointing to a rental detail page
                anchors = soup.select("a[href*='/huren/appartement-amsterdam/'], a[href*='/huren/kamer-amsterdam/']")
                processed_urls = set()
                
                for a in anchors:
                    href = a.get("href", "")
                    if not href or href in processed_urls:
                        continue
                    processed_urls.add(href)
                    
                    try:
                        listing = self._parse_from_anchor(a)
                        if listing:
                            listings.append(listing)
                    except Exception as e:
                        logger.debug(f"[Kamernet] Parse error: {e}")
            except Exception as e:
                logger.error(f"[Kamernet] Fetch error for {url}: {e}")
                
        return listings

    def _parse_from_anchor(self, a_tag) -> RawListing | None:
        href = a_tag.get("href", "")
        url = href if href.startswith("http") else f"{self.BASE_URL}{href}"
        
        # Source ID is usually the last part of the URL (e.g., /appartement-2361069)
        source_id = href.strip("/").split("/")[-1] if href else None
        if source_id and "-" in source_id:
            source_id = source_id.split("-")[-1]

        # Use the parent container to get all text
        container = a_tag.find_parent("div", class_=re.compile(r"card|tile|item|flex-col", re.I))
        text_content = ""
        if container:
            text_content = container.get_text(separator=" ", strip=True)
        else:
            text_content = a_tag.parent.get_text(separator=" ", strip=True)

        # Title is often inside the anchor or nearby
        title = a_tag.get_text(separator=" ", strip=True)
        if not title or len(title) < 3:
            # Fallback: try to find a header near the anchor
            if container:
                h_tag = container.find(["h2", "h3", "h4", "h5"])
                if h_tag:
                    title = h_tag.get_text(strip=True)
        
        if not title:
             title = f"Kamernet {source_id}"

        # Price
        price_raw = None
        m_price = re.search(r"€\s*([\d.,]+)", text_content)
        if m_price:
            from src.normalizer.price import _parse_price_string
            price_raw = _parse_price_string(m_price.group(1))

        # Location - Often a postcode or street name
        location = ""
        m_loc = re.search(r"(\d{4}\s*[A-Z]{2})\s+([^€]+)", text_content)
        if m_loc:
             location = f"{m_loc.group(1)} {m_loc.group(2).strip()}"
        else:
             # Look for Amsterdam
             m_ams = re.search(r"(Amsterdam[^€]*)", text_content)
             if m_ams:
                 # Take just a small snippet
                 location = m_ams.group(1)[:50].strip()

        # Area
        area_m2 = None
        m_area = re.search(r"(\d+)\s*m[2²]", text_content)
        if m_area:
            area_m2 = float(m_area.group(1))

        return RawListing(
            source=self.SOURCE_NAME, source_id=source_id, url=url, title=title,
            raw_location_text=location, price_raw=price_raw, area_m2=area_m2,
        )
