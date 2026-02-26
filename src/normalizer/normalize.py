"""Main normalization pipeline for raw listings."""

from __future__ import annotations

import hashlib
import re
from urllib.parse import urlparse, urlunparse

from src.matcher.neighborhood import match_neighborhood
from src.models import NormalizedListing, PriceQuality, RawListing
from src.normalizer.price import parse_price


def normalize_listing(raw: RawListing) -> NormalizedListing:
    """Normalize a raw listing into a consistent format."""
    # Price normalization
    price_text = f"{raw.title} {raw.description_snippet}"
    price_result = parse_price(
        price_raw=raw.price_raw,
        service_costs_raw=raw.service_costs_raw,
        price_text=price_text,
        includes_service_costs=raw.price_includes_service_costs,
        gwl_included=raw.gwl_included,
    )

    # Neighborhood matching
    hood_result = match_neighborhood(
        title=raw.title,
        location_text=raw.raw_location_text,
        description=raw.description_snippet,
    )

    # Image hash for dedup
    images_hash = None
    if raw.image_urls:
        # Hash the sorted, normalized image URLs
        normalized_urls = sorted(set(canonicalize_url(u) for u in raw.image_urls if u))
        if normalized_urls:
            hash_input = "|".join(normalized_urls)
            images_hash = hashlib.md5(hash_input.encode()).hexdigest()

    # URL canonicalization
    url = canonicalize_url(raw.url)

    return NormalizedListing(
        source=raw.source,
        source_id=raw.source_id,
        url=url,
        title=raw.title.strip(),
        raw_location_text=raw.raw_location_text.strip(),
        neighborhood_match=hood_result.name,
        neighborhood_confidence=hood_result.confidence,
        price_total_eur=price_result.total_eur,
        price_quality=price_result.quality,
        price_includes_service_costs=price_result.includes_service_costs,
        gwl_included=price_result.gwl_included,
        area_m2=raw.area_m2,
        bedrooms=raw.bedrooms,
        property_type=raw.property_type,
        available_from=raw.available_from,
        description_snippet=raw.description_snippet.strip()[:500],
        images_hash=images_hash,
        ambiguous_neighborhood=hood_result.ambiguous,
    )


def canonicalize_url(url: str) -> str:
    """Normalize a URL for deduplication purposes."""
    if not url:
        return ""
    url = url.strip()
    parsed = urlparse(url)
    # Remove trailing slashes, fragments, common tracking params
    path = parsed.path.rstrip("/")
    # Remove common tracking parameters
    clean = urlunparse((
        parsed.scheme or "https",
        parsed.netloc.lower(),
        path,
        "",  # params
        "",  # query (strip tracking params)
        "",  # fragment
    ))
    return clean
