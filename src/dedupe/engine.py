"""Deduplication engine for cross-source matching."""

from __future__ import annotations

import re
import uuid
from dataclasses import dataclass

from rapidfuzz import fuzz

from src.config import (
    DEDUPE_AREA_TOLERANCE_M2,
    DEDUPE_COMBINED_THRESHOLD,
    DEDUPE_PRICE_TOLERANCE_EUR,
    DEDUPE_TITLE_SIMILARITY_THRESHOLD,
)
from src.models import NormalizedListing


@dataclass
class DedupeScore:
    """Score breakdown for a potential duplicate match."""

    title_sim: float = 0.0
    price_sim: float = 0.0
    area_sim: float = 0.0
    url_match: bool = False
    images_match: bool = False
    address_sim: float = 0.0
    combined: float = 0.0


def generate_dedupe_id() -> str:
    """Generate a new unique dedupe_id."""
    return str(uuid.uuid4())


def compute_dedupe_score(a: NormalizedListing, b: NormalizedListing) -> DedupeScore:
    """Compute a deduplication similarity score between two listings."""
    score = DedupeScore()

    # Title similarity (using token_sort_ratio for robustness)
    if a.title and b.title:
        score.title_sim = fuzz.token_sort_ratio(
            _clean_title(a.title), _clean_title(b.title)
        ) / 100.0

    # Price similarity
    if a.price_total_eur is not None and b.price_total_eur is not None:
        diff = abs(a.price_total_eur - b.price_total_eur)
        if diff <= DEDUPE_PRICE_TOLERANCE_EUR:
            score.price_sim = 1.0 - (diff / max(DEDUPE_PRICE_TOLERANCE_EUR, 1))
    elif a.price_total_eur is None and b.price_total_eur is None:
        score.price_sim = 0.5  # both unknown, neutral

    # Area similarity
    if a.area_m2 is not None and b.area_m2 is not None:
        diff = abs(a.area_m2 - b.area_m2)
        if diff <= DEDUPE_AREA_TOLERANCE_M2:
            score.area_sim = 1.0 - (diff / max(DEDUPE_AREA_TOLERANCE_M2, 1))
    elif a.area_m2 is None and b.area_m2 is None:
        score.area_sim = 0.3  # both unknown

    # URL match (same listing page on different paths)
    if a.url and b.url:
        score.url_match = _urls_same_property(a.url, b.url)

    # Images hash match
    if a.images_hash and b.images_hash:
        score.images_match = a.images_hash == b.images_hash

    # Address/location similarity
    if a.raw_location_text and b.raw_location_text:
        score.address_sim = fuzz.token_sort_ratio(
            a.raw_location_text.lower(), b.raw_location_text.lower()
        ) / 100.0

    # Combined score with weights
    weights = {
        "title": 0.25,
        "price": 0.20,
        "area": 0.15,
        "url": 0.10,
        "images": 0.15,
        "address": 0.15,
    }

    score.combined = (
        weights["title"] * score.title_sim
        + weights["price"] * score.price_sim
        + weights["area"] * score.area_sim
        + weights["url"] * (1.0 if score.url_match else 0.0)
        + weights["images"] * (1.0 if score.images_match else 0.0)
        + weights["address"] * score.address_sim
    )

    # Boost for strong individual signals
    if score.images_match:
        score.combined = max(score.combined, 0.85)
    if score.url_match:
        score.combined = max(score.combined, 0.90)

    return score


def find_duplicate(
    listing: NormalizedListing,
    existing: list[NormalizedListing],
) -> tuple[str | None, DedupeScore | None]:
    """Find the best duplicate match for a listing among existing ones.

    Returns (dedupe_id_of_match, score) or (None, None) if no match.
    """
    best_match_idx = None
    best_score = None

    for i, existing_listing in enumerate(existing):
        # Skip same source - handled by source+source_id
        if existing_listing.source == listing.source:
            continue

        # Quick reject: different neighborhoods (if both have one)
        if (
            listing.neighborhood_match
            and existing_listing.neighborhood_match
            and listing.neighborhood_match != existing_listing.neighborhood_match
        ):
            continue

        score = compute_dedupe_score(listing, existing_listing)
        if score.combined >= DEDUPE_COMBINED_THRESHOLD:
            if best_score is None or score.combined > best_score.combined:
                best_score = score
                best_match_idx = i

    if best_match_idx is not None and best_score is not None:
        return existing[best_match_idx].source_id, best_score
    return None, None


def _clean_title(title: str) -> str:
    """Clean title for comparison: remove source-specific prefixes, normalize."""
    title = title.lower()
    # Remove common prefixes like "Te huur:", "Huurwoning", etc.
    prefixes = [
        r"^te\s+huur\s*[:!-]\s*",
        r"^huurwoning\s*[:!-]\s*",
        r"^appartement\s*[:!-]\s*",
        r"^studio\s*[:!-]\s*",
    ]
    for pat in prefixes:
        title = re.sub(pat, "", title)
    # Collapse whitespace
    title = re.sub(r"\s+", " ", title).strip()
    return title


def _urls_same_property(url_a: str, url_b: str) -> bool:
    """Check if two URLs likely point to the same property."""
    # Same domain + very similar path
    from urllib.parse import urlparse

    pa = urlparse(url_a)
    pb = urlparse(url_b)

    if pa.netloc == pb.netloc and pa.path == pb.path:
        return True

    return False
