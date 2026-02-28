"""Data models for Amsterdam Rental Monitor."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional


class ListingStatus(str, Enum):
    ACTIVE = "ACTIVE"
    REMOVED = "REMOVED"


class PriceQuality(str, Enum):
    CONFIRMED = "CONFIRMED"  # service costs explicitly included
    UNKNOWN = "UNKNOWN"  # unclear if service costs are included


class ChangeType(str, Enum):
    NEW = "NEW"
    CHANGED = "CHANGED"
    REMOVED = "REMOVED"


@dataclass
class RawListing:
    """Raw listing as parsed from a source, before normalization."""

    source: str
    source_id: Optional[str] = None
    url: str = ""
    title: str = ""
    raw_location_text: str = ""
    price_raw: Optional[float] = None
    service_costs_raw: Optional[float] = None
    price_includes_service_costs: bool = False
    gwl_included: bool = False
    area_m2: Optional[float] = None
    bedrooms: Optional[int] = None
    property_type: Optional[str] = None
    available_from: Optional[str] = None
    description_snippet: str = ""
    image_urls: list[str] = field(default_factory=list)


@dataclass
class NormalizedListing:
    """Listing after normalization and neighborhood matching."""

    source: str
    source_id: Optional[str]
    url: str
    title: str
    raw_location_text: str
    neighborhood_match: Optional[str]
    neighborhood_confidence: float
    price_total_eur: Optional[float]
    price_quality: PriceQuality
    price_includes_service_costs: bool
    gwl_included: bool
    area_m2: Optional[float]
    bedrooms: Optional[int]
    property_type: Optional[str]
    available_from: Optional[str]
    description_snippet: str
    images_hash: Optional[str]
    ambiguous_neighborhood: bool = False


@dataclass
class StoredListing:
    """Full listing as stored in the database."""

    id: int
    dedupe_id: str
    source: str
    source_id: Optional[str]
    url: str
    title: str
    raw_location_text: str
    neighborhood_match: Optional[str]
    neighborhood_confidence: float
    price_total_eur: Optional[float]
    price_quality: str
    price_includes_service_costs: bool
    gwl_included: bool
    area_m2: Optional[float]
    bedrooms: Optional[int]
    property_type: Optional[str]
    available_from: Optional[str]
    description_snippet: str
    images_hash: Optional[str]
    first_seen_at: str
    last_seen_at: str
    last_changed_at: str
    status: str
    change_log: str
    ambiguous_neighborhood: bool
    missing_runs: int = 0


@dataclass
class ChangeRecord:
    """Record of a change detected in a listing."""

    change_type: ChangeType
    listing: StoredListing
    changes: dict = field(default_factory=dict)  # field -> (old, new)
