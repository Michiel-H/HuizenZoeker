"""Tests for deduplication scoring."""

import pytest
from src.dedupe.engine import compute_dedupe_score, generate_dedupe_id
from src.models import NormalizedListing, PriceQuality


def _make_listing(**kwargs) -> NormalizedListing:
    defaults = dict(
        source="TestSource",
        source_id="123",
        url="https://example.com/listing/123",
        title="Nice apartment in De Pijp",
        raw_location_text="De Pijp, Amsterdam",
        neighborhood_match="De Pijp",
        neighborhood_confidence=0.9,
        price_total_eur=1500.0,
        price_quality=PriceQuality.CONFIRMED,
        price_includes_service_costs=True,
        gwl_included=False,
        area_m2=65.0,
        bedrooms=2,
        property_type="apartment",
        available_from="2024-03-01",
        description_snippet="Beautiful apartment with balcony",
        images_hash=None,
        ambiguous_neighborhood=False,
    )
    defaults.update(kwargs)
    return NormalizedListing(**defaults)


class TestDedupeScore:
    def test_identical_listings(self):
        a = _make_listing()
        b = _make_listing(source="OtherSource", source_id="456",
                         url="https://other.com/456")
        score = compute_dedupe_score(a, b)
        assert score.combined > 0.7

    def test_different_listings(self):
        a = _make_listing(
            title="Apartment in Centrum", price_total_eur=2000.0,
            neighborhood_match="Centrum", area_m2=45.0,
            url="https://site-a.com/111",
            raw_location_text="Centrum, Amsterdam",
        )
        b = _make_listing(
            title="Studio in De Pijp", price_total_eur=900.0,
            neighborhood_match="De Pijp", area_m2=28.0,
            source="OtherSource",
            url="https://site-b.com/222",
            raw_location_text="De Pijp, Amsterdam",
        )
        score = compute_dedupe_score(a, b)
        assert score.combined < 0.5

    def test_same_images_high_score(self):
        a = _make_listing(images_hash="abc123")
        b = _make_listing(source="OtherSource", images_hash="abc123")
        score = compute_dedupe_score(a, b)
        assert score.combined >= 0.85

    def test_same_url_high_score(self):
        a = _make_listing(url="https://example.com/listing/123")
        b = _make_listing(source="OtherSource", url="https://example.com/listing/123")
        score = compute_dedupe_score(a, b)
        assert score.combined >= 0.90

    def test_similar_price_boosts_score(self):
        a = _make_listing(price_total_eur=1500.0)
        b = _make_listing(source="OtherSource", price_total_eur=1520.0)
        score = compute_dedupe_score(a, b)
        assert score.price_sim > 0.5

    def test_very_different_price(self):
        a = _make_listing(price_total_eur=1500.0)
        b = _make_listing(source="OtherSource", price_total_eur=2100.0)
        score = compute_dedupe_score(a, b)
        assert score.price_sim == 0.0

    def test_generate_dedupe_id_unique(self):
        id1 = generate_dedupe_id()
        id2 = generate_dedupe_id()
        assert id1 != id2
        assert len(id1) == 36  # UUID format

    def test_title_similarity(self):
        a = _make_listing(title="Prachtig appartement Ferdinand Bolstraat 45")
        b = _make_listing(
            source="OtherSource",
            title="Te huur: Appartement Ferdinand Bolstraat 45",
        )
        score = compute_dedupe_score(a, b)
        assert score.title_sim > 0.5
