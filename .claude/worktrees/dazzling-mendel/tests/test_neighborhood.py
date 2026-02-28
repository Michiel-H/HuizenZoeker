"""Tests for neighborhood matching."""

import pytest
from src.matcher.neighborhood import match_neighborhood


class TestNeighborhoodMatching:
    def test_exact_match_pijp(self):
        result = match_neighborhood(location_text="De Pijp, Amsterdam")
        assert result.name == "De Pijp"
        assert result.confidence > 0.5

    def test_variant_oud_zuid(self):
        result = match_neighborhood(location_text="Amsterdam Oud-Zuid")
        assert result.name == "Oud-Zuid"
        assert result.confidence > 0.5

    def test_variant_oud_west(self):
        result = match_neighborhood(location_text="oud west")
        assert result.name == "Oud-West"
        assert result.confidence > 0.5

    def test_match_in_title(self):
        result = match_neighborhood(title="Beautiful apartment in Westerpark")
        assert result.name == "Westerpark"
        assert result.confidence > 0.5

    def test_match_in_description(self):
        result = match_neighborhood(
            description="This lovely flat is located in the heart of De Baarsjes"
        )
        assert result.name == "De Baarsjes"
        assert result.confidence > 0.3

    def test_no_match(self):
        result = match_neighborhood(
            location_text="Amsterdam Zuidoost",
            title="Nice flat in Bijlmer",
        )
        assert result.name is None
        assert result.confidence == 0.0

    def test_centrum_match(self):
        result = match_neighborhood(location_text="Amsterdam Centrum")
        assert result.name == "Centrum"
        assert result.confidence > 0.5

    def test_houthavens_match(self):
        result = match_neighborhood(location_text="Houthavens, Amsterdam")
        assert result.name == "Houthavens"
        assert result.confidence > 0.5

    def test_rivierenbuurt_match(self):
        result = match_neighborhood(location_text="Rivierenbuurt")
        assert result.name == "Rivierenbuurt"
        assert result.confidence > 0.5

    def test_schinkelbuurt_match(self):
        result = match_neighborhood(title="Apartment Schinkelbuurt")
        assert result.name == "Schinkelbuurt"
        assert result.confidence > 0.5

    def test_plantagebuurt_match(self):
        result = match_neighborhood(location_text="Plantagebuurt")
        assert result.name == "Plantagebuurt"
        assert result.confidence > 0.5

    def test_weesperzijde_match(self):
        result = match_neighborhood(location_text="Weesperzijde, Amsterdam")
        assert result.name == "Weesperzijde"
        assert result.confidence > 0.5

    def test_location_takes_priority(self):
        """Location field should give higher confidence than description."""
        result_loc = match_neighborhood(location_text="De Pijp")
        result_desc = match_neighborhood(description="located near De Pijp")
        assert result_loc.confidence >= result_desc.confidence

    def test_pijp_variant_without_de(self):
        result = match_neighborhood(location_text="Pijp, Amsterdam")
        assert result.name == "De Pijp"
