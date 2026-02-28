"""Tests for price parsing and normalization."""

import pytest
from src.normalizer.price import parse_price, extract_price_from_text, _parse_price_string
from src.models import PriceQuality


class TestParsePriceString:
    def test_dutch_thousands(self):
        assert _parse_price_string("1.500") == 1500.0

    def test_dutch_with_decimals(self):
        assert _parse_price_string("1.500,00") == 1500.0

    def test_english_format(self):
        assert _parse_price_string("1,500.00") == 1500.0

    def test_plain_number(self):
        assert _parse_price_string("1500") == 1500.0

    def test_small_decimal(self):
        # "1500.50" should be treated as decimal
        assert _parse_price_string("1500.50") == 1500.50

    def test_invalid(self):
        assert _parse_price_string("") is None
        assert _parse_price_string("abc") is None

    def test_out_of_range(self):
        assert _parse_price_string("50") is None  # too low
        assert _parse_price_string("999999") is None  # too high


class TestExtractPriceFromText:
    def test_euro_sign(self):
        assert extract_price_from_text("€ 1.500 per maand") == 1500.0

    def test_euro_no_space(self):
        assert extract_price_from_text("€1500") == 1500.0

    def test_pm_suffix(self):
        assert extract_price_from_text("1.800 p/m") == 1800.0

    def test_per_maand(self):
        assert extract_price_from_text("1.200 per maand") == 1200.0

    def test_no_price(self):
        assert extract_price_from_text("Beautiful apartment in Amsterdam") is None


class TestParsePrice:
    def test_basic_price(self):
        result = parse_price(price_raw=1500.0)
        assert result.total_eur == 1500.0
        assert result.quality == PriceQuality.UNKNOWN

    def test_with_service_costs(self):
        result = parse_price(price_raw=1200.0, service_costs_raw=150.0)
        assert result.total_eur == 1350.0
        assert result.quality == PriceQuality.CONFIRMED
        assert result.includes_service_costs is True

    def test_incl_service_text(self):
        result = parse_price(
            price_raw=1500.0,
            price_text="€1.500 incl. servicekosten",
        )
        assert result.total_eur == 1500.0
        assert result.quality == PriceQuality.CONFIRMED
        assert result.includes_service_costs is True

    def test_excl_service_text_with_amount(self):
        result = parse_price(
            price_raw=1200.0,
            price_text="excl. servicekosten: €150",
        )
        assert result.total_eur == 1350.0
        assert result.quality == PriceQuality.CONFIRMED

    def test_gwl_detection(self):
        result = parse_price(
            price_raw=1500.0,
            price_text="€1.500 incl. g/w/l",
        )
        assert result.gwl_included is True

    def test_no_price(self):
        result = parse_price(price_raw=None)
        assert result.total_eur is None
        assert result.quality == PriceQuality.UNKNOWN

    def test_already_includes_service(self):
        result = parse_price(price_raw=1600.0, includes_service_costs=True)
        assert result.total_eur == 1600.0
        assert result.quality == PriceQuality.CONFIRMED
