"""Price normalization for Amsterdam rental listings."""

from __future__ import annotations

import re
from dataclasses import dataclass

from src.models import PriceQuality


@dataclass
class PriceResult:
    """Result of price normalization."""

    total_eur: float | None
    quality: PriceQuality
    includes_service_costs: bool
    gwl_included: bool


def parse_price(
    price_raw: float | None,
    service_costs_raw: float | None = None,
    price_text: str = "",
    includes_service_costs: bool = False,
    gwl_included: bool = False,
) -> PriceResult:
    """Normalize price information.

    Rules:
    - If service costs are explicitly given, add them to the base price.
    - If the source says "incl. servicekosten", mark as CONFIRMED.
    - If unclear, mark as UNKNOWN.
    - GWL is tracked but not added to the price.
    """
    if price_raw is None:
        # Try to extract from text
        price_raw = extract_price_from_text(price_text)

    if price_raw is None:
        return PriceResult(
            total_eur=None,
            quality=PriceQuality.UNKNOWN,
            includes_service_costs=False,
            gwl_included=gwl_included,
        )

    total = price_raw

    # Check text for service cost indicators
    text_lower = price_text.lower() if price_text else ""

    # Detect GWL inclusion
    gwl_patterns = [
        r"incl\.?\s*g\s*/?\s*w\s*/?\s*l",
        r"inclusief\s+gas",
        r"inclusief\s+g/w/l",
        r"all[\s-]?in",
    ]
    for pat in gwl_patterns:
        if re.search(pat, text_lower):
            gwl_included = True

    # Detect service costs
    service_cost_patterns = [
        r"incl\.?\s*service",
        r"inclusief\s+service",
        r"servicekosten\s+inbegrepen",
    ]
    excl_service_patterns = [
        r"excl\.?\s*servicekosten\s*:?\s*€?\s*(\d+)",
        r"exclusief\s+servicekosten\s*:?\s*€?\s*(\d+)",
        r"servicekosten\s*:?\s*€?\s*(\d+)",
        r"\+\s*€?\s*(\d+)\s*service",
    ]

    if service_costs_raw is not None and service_costs_raw > 0:
        total += service_costs_raw
        quality = PriceQuality.CONFIRMED
        includes_service_costs = True
    elif includes_service_costs:
        quality = PriceQuality.CONFIRMED
    else:
        # Check text for clues
        quality = PriceQuality.UNKNOWN

        for pat in service_cost_patterns:
            if re.search(pat, text_lower):
                quality = PriceQuality.CONFIRMED
                includes_service_costs = True
                break

        if quality == PriceQuality.UNKNOWN:
            for pat in excl_service_patterns:
                match = re.search(pat, text_lower)
                if match:
                    # Try to extract service cost amount
                    groups = match.groups()
                    if groups and groups[0]:
                        try:
                            sc = float(groups[0])
                            total += sc
                            quality = PriceQuality.CONFIRMED
                            includes_service_costs = True
                        except ValueError:
                            pass
                    break

    return PriceResult(
        total_eur=total,
        quality=quality,
        includes_service_costs=includes_service_costs,
        gwl_included=gwl_included,
    )


def extract_price_from_text(text: str) -> float | None:
    """Extract a monthly rent price from free text."""
    if not text:
        return None

    # Common patterns: "€ 1.500", "€1500", "1.500 p/m", "1,500.00"
    patterns = [
        r"€\s*([\d.,]+)",
        r"eur\s*([\d.,]+)",
        r"([\d.,]+)\s*(?:p/?m|per\s+maand|/\s*mnd|/\s*maand)",
    ]

    for pattern in patterns:
        match = re.search(pattern, text.lower())
        if match:
            price_str = match.group(1).strip()
            return _parse_price_string(price_str)

    return None


def _parse_price_string(s: str) -> float | None:
    """Parse a price string like '1.500', '1,500.00', '1500' to float."""
    if not s:
        return None

    # Remove any trailing dots/commas
    s = s.strip().rstrip(".,")

    # Determine decimal separator
    # "1.500" -> 1500 (Dutch: dot is thousands separator)
    # "1,500.00" -> 1500.00 (English format)
    # "1.500,00" -> 1500.00 (Dutch format with decimals)

    has_dot = "." in s
    has_comma = "," in s

    if has_dot and has_comma:
        if s.rfind(",") > s.rfind("."):
            # Comma is decimal separator: "1.500,00"
            s = s.replace(".", "").replace(",", ".")
        else:
            # Dot is decimal separator: "1,500.00"
            s = s.replace(",", "")
    elif has_dot:
        # Could be "1.500" (thousands) or "1500.00" (decimal)
        parts = s.split(".")
        if len(parts) == 2 and len(parts[1]) == 3:
            # "1.500" -> thousands separator
            s = s.replace(".", "")
        # else: "1500.00" -> already fine
    elif has_comma:
        parts = s.split(",")
        if len(parts) == 2 and len(parts[1]) == 3:
            # "1,500" -> thousands separator
            s = s.replace(",", "")
        else:
            # "1500,00" -> decimal separator
            s = s.replace(",", ".")

    try:
        val = float(s)
        # Sanity check: rent should be between 100 and 50000
        if 100 <= val <= 50000:
            return val
        return None
    except ValueError:
        return None
