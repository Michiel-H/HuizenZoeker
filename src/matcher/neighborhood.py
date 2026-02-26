"""Neighborhood matching for Amsterdam rental listings."""

from __future__ import annotations

import re
from dataclasses import dataclass

from src.config import TARGET_NEIGHBORHOODS


@dataclass
class NeighborhoodResult:
    """Result of a neighborhood match."""

    name: str | None  # canonical name or None if no match
    confidence: float  # 0.0 to 1.0
    ambiguous: bool  # True if multiple neighborhoods matched


def _normalize_text(text: str) -> str:
    """Lowercase and collapse whitespace/hyphens for matching."""
    text = text.lower()
    text = re.sub(r"[,.\-/|()]+", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def match_neighborhood(
    title: str = "",
    location_text: str = "",
    description: str = "",
) -> NeighborhoodResult:
    """Match text fields against target neighborhoods.

    Uses a scoring approach:
    - Match in location field: high confidence
    - Match in title: medium-high confidence
    - Match in description: medium confidence
    - Multiple fields matching same neighborhood: boosted confidence
    """
    # Combine for searching, but track where matches come from
    fields = {
        "location": _normalize_text(location_text),
        "title": _normalize_text(title),
        "description": _normalize_text(description),
    }

    # Confidence weights per field
    field_weights = {
        "location": 0.9,
        "title": 0.8,
        "description": 0.6,
    }

    matches: dict[str, float] = {}  # neighborhood_name -> best confidence

    for canonical_name, variants in TARGET_NEIGHBORHOODS.items():
        best_score = 0.0

        for variant in variants:
            norm_variant = _normalize_text(variant)
            # Use word-boundary matching to avoid partial matches
            # e.g., "west" should not match inside "westerpark"
            pattern = r"(?:^|\s|,)" + re.escape(norm_variant) + r"(?:\s|,|$)"

            for field_name, field_text in fields.items():
                if not field_text:
                    continue
                if re.search(pattern, field_text):
                    score = field_weights[field_name]
                    # Bonus for longer variant matches (more specific)
                    if len(norm_variant) > 8:
                        score = min(1.0, score + 0.05)
                    best_score = max(best_score, score)

        if best_score > 0:
            matches[canonical_name] = best_score

    if not matches:
        return NeighborhoodResult(name=None, confidence=0.0, ambiguous=False)

    # Sort by confidence descending
    sorted_matches = sorted(matches.items(), key=lambda x: x[1], reverse=True)
    best_name, best_conf = sorted_matches[0]

    # Handle ambiguity: if two neighborhoods match with similar confidence
    ambiguous = False
    if len(sorted_matches) > 1:
        second_conf = sorted_matches[1][1]
        if best_conf - second_conf < 0.15:
            ambiguous = True

    # Handle overlap cases: "Oud-West" vs "Amsterdam-West (De Baarsjes)"
    # If "De Baarsjes" matches, don't also report "Oud-West"
    # Prefer more specific matches
    if len(sorted_matches) > 1 and best_conf == sorted_matches[1][1]:
        # Equal scores: prefer the one with longer name (more specific)
        if len(sorted_matches[1][0]) > len(best_name):
            best_name = sorted_matches[1][0]

    return NeighborhoodResult(
        name=best_name,
        confidence=best_conf,
        ambiguous=ambiguous,
    )
