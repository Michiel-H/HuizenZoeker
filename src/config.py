"""Central configuration for Amsterdam Rental Monitor."""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# Paths
ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR / "data"
DB_PATH = DATA_DIR / "rentals.db"
STATE_PATH = DATA_DIR / "state.json"

# Ensure data directory exists
DATA_DIR.mkdir(exist_ok=True)

# Email
GMAIL_ADDRESS = os.getenv("GMAIL_ADDRESS", "")
GMAIL_APP_PASSWORD = os.getenv("GMAIL_APP_PASSWORD", "")
TO_EMAIL = os.getenv("TO_EMAIL", GMAIL_ADDRESS)

# Timezone
TIMEZONE = os.getenv("TIMEZONE", "Europe/Amsterdam")

# Scraping
REQUEST_DELAY_SEC = 1.0  # min delay between requests to same domain
REQUEST_TIMEOUT_SEC = 30
MAX_RETRIES = 3
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# Filtering
MAX_PRICE_EUR = 2500

# Target neighborhoods with synonyms/variants for matching
TARGET_NEIGHBORHOODS: dict[str, list[str]] = {
    "De Baarsjes": [
        "de baarsjes",
        "baarsjes",
        "amsterdam-west de baarsjes",
        "amsterdam west de baarsjes",
    ],
    "Centrum": [
        "centrum",
        "amsterdam centrum",
        "amsterdam-centrum",
        "binnenstad",
    ],
    "Houthavens": [
        "houthavens",
        "houthaven",
    ],
    "Oud-West": [
        "oud-west",
        "oud west",
        "oudwest",
        "amsterdam oud-west",
        "amsterdam oud west",
    ],
    "Oud-Zuid": [
        "oud-zuid",
        "oud zuid",
        "oudzuid",
        "amsterdam oud-zuid",
        "amsterdam oud zuid",
        "amsterdam-zuid oud-zuid",
    ],
    "De Pijp": [
        "de pijp",
        "pijp",
        "depijp",
    ],
    "Plantagebuurt": [
        "plantagebuurt",
        "plantage",
        "plantage buurt",
    ],
    "Rivierenbuurt": [
        "rivierenbuurt",
        "rivieren buurt",
    ],
    "Schinkelbuurt": [
        "schinkelbuurt",
        "schinkel buurt",
        "schinkel",
    ],
    "Weesperzijde": [
        "weesperzijde",
    ],
    "Westerpark": [
        "westerpark",
        "wester park",
    ],
}

# Deduplication thresholds
DEDUPE_PRICE_TOLERANCE_EUR = 50
DEDUPE_AREA_TOLERANCE_M2 = 5
DEDUPE_TITLE_SIMILARITY_THRESHOLD = 0.75
DEDUPE_COMBINED_THRESHOLD = 0.70

# Change detection
REMOVED_AFTER_MISSING_RUNS = 2
