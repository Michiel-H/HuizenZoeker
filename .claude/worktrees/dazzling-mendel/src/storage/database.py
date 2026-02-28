"""SQLite database layer for Amsterdam Rental Monitor."""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from typing import Optional

from src.config import DB_PATH
from src.models import StoredListing

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS listings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    dedupe_id TEXT NOT NULL,
    source TEXT NOT NULL,
    source_id TEXT,
    url TEXT NOT NULL,
    title TEXT NOT NULL DEFAULT '',
    raw_location_text TEXT NOT NULL DEFAULT '',
    neighborhood_match TEXT,
    neighborhood_confidence REAL NOT NULL DEFAULT 0.0,
    price_total_eur REAL,
    price_quality TEXT NOT NULL DEFAULT 'UNKNOWN',
    price_includes_service_costs INTEGER NOT NULL DEFAULT 0,
    gwl_included INTEGER NOT NULL DEFAULT 0,
    area_m2 REAL,
    bedrooms INTEGER,
    property_type TEXT,
    available_from TEXT,
    description_snippet TEXT NOT NULL DEFAULT '',
    images_hash TEXT,
    first_seen_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    last_changed_at TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'ACTIVE',
    change_log TEXT NOT NULL DEFAULT '[]',
    ambiguous_neighborhood INTEGER NOT NULL DEFAULT 0,
    missing_runs INTEGER NOT NULL DEFAULT 0,
    UNIQUE(source, source_id)
);

CREATE INDEX IF NOT EXISTS idx_listings_dedupe_id ON listings(dedupe_id);
CREATE INDEX IF NOT EXISTS idx_listings_status ON listings(status);
CREATE INDEX IF NOT EXISTS idx_listings_neighborhood ON listings(neighborhood_match);
CREATE INDEX IF NOT EXISTS idx_listings_price ON listings(price_total_eur);
CREATE INDEX IF NOT EXISTS idx_listings_source ON listings(source);
CREATE INDEX IF NOT EXISTS idx_listings_last_seen ON listings(last_seen_at);

CREATE TABLE IF NOT EXISTS email_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sent_date TEXT NOT NULL UNIQUE,
    sent_at TEXT NOT NULL,
    new_count INTEGER NOT NULL DEFAULT 0,
    changed_count INTEGER NOT NULL DEFAULT 0,
    removed_count INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS run_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_at TEXT NOT NULL,
    source TEXT NOT NULL,
    fetched INTEGER NOT NULL DEFAULT 0,
    kept INTEGER NOT NULL DEFAULT 0,
    filtered INTEGER NOT NULL DEFAULT 0,
    new_count INTEGER NOT NULL DEFAULT 0,
    changed_count INTEGER NOT NULL DEFAULT 0,
    removed_count INTEGER NOT NULL DEFAULT 0,
    errors TEXT NOT NULL DEFAULT ''
);
"""


@contextmanager
def get_db(db_path: str | None = None):
    """Get a database connection as a context manager."""
    path = db_path or str(DB_PATH)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db(db_path: str | None = None) -> None:
    """Initialize the database schema."""
    with get_db(db_path) as conn:
        conn.executescript(SCHEMA_SQL)


def row_to_stored_listing(row: sqlite3.Row) -> StoredListing:
    """Convert a database row to a StoredListing."""
    return StoredListing(
        id=row["id"],
        dedupe_id=row["dedupe_id"],
        source=row["source"],
        source_id=row["source_id"],
        url=row["url"],
        title=row["title"],
        raw_location_text=row["raw_location_text"],
        neighborhood_match=row["neighborhood_match"],
        neighborhood_confidence=row["neighborhood_confidence"],
        price_total_eur=row["price_total_eur"],
        price_quality=row["price_quality"],
        price_includes_service_costs=bool(row["price_includes_service_costs"]),
        gwl_included=bool(row["gwl_included"]),
        area_m2=row["area_m2"],
        bedrooms=row["bedrooms"],
        property_type=row["property_type"],
        available_from=row["available_from"],
        description_snippet=row["description_snippet"],
        images_hash=row["images_hash"],
        first_seen_at=row["first_seen_at"],
        last_seen_at=row["last_seen_at"],
        last_changed_at=row["last_changed_at"],
        status=row["status"],
        change_log=row["change_log"],
        ambiguous_neighborhood=bool(row["ambiguous_neighborhood"]),
        missing_runs=row["missing_runs"],
    )


def upsert_listing(
    conn: sqlite3.Connection,
    dedupe_id: str,
    source: str,
    source_id: Optional[str],
    url: str,
    title: str,
    raw_location_text: str,
    neighborhood_match: Optional[str],
    neighborhood_confidence: float,
    price_total_eur: Optional[float],
    price_quality: str,
    price_includes_service_costs: bool,
    gwl_included: bool,
    area_m2: Optional[float],
    bedrooms: Optional[int],
    property_type: Optional[str],
    available_from: Optional[str],
    description_snippet: str,
    images_hash: Optional[str],
    ambiguous_neighborhood: bool,
) -> tuple[str, dict]:
    """Insert or update a listing. Returns (change_type, changes_dict)."""
    now = datetime.utcnow().isoformat()

    # Check if listing exists by source + source_id
    existing = None
    if source_id:
        row = conn.execute(
            "SELECT * FROM listings WHERE source = ? AND source_id = ?",
            (source, source_id),
        ).fetchone()
        if row:
            existing = row_to_stored_listing(row)

    if existing is None:
        # Check by dedupe_id + source
        row = conn.execute(
            "SELECT * FROM listings WHERE dedupe_id = ? AND source = ?",
            (dedupe_id, source),
        ).fetchone()
        if row:
            existing = row_to_stored_listing(row)

    if existing is None:
        # New listing
        conn.execute(
            """INSERT INTO listings (
                dedupe_id, source, source_id, url, title, raw_location_text,
                neighborhood_match, neighborhood_confidence, price_total_eur,
                price_quality, price_includes_service_costs, gwl_included,
                area_m2, bedrooms, property_type, available_from,
                description_snippet, images_hash, first_seen_at, last_seen_at,
                last_changed_at, status, change_log, ambiguous_neighborhood,
                missing_runs
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'ACTIVE', '[]', ?, 0)""",
            (
                dedupe_id, source, source_id, url, title, raw_location_text,
                neighborhood_match, neighborhood_confidence, price_total_eur,
                price_quality, int(price_includes_service_costs), int(gwl_included),
                area_m2, bedrooms, property_type, available_from,
                description_snippet, images_hash, now, now, now,
                int(ambiguous_neighborhood),
            ),
        )
        return "NEW", {}
    else:
        # Existing listing - check for changes
        changes = {}
        if price_total_eur is not None and existing.price_total_eur is not None:
            if abs(price_total_eur - existing.price_total_eur) > 0.01:
                changes["price_total_eur"] = (existing.price_total_eur, price_total_eur)
        elif price_total_eur != existing.price_total_eur:
            changes["price_total_eur"] = (existing.price_total_eur, price_total_eur)

        if title != existing.title and title:
            changes["title"] = (existing.title, title)
        if description_snippet != existing.description_snippet and description_snippet:
            changes["description_snippet"] = (
                existing.description_snippet[:100],
                description_snippet[:100],
            )

        # Update change_log if there are changes
        change_log = json.loads(existing.change_log) if existing.change_log else []
        change_type = "CHANGED" if changes else "UNCHANGED"

        if changes:
            change_log.append({
                "timestamp": now,
                "changes": {k: {"old": str(v[0]), "new": str(v[1])} for k, v in changes.items()},
            })

        # Re-activate if was removed
        new_status = "ACTIVE"
        if existing.status == "REMOVED" and not changes:
            change_type = "REACTIVATED"

        conn.execute(
            """UPDATE listings SET
                dedupe_id = ?, url = ?, title = ?, raw_location_text = ?,
                neighborhood_match = ?, neighborhood_confidence = ?,
                price_total_eur = ?, price_quality = ?,
                price_includes_service_costs = ?, gwl_included = ?,
                area_m2 = ?, bedrooms = ?, property_type = ?,
                available_from = ?, description_snippet = ?, images_hash = ?,
                last_seen_at = ?, last_changed_at = ?,
                status = ?, change_log = ?, ambiguous_neighborhood = ?,
                missing_runs = 0
            WHERE id = ?""",
            (
                dedupe_id, url, title or existing.title,
                raw_location_text or existing.raw_location_text,
                neighborhood_match, neighborhood_confidence,
                price_total_eur if price_total_eur is not None else existing.price_total_eur,
                price_quality,
                int(price_includes_service_costs), int(gwl_included),
                area_m2 if area_m2 is not None else existing.area_m2,
                bedrooms if bedrooms is not None else existing.bedrooms,
                property_type or existing.property_type,
                available_from or existing.available_from,
                description_snippet or existing.description_snippet,
                images_hash or existing.images_hash,
                now, now if changes else existing.last_changed_at,
                new_status, json.dumps(change_log),
                int(ambiguous_neighborhood),
                existing.id,
            ),
        )
        return change_type, changes


def mark_missing(conn: sqlite3.Connection, source: str, seen_source_ids: set[str]) -> list[StoredListing]:
    """Increment missing_runs for listings not seen in this run. Return newly removed."""
    from src.config import REMOVED_AFTER_MISSING_RUNS

    # Get all active listings for this source
    rows = conn.execute(
        "SELECT * FROM listings WHERE source = ? AND status = 'ACTIVE'",
        (source,),
    ).fetchall()

    removed = []
    for row in rows:
        listing = row_to_stored_listing(row)
        sid = listing.source_id or listing.url
        if sid not in seen_source_ids:
            new_missing = listing.missing_runs + 1
            if new_missing >= REMOVED_AFTER_MISSING_RUNS:
                conn.execute(
                    "UPDATE listings SET status = 'REMOVED', missing_runs = ?, last_changed_at = ? WHERE id = ?",
                    (new_missing, datetime.utcnow().isoformat(), listing.id),
                )
                listing.status = "REMOVED"
                removed.append(listing)
            else:
                conn.execute(
                    "UPDATE listings SET missing_runs = ? WHERE id = ?",
                    (new_missing, listing.id),
                )
    return removed


def get_listings(
    conn: sqlite3.Connection,
    status: Optional[str] = None,
    neighborhood: Optional[str] = None,
    min_price: Optional[float] = None,
    max_price: Optional[float] = None,
    source: Optional[str] = None,
    since: Optional[str] = None,
) -> list[StoredListing]:
    """Query listings with optional filters."""
    query = "SELECT * FROM listings WHERE 1=1"
    params: list = []

    if status:
        query += " AND status = ?"
        params.append(status)
    if neighborhood:
        query += " AND neighborhood_match = ?"
        params.append(neighborhood)
    if min_price is not None:
        query += " AND price_total_eur >= ?"
        params.append(min_price)
    if max_price is not None:
        query += " AND price_total_eur <= ?"
        params.append(max_price)
    if source:
        query += " AND source = ?"
        params.append(source)
    if since:
        query += " AND last_seen_at >= ?"
        params.append(since)

    query += " ORDER BY last_changed_at DESC"
    rows = conn.execute(query, params).fetchall()
    return [row_to_stored_listing(r) for r in rows]


def get_daily_changes(
    conn: sqlite3.Connection,
    since: str,
) -> dict[str, list[StoredListing]]:
    """Get NEW, CHANGED, and REMOVED listings since a timestamp."""
    result: dict[str, list[StoredListing]] = {
        "NEW": [],
        "CHANGED": [],
        "REMOVED": [],
    }

    # NEW: first_seen_at >= since and status = ACTIVE
    rows = conn.execute(
        "SELECT * FROM listings WHERE first_seen_at >= ? AND status = 'ACTIVE' ORDER BY price_total_eur ASC",
        (since,),
    ).fetchall()
    result["NEW"] = [row_to_stored_listing(r) for r in rows]

    # CHANGED: last_changed_at >= since, first_seen_at < since, status = ACTIVE
    rows = conn.execute(
        """SELECT * FROM listings
           WHERE last_changed_at >= ? AND first_seen_at < ? AND status = 'ACTIVE'
           ORDER BY last_changed_at DESC""",
        (since, since),
    ).fetchall()
    result["CHANGED"] = [row_to_stored_listing(r) for r in rows]

    # REMOVED: status = REMOVED and last_changed_at >= since
    rows = conn.execute(
        """SELECT * FROM listings
           WHERE status = 'REMOVED' AND last_changed_at >= ?
           ORDER BY last_changed_at DESC""",
        (since,),
    ).fetchall()
    result["REMOVED"] = [row_to_stored_listing(r) for r in rows]

    return result


def was_email_sent_today(conn: sqlite3.Connection, today_str: str) -> bool:
    """Check if an email was already sent today."""
    row = conn.execute(
        "SELECT id FROM email_log WHERE sent_date = ?", (today_str,)
    ).fetchone()
    return row is not None


def log_email_sent(
    conn: sqlite3.Connection,
    today_str: str,
    new_count: int,
    changed_count: int,
    removed_count: int,
) -> None:
    """Record that an email was sent today."""
    conn.execute(
        "INSERT INTO email_log (sent_date, sent_at, new_count, changed_count, removed_count) VALUES (?, ?, ?, ?, ?)",
        (today_str, datetime.utcnow().isoformat(), new_count, changed_count, removed_count),
    )


def log_run(
    conn: sqlite3.Connection,
    source: str,
    fetched: int,
    kept: int,
    filtered: int,
    new_count: int,
    changed_count: int,
    removed_count: int,
    errors: str,
) -> None:
    """Log a scraping run."""
    conn.execute(
        """INSERT INTO run_log
           (run_at, source, fetched, kept, filtered, new_count, changed_count, removed_count, errors)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (datetime.utcnow().isoformat(), source, fetched, kept, filtered, new_count, changed_count, removed_count, errors),
    )
