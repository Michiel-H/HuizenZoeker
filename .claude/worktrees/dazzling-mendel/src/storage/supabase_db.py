"""Supabase (PostgreSQL) database layer for Amsterdam Rental Monitor.

Drop-in replacement for the SQLite layer. Activated when SUPABASE_DB_URL
is set in the environment. Uses psycopg2 to connect directly to Supabase's
PostgreSQL database.
"""

from __future__ import annotations

import json
import logging
from contextlib import contextmanager
from datetime import datetime
from typing import Optional

import psycopg2
import psycopg2.extras

from src.models import StoredListing

logger = logging.getLogger(__name__)

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS listings (
    id SERIAL PRIMARY KEY,
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
    price_includes_service_costs BOOLEAN NOT NULL DEFAULT FALSE,
    gwl_included BOOLEAN NOT NULL DEFAULT FALSE,
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
    ambiguous_neighborhood BOOLEAN NOT NULL DEFAULT FALSE,
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
    id SERIAL PRIMARY KEY,
    sent_date TEXT NOT NULL UNIQUE,
    sent_at TEXT NOT NULL,
    new_count INTEGER NOT NULL DEFAULT 0,
    changed_count INTEGER NOT NULL DEFAULT 0,
    removed_count INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS run_log (
    id SERIAL PRIMARY KEY,
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
def get_pg(db_url: str):
    """Get a PostgreSQL connection as a context manager."""
    conn = psycopg2.connect(db_url)
    conn.autocommit = False
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_pg(db_url: str) -> None:
    """Initialize the PostgreSQL schema."""
    with get_pg(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(SCHEMA_SQL)


def _row_to_dict(cur, row) -> dict:
    """Convert a psycopg2 row to a dict."""
    if row is None:
        return None
    cols = [desc[0] for desc in cur.description]
    return dict(zip(cols, row))


def row_to_stored_listing(d: dict) -> StoredListing:
    """Convert a dict row to a StoredListing."""
    return StoredListing(
        id=d["id"],
        dedupe_id=d["dedupe_id"],
        source=d["source"],
        source_id=d["source_id"],
        url=d["url"],
        title=d["title"],
        raw_location_text=d["raw_location_text"],
        neighborhood_match=d["neighborhood_match"],
        neighborhood_confidence=d["neighborhood_confidence"],
        price_total_eur=d["price_total_eur"],
        price_quality=d["price_quality"],
        price_includes_service_costs=bool(d["price_includes_service_costs"]),
        gwl_included=bool(d["gwl_included"]),
        area_m2=d["area_m2"],
        bedrooms=d["bedrooms"],
        property_type=d["property_type"],
        available_from=d["available_from"],
        description_snippet=d["description_snippet"],
        images_hash=d["images_hash"],
        first_seen_at=d["first_seen_at"],
        last_seen_at=d["last_seen_at"],
        last_changed_at=d["last_changed_at"],
        status=d["status"],
        change_log=d["change_log"],
        ambiguous_neighborhood=bool(d["ambiguous_neighborhood"]),
        missing_runs=d["missing_runs"],
    )


def _fetch_one(cur) -> dict | None:
    row = cur.fetchone()
    if row is None:
        return None
    return _row_to_dict(cur, row)


def _fetch_all(cur) -> list[dict]:
    rows = cur.fetchall()
    return [_row_to_dict(cur, r) for r in rows]


def upsert_listing_pg(
    conn,
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
    cur = conn.cursor()

    # Check if listing exists by source + source_id
    existing = None
    if source_id:
        cur.execute(
            "SELECT * FROM listings WHERE source = %s AND source_id = %s",
            (source, source_id),
        )
        row = _fetch_one(cur)
        if row:
            existing = row_to_stored_listing(row)

    if existing is None:
        cur.execute(
            "SELECT * FROM listings WHERE dedupe_id = %s AND source = %s",
            (dedupe_id, source),
        )
        row = _fetch_one(cur)
        if row:
            existing = row_to_stored_listing(row)

    if existing is None:
        cur.execute(
            """INSERT INTO listings (
                dedupe_id, source, source_id, url, title, raw_location_text,
                neighborhood_match, neighborhood_confidence, price_total_eur,
                price_quality, price_includes_service_costs, gwl_included,
                area_m2, bedrooms, property_type, available_from,
                description_snippet, images_hash, first_seen_at, last_seen_at,
                last_changed_at, status, change_log, ambiguous_neighborhood,
                missing_runs
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'ACTIVE','[]',%s,0)""",
            (
                dedupe_id, source, source_id, url, title, raw_location_text,
                neighborhood_match, neighborhood_confidence, price_total_eur,
                price_quality, price_includes_service_costs, gwl_included,
                area_m2, bedrooms, property_type, available_from,
                description_snippet, images_hash, now, now, now,
                ambiguous_neighborhood,
            ),
        )
        return "NEW", {}
    else:
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

        change_log = json.loads(existing.change_log) if existing.change_log else []
        change_type = "CHANGED" if changes else "UNCHANGED"

        if changes:
            change_log.append({
                "timestamp": now,
                "changes": {k: {"old": str(v[0]), "new": str(v[1])} for k, v in changes.items()},
            })

        new_status = "ACTIVE"
        if existing.status == "REMOVED" and not changes:
            change_type = "REACTIVATED"

        cur.execute(
            """UPDATE listings SET
                dedupe_id=%s, url=%s, title=%s, raw_location_text=%s,
                neighborhood_match=%s, neighborhood_confidence=%s,
                price_total_eur=%s, price_quality=%s,
                price_includes_service_costs=%s, gwl_included=%s,
                area_m2=%s, bedrooms=%s, property_type=%s,
                available_from=%s, description_snippet=%s, images_hash=%s,
                last_seen_at=%s, last_changed_at=%s,
                status=%s, change_log=%s, ambiguous_neighborhood=%s,
                missing_runs=0
            WHERE id=%s""",
            (
                dedupe_id, url, title or existing.title,
                raw_location_text or existing.raw_location_text,
                neighborhood_match, neighborhood_confidence,
                price_total_eur if price_total_eur is not None else existing.price_total_eur,
                price_quality,
                price_includes_service_costs, gwl_included,
                area_m2 if area_m2 is not None else existing.area_m2,
                bedrooms if bedrooms is not None else existing.bedrooms,
                property_type or existing.property_type,
                available_from or existing.available_from,
                description_snippet or existing.description_snippet,
                images_hash or existing.images_hash,
                now, now if changes else existing.last_changed_at,
                new_status, json.dumps(change_log),
                ambiguous_neighborhood,
                existing.id,
            ),
        )
        return change_type, changes


def mark_missing_pg(conn, source: str, seen_source_ids: set[str]) -> list[StoredListing]:
    """Increment missing_runs for listings not seen in this run."""
    from src.config import REMOVED_AFTER_MISSING_RUNS

    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM listings WHERE source = %s AND status = 'ACTIVE'",
        (source,),
    )
    rows = _fetch_all(cur)

    removed = []
    for d in rows:
        listing = row_to_stored_listing(d)
        sid = listing.source_id or listing.url
        if sid not in seen_source_ids:
            new_missing = listing.missing_runs + 1
            if new_missing >= REMOVED_AFTER_MISSING_RUNS:
                cur.execute(
                    "UPDATE listings SET status='REMOVED', missing_runs=%s, last_changed_at=%s WHERE id=%s",
                    (new_missing, datetime.utcnow().isoformat(), listing.id),
                )
                listing.status = "REMOVED"
                removed.append(listing)
            else:
                cur.execute(
                    "UPDATE listings SET missing_runs=%s WHERE id=%s",
                    (new_missing, listing.id),
                )
    return removed


def get_listings_pg(
    conn,
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
        query += " AND status = %s"
        params.append(status)
    if neighborhood:
        query += " AND neighborhood_match = %s"
        params.append(neighborhood)
    if min_price is not None:
        query += " AND price_total_eur >= %s"
        params.append(min_price)
    if max_price is not None:
        query += " AND price_total_eur <= %s"
        params.append(max_price)
    if source:
        query += " AND source = %s"
        params.append(source)
    if since:
        query += " AND last_seen_at >= %s"
        params.append(since)

    query += " ORDER BY last_changed_at DESC"
    cur = conn.cursor()
    cur.execute(query, params)
    return [row_to_stored_listing(d) for d in _fetch_all(cur)]


def get_daily_changes_pg(conn, since: str) -> dict[str, list[StoredListing]]:
    """Get NEW, CHANGED, and REMOVED listings since a timestamp."""
    result: dict[str, list[StoredListing]] = {"NEW": [], "CHANGED": [], "REMOVED": []}
    cur = conn.cursor()

    cur.execute(
        "SELECT * FROM listings WHERE first_seen_at >= %s AND status = 'ACTIVE' ORDER BY price_total_eur ASC",
        (since,),
    )
    result["NEW"] = [row_to_stored_listing(d) for d in _fetch_all(cur)]

    cur.execute(
        "SELECT * FROM listings WHERE last_changed_at >= %s AND first_seen_at < %s AND status = 'ACTIVE' ORDER BY last_changed_at DESC",
        (since, since),
    )
    result["CHANGED"] = [row_to_stored_listing(d) for d in _fetch_all(cur)]

    cur.execute(
        "SELECT * FROM listings WHERE status = 'REMOVED' AND last_changed_at >= %s ORDER BY last_changed_at DESC",
        (since,),
    )
    result["REMOVED"] = [row_to_stored_listing(d) for d in _fetch_all(cur)]

    return result


def was_email_sent_today_pg(conn, today_str: str) -> bool:
    cur = conn.cursor()
    cur.execute("SELECT id FROM email_log WHERE sent_date = %s", (today_str,))
    return cur.fetchone() is not None


def log_email_sent_pg(conn, today_str: str, new_count: int, changed_count: int, removed_count: int) -> None:
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO email_log (sent_date, sent_at, new_count, changed_count, removed_count) VALUES (%s,%s,%s,%s,%s)",
        (today_str, datetime.utcnow().isoformat(), new_count, changed_count, removed_count),
    )


def log_run_pg(conn, source: str, fetched: int, kept: int, filtered: int, new_count: int, changed_count: int, removed_count: int, errors: str) -> None:
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO run_log (run_at, source, fetched, kept, filtered, new_count, changed_count, removed_count, errors) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (datetime.utcnow().isoformat(), source, fetched, kept, filtered, new_count, changed_count, removed_count, errors),
    )


def get_sources_pg(conn) -> list[str]:
    """Get distinct sources."""
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT source FROM listings ORDER BY source")
    return [r[0] for r in cur.fetchall()]
