"""Unified database backend that auto-selects SQLite or Supabase (PostgreSQL).

If SUPABASE_DB_URL is set → uses PostgreSQL via psycopg2.
Otherwise → falls back to local SQLite.

All public functions have the same signatures regardless of backend.
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Optional

from src.models import StoredListing

# Detect backend
SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL", "").strip()
USE_SUPABASE = bool(SUPABASE_DB_URL)


@contextmanager
def get_connection():
    """Get a database connection using the active backend."""
    if USE_SUPABASE:
        from src.storage.supabase_db import get_pg
        with get_pg(SUPABASE_DB_URL) as conn:
            yield conn
    else:
        from src.storage.database import get_db
        with get_db() as conn:
            yield conn


def init():
    """Initialize the database schema."""
    if USE_SUPABASE:
        from src.storage.supabase_db import init_pg
        init_pg(SUPABASE_DB_URL)
    else:
        from src.storage.database import init_db
        init_db()


def upsert_listing(conn, **kwargs) -> tuple[str, dict]:
    if USE_SUPABASE:
        from src.storage.supabase_db import upsert_listing_pg
        return upsert_listing_pg(conn, **kwargs)
    else:
        from src.storage.database import upsert_listing as upsert_sqlite
        return upsert_sqlite(conn, **kwargs)


def mark_missing(conn, source: str, seen_source_ids: set[str]) -> list[StoredListing]:
    if USE_SUPABASE:
        from src.storage.supabase_db import mark_missing_pg
        return mark_missing_pg(conn, source, seen_source_ids)
    else:
        from src.storage.database import mark_missing as mark_sqlite
        return mark_sqlite(conn, source, seen_source_ids)


def get_listings(conn, **kwargs) -> list[StoredListing]:
    if USE_SUPABASE:
        from src.storage.supabase_db import get_listings_pg
        return get_listings_pg(conn, **kwargs)
    else:
        from src.storage.database import get_listings as get_sqlite
        return get_sqlite(conn, **kwargs)


def get_daily_changes(conn, since: str) -> dict[str, list[StoredListing]]:
    if USE_SUPABASE:
        from src.storage.supabase_db import get_daily_changes_pg
        return get_daily_changes_pg(conn, since)
    else:
        from src.storage.database import get_daily_changes as get_sqlite
        return get_sqlite(conn, since)


def was_email_sent_today(conn, today_str: str) -> bool:
    if USE_SUPABASE:
        from src.storage.supabase_db import was_email_sent_today_pg
        return was_email_sent_today_pg(conn, today_str)
    else:
        from src.storage.database import was_email_sent_today as check_sqlite
        return check_sqlite(conn, today_str)


def log_email_sent(conn, today_str: str, new_count: int, changed_count: int, removed_count: int) -> None:
    if USE_SUPABASE:
        from src.storage.supabase_db import log_email_sent_pg
        log_email_sent_pg(conn, today_str, new_count, changed_count, removed_count)
    else:
        from src.storage.database import log_email_sent as log_sqlite
        log_sqlite(conn, today_str, new_count, changed_count, removed_count)


def log_run(conn, **kwargs) -> None:
    if USE_SUPABASE:
        from src.storage.supabase_db import log_run_pg
        log_run_pg(conn, **kwargs)
    else:
        from src.storage.database import log_run as log_sqlite
        log_sqlite(conn, **kwargs)


def get_backend_name() -> str:
    return "Supabase (PostgreSQL)" if USE_SUPABASE else "SQLite (local)"
