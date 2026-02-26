#!/usr/bin/env python3
"""Entry point: check if 08:00 Amsterdam time and send daily digest if needed."""

import logging
import sys
from datetime import datetime, timedelta

from zoneinfo import ZoneInfo

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger(__name__)


def main():
    from src.config import TIMEZONE
    from src.storage.database import (
        get_daily_changes,
        get_db,
        init_db,
        log_email_sent,
        was_email_sent_today,
    )
    from src.notifier.email_sender import send_daily_digest

    init_db()

    tz = ZoneInfo(TIMEZONE)
    now_local = datetime.now(tz)
    today_str = now_local.strftime("%Y-%m-%d")
    current_hour = now_local.hour

    logger.info(f"Local time ({TIMEZONE}): {now_local.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Current hour: {current_hour}")

    # Only send at 08:00 local time (allow 07:30 - 08:30 window for cron flexibility)
    if not (7 <= current_hour <= 8):
        logger.info(f"Not email time (hour={current_hour}). Skipping digest.")
        return

    with get_db() as conn:
        if was_email_sent_today(conn, today_str):
            logger.info(f"Email already sent today ({today_str}). Skipping.")
            return

        # Get changes since yesterday
        yesterday = (now_local - timedelta(days=1)).strftime("%Y-%m-%dT00:00:00")
        changes = get_daily_changes(conn, yesterday)

        n_new = len(changes["NEW"])
        n_changed = len(changes["CHANGED"])
        n_removed = len(changes["REMOVED"])

        logger.info(f"Daily changes: {n_new} new, {n_changed} changed, {n_removed} removed")

        success = send_daily_digest(
            date_str=today_str,
            new_listings=changes["NEW"],
            changed_listings=changes["CHANGED"],
            removed_listings=changes["REMOVED"],
        )

        if success:
            log_email_sent(conn, today_str, n_new, n_changed, n_removed)
            logger.info("Daily digest sent and logged.")
        else:
            logger.error("Failed to send daily digest.")


if __name__ == "__main__":
    main()
