#!/usr/bin/env python3
"""Entry point: run the collection pipeline."""

import logging
import sys
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger(__name__)


def main():
    logger.info("=== Amsterdam Rental Monitor — Collection Run ===")
    logger.info(f"Started at: {datetime.utcnow().isoformat()}Z")

    from src.pipeline import run_pipeline

    summary = run_pipeline()

    logger.info("=== Run Summary ===")
    logger.info(f"  Fetched:  {summary['total_fetched']}")
    logger.info(f"  Kept:     {summary['total_kept']}")
    logger.info(f"  Filtered: {summary['total_filtered']}")
    logger.info(f"  New:      {summary['total_new']}")
    logger.info(f"  Changed:  {summary['total_changed']}")
    logger.info(f"  Removed:  {summary['total_removed']}")
    logger.info(f"  Errors:   {len(summary['errors'])}")
    for err in summary["errors"]:
        logger.error(f"  - {err}")

    logger.info("=== Collection complete ===")


if __name__ == "__main__":
    main()
