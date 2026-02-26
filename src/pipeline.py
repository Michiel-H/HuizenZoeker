"""Main pipeline: collect, normalize, filter, deduplicate, store."""

from __future__ import annotations

import logging
from datetime import datetime

from src.collectors.registry import get_all_collectors
from src.config import MAX_PRICE_EUR
from src.dedupe.engine import compute_dedupe_score, generate_dedupe_id
from src.models import NormalizedListing, RawListing
from src.normalizer.normalize import normalize_listing
from src.storage.database import (
    get_db,
    get_listings,
    init_db,
    log_run,
    mark_missing,
    upsert_listing,
)

logger = logging.getLogger(__name__)


def run_pipeline() -> dict:
    """Run the full collection pipeline.

    Returns a summary dict with counts.
    """
    init_db()
    summary = {
        "total_fetched": 0,
        "total_kept": 0,
        "total_filtered": 0,
        "total_new": 0,
        "total_changed": 0,
        "total_removed": 0,
        "errors": [],
    }

    collectors = get_all_collectors()

    for collector in collectors:
        source_name = collector.SOURCE_NAME
        logger.info(f"=== Collecting from {source_name} ===")

        try:
            raw_listings = collector.safe_collect()
            fetched = len(raw_listings)
            summary["total_fetched"] += fetched

            # Normalize
            normalized = []
            for raw in raw_listings:
                try:
                    norm = normalize_listing(raw)
                    normalized.append(norm)
                except Exception as e:
                    logger.error(f"[{source_name}] Normalization error: {e}")

            # Filter: price and neighborhood
            kept = []
            filtered_count = 0
            for listing in normalized:
                # Must have a neighborhood match
                if listing.neighborhood_match is None:
                    filtered_count += 1
                    continue

                # Price filter
                if listing.price_total_eur is not None:
                    if listing.price_total_eur >= MAX_PRICE_EUR:
                        filtered_count += 1
                        continue
                # If price unknown, still include but mark

                kept.append(listing)

            summary["total_kept"] += len(kept)
            summary["total_filtered"] += filtered_count

            # Deduplicate and store
            new_count = 0
            changed_count = 0
            seen_source_ids: set[str] = set()

            with get_db() as conn:
                # Get existing listings for cross-source dedup
                existing_listings = get_listings(conn, status="ACTIVE")
                existing_normalized = [
                    NormalizedListing(
                        source=sl.source,
                        source_id=sl.source_id,
                        url=sl.url,
                        title=sl.title,
                        raw_location_text=sl.raw_location_text,
                        neighborhood_match=sl.neighborhood_match,
                        neighborhood_confidence=sl.neighborhood_confidence,
                        price_total_eur=sl.price_total_eur,
                        price_quality=sl.price_quality,
                        price_includes_service_costs=sl.price_includes_service_costs,
                        gwl_included=sl.gwl_included,
                        area_m2=sl.area_m2,
                        bedrooms=sl.bedrooms,
                        property_type=sl.property_type,
                        available_from=sl.available_from,
                        description_snippet=sl.description_snippet,
                        images_hash=sl.images_hash,
                        ambiguous_neighborhood=sl.ambiguous_neighborhood,
                    )
                    for sl in existing_listings
                ]
                existing_dedupe_map = {
                    (sl.source, sl.source_id): sl.dedupe_id
                    for sl in existing_listings
                    if sl.source_id
                }

                for listing in kept:
                    # Track seen source IDs for missing detection
                    sid = listing.source_id or listing.url
                    seen_source_ids.add(sid)

                    # Find dedupe_id: check existing by source+source_id first
                    dedupe_id = existing_dedupe_map.get(
                        (listing.source, listing.source_id)
                    )

                    if dedupe_id is None:
                        # Cross-source dedup
                        from src.config import DEDUPE_COMBINED_THRESHOLD

                        best_dedupe_id = None
                        best_score = 0.0

                        for existing_norm, existing_stored in zip(
                            existing_normalized, existing_listings
                        ):
                            if existing_norm.source == listing.source:
                                continue
                            score = compute_dedupe_score(listing, existing_norm)
                            if (
                                score.combined >= DEDUPE_COMBINED_THRESHOLD
                                and score.combined > best_score
                            ):
                                best_score = score.combined
                                best_dedupe_id = existing_stored.dedupe_id

                        dedupe_id = best_dedupe_id or generate_dedupe_id()

                    change_type, changes = upsert_listing(
                        conn=conn,
                        dedupe_id=dedupe_id,
                        source=listing.source,
                        source_id=listing.source_id,
                        url=listing.url,
                        title=listing.title,
                        raw_location_text=listing.raw_location_text,
                        neighborhood_match=listing.neighborhood_match,
                        neighborhood_confidence=listing.neighborhood_confidence,
                        price_total_eur=listing.price_total_eur,
                        price_quality=listing.price_quality.value,
                        price_includes_service_costs=listing.price_includes_service_costs,
                        gwl_included=listing.gwl_included,
                        area_m2=listing.area_m2,
                        bedrooms=listing.bedrooms,
                        property_type=listing.property_type,
                        available_from=listing.available_from,
                        description_snippet=listing.description_snippet,
                        images_hash=listing.images_hash,
                        ambiguous_neighborhood=listing.ambiguous_neighborhood,
                    )

                    if change_type == "NEW":
                        new_count += 1
                    elif change_type == "CHANGED":
                        changed_count += 1

                # Mark missing listings for this source
                removed = mark_missing(conn, source_name, seen_source_ids)
                removed_count = len(removed)

                # Log the run
                log_run(
                    conn=conn,
                    source=source_name,
                    fetched=fetched,
                    kept=len(kept),
                    filtered=filtered_count,
                    new_count=new_count,
                    changed_count=changed_count,
                    removed_count=removed_count,
                    errors="",
                )

            summary["total_new"] += new_count
            summary["total_changed"] += changed_count
            summary["total_removed"] += removed_count

            logger.info(
                f"[{source_name}] fetched={fetched} kept={len(kept)} "
                f"filtered={filtered_count} new={new_count} "
                f"changed={changed_count} removed={removed_count}"
            )

        except Exception as e:
            error_msg = f"[{source_name}] Pipeline error: {e}"
            logger.error(error_msg, exc_info=True)
            summary["errors"].append(error_msg)

            # Log the failed run
            try:
                with get_db() as conn:
                    log_run(
                        conn=conn,
                        source=source_name,
                        fetched=0,
                        kept=0,
                        filtered=0,
                        new_count=0,
                        changed_count=0,
                        removed_count=0,
                        errors=str(e),
                    )
            except Exception:
                pass

        finally:
            collector.close()

    logger.info(
        f"=== Pipeline complete: fetched={summary['total_fetched']} "
        f"kept={summary['total_kept']} filtered={summary['total_filtered']} "
        f"new={summary['total_new']} changed={summary['total_changed']} "
        f"removed={summary['total_removed']} errors={len(summary['errors'])} ==="
    )
    return summary
