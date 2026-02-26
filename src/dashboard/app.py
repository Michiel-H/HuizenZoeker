"""Amsterdam Rental Monitor — Streamlit Dashboard."""

import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

import streamlit as st

# Add project root to path for imports
ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from src.config import DB_PATH, TARGET_NEIGHBORHOODS
from src.storage.database import get_db, get_listings, init_db

st.set_page_config(
    page_title="Amsterdam Rental Monitor",
    page_icon="🏠",
    layout="wide",
)


def main():
    st.title("🏠 Amsterdam Rental Monitor")
    st.caption("Daily overview of rental listings in Amsterdam")

    init_db()

    if not DB_PATH.exists():
        st.warning("No database found. Run the collection pipeline first.")
        return

    # Sidebar filters
    st.sidebar.header("🔍 Filters")

    neighborhoods = ["All"] + sorted(TARGET_NEIGHBORHOODS.keys())
    selected_hood = st.sidebar.selectbox("Neighborhood", neighborhoods)

    price_range = st.sidebar.slider(
        "Price range (€/month)",
        min_value=0,
        max_value=3000,
        value=(0, 2200),
        step=50,
    )

    sources_list = _get_sources()
    selected_source = st.sidebar.selectbox("Source", ["All"] + sources_list)

    status_options = ["ACTIVE", "REMOVED", "All"]
    selected_status = st.sidebar.selectbox("Status", status_options)

    sort_options = {
        "Newest first": "newest",
        "Price: low to high": "price_asc",
        "Price: high to low": "price_desc",
    }
    selected_sort = st.sidebar.selectbox("Sort by", list(sort_options.keys()))

    # Date range filter
    date_range = st.sidebar.date_input(
        "Since date",
        value=datetime.now() - timedelta(days=7),
    )

    # Query listings
    with get_db() as conn:
        filters = {}
        if selected_hood != "All":
            filters["neighborhood"] = selected_hood
        if selected_source != "All":
            filters["source"] = selected_source
        if selected_status != "All":
            filters["status"] = selected_status
        filters["min_price"] = price_range[0] if price_range[0] > 0 else None
        filters["max_price"] = price_range[1] if price_range[1] < 3000 else None
        if date_range:
            filters["since"] = str(date_range) + "T00:00:00"

        all_listings = get_listings(conn, **{k: v for k, v in filters.items() if v is not None})

    # Sort
    sort_key = sort_options[selected_sort]
    if sort_key == "price_asc":
        all_listings.sort(key=lambda x: x.price_total_eur or 99999)
    elif sort_key == "price_desc":
        all_listings.sort(key=lambda x: -(x.price_total_eur or 0))
    else:  # newest
        all_listings.sort(key=lambda x: x.last_changed_at or "", reverse=True)

    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)
    active = [l for l in all_listings if l.status == "ACTIVE"]
    removed = [l for l in all_listings if l.status == "REMOVED"]
    avg_price = sum(l.price_total_eur for l in active if l.price_total_eur) / max(len([l for l in active if l.price_total_eur]), 1)

    col1.metric("Total Listings", len(all_listings))
    col2.metric("Active", len(active))
    col3.metric("Removed", len(removed))
    col4.metric("Avg Price", f"€{avg_price:,.0f}")

    st.divider()

    # Listings display
    if not all_listings:
        st.info("No listings found matching your filters.")
        return

    for listing in all_listings:
        _render_listing(listing)

    # Export CSV
    st.divider()
    if st.button("📥 Export as CSV"):
        csv_data = _to_csv(all_listings)
        st.download_button(
            label="Download CSV",
            data=csv_data,
            file_name=f"amsterdam_rentals_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv",
        )


def _render_listing(listing):
    """Render a single listing card."""
    status_emoji = "🟢" if listing.status == "ACTIVE" else "🔴"
    price_str = f"€{listing.price_total_eur:,.0f}" if listing.price_total_eur else "Price unknown"
    quality_badge = ""
    if listing.price_quality == "UNKNOWN":
        quality_badge = " ⚠️ Service costs unknown"
    gwl_badge = " (incl. g/w/l)" if listing.gwl_included else ""

    with st.container():
        col1, col2, col3 = st.columns([3, 1, 1])

        with col1:
            st.markdown(f"### {status_emoji} [{listing.title or 'No title'}]({listing.url})")
            st.caption(
                f"📍 {listing.neighborhood_match or 'Unknown'} · "
                f"🏢 {listing.source} · "
                f"📐 {listing.area_m2 or '?'} m² · "
                f"🛏️ {listing.bedrooms or '?'} bedrooms"
            )
            if listing.description_snippet:
                st.text(listing.description_snippet[:200])

        with col2:
            st.markdown(f"**{price_str}**{quality_badge}{gwl_badge}")
            st.caption(f"/month")

        with col3:
            st.caption(f"First seen: {listing.first_seen_at[:10] if listing.first_seen_at else '?'}")
            st.caption(f"Last update: {listing.last_changed_at[:10] if listing.last_changed_at else '?'}")

            # Show change log for changed listings
            if listing.change_log and listing.change_log != "[]":
                try:
                    log = json.loads(listing.change_log)
                    if log:
                        with st.expander("📝 Changes"):
                            for entry in log[-3:]:  # last 3 changes
                                ts = entry.get("timestamp", "?")[:10]
                                for field, vals in entry.get("changes", {}).items():
                                    st.markdown(
                                        f"**{ts}** — {field}: "
                                        f"`{vals.get('old', '?')}` → `{vals.get('new', '?')}`"
                                    )
                except (json.JSONDecodeError, TypeError):
                    pass

        if listing.ambiguous_neighborhood:
            st.warning("⚠️ Neighborhood match is ambiguous")

        st.divider()


def _get_sources() -> list[str]:
    """Get distinct sources from the database."""
    try:
        with get_db() as conn:
            rows = conn.execute("SELECT DISTINCT source FROM listings ORDER BY source").fetchall()
            return [r["source"] for r in rows]
    except Exception:
        return []


def _to_csv(listings) -> str:
    """Convert listings to CSV string."""
    import csv
    import io

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "title", "url", "price_eur", "price_quality", "neighborhood",
        "area_m2", "bedrooms", "source", "status", "first_seen", "last_changed",
    ])
    for l in listings:
        writer.writerow([
            l.title, l.url, l.price_total_eur, l.price_quality,
            l.neighborhood_match, l.area_m2, l.bedrooms, l.source,
            l.status, l.first_seen_at, l.last_changed_at,
        ])
    return output.getvalue()


if __name__ == "__main__":
    main()
