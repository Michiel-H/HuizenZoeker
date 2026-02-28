"""Gmail SMTP email sender for daily rental digest."""

from __future__ import annotations

import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from src.config import GMAIL_ADDRESS, GMAIL_APP_PASSWORD, TO_EMAIL
from src.models import StoredListing

logger = logging.getLogger(__name__)


def send_daily_digest(
    date_str: str,
    new_listings: list[StoredListing],
    changed_listings: list[StoredListing],
    removed_listings: list[StoredListing],
) -> bool:
    """Send the daily digest email via Gmail SMTP.

    Returns True if sent successfully, False otherwise.
    """
    if not GMAIL_ADDRESS or not GMAIL_APP_PASSWORD:
        logger.error("Gmail credentials not configured. Set GMAIL_ADDRESS and GMAIL_APP_PASSWORD.")
        return False

    to_addr = TO_EMAIL or GMAIL_ADDRESS
    n_new = len(new_listings)
    n_changed = len(changed_listings)
    n_removed = len(removed_listings)

    subject = (
        f"Amsterdam Rentals — Daily Update ({date_str}) — "
        f"{n_new} new / {n_changed} changed / {n_removed} removed"
    )

    html_body = _build_html(date_str, new_listings, changed_listings, removed_listings)
    text_body = _build_text(date_str, new_listings, changed_listings, removed_listings)

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = GMAIL_ADDRESS
    msg["To"] = to_addr

    msg.attach(MIMEText(text_body, "plain", "utf-8"))
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD)
            server.send_message(msg)
        logger.info(f"Daily digest sent to {to_addr}")
        return True
    except Exception as e:
        logger.error(f"Failed to send email: {e}", exc_info=True)
        return False


def _format_price(listing: StoredListing) -> str:
    """Format price with service cost indicator."""
    if listing.price_total_eur is None:
        return "Prijs onbekend"
    price_str = f"€{listing.price_total_eur:,.0f}".replace(",", ".")
    if listing.price_quality == "UNKNOWN":
        price_str += " ⚠️ servicekosten onbekend"
    if listing.gwl_included:
        price_str += " (incl. g/w/l)"
    return price_str


def _listing_html(listing: StoredListing, show_changes: bool = False) -> str:
    """Render a single listing as HTML."""
    price = _format_price(listing)
    hood = listing.neighborhood_match or "Buurt onbekend"
    snippet = (listing.description_snippet or "")[:150]

    changes_html = ""
    if show_changes and listing.change_log:
        import json
        try:
            log = json.loads(listing.change_log)
            if log:
                last = log[-1]
                diffs = last.get("changes", {})
                parts = []
                for field, vals in diffs.items():
                    parts.append(f"<em>{field}</em>: {vals.get('old', '?')} → {vals.get('new', '?')}")
                if parts:
                    changes_html = f'<div style="color:#c0392b;font-size:12px;">{"  |  ".join(parts)}</div>'
        except (json.JSONDecodeError, IndexError):
            pass

    return f"""
    <div style="border-bottom:1px solid #eee;padding:10px 0;">
        <div style="font-size:15px;font-weight:bold;">
            <a href="{listing.url}" style="color:#2c3e50;text-decoration:none;">{listing.title or 'Geen titel'}</a>
        </div>
        <div style="font-size:14px;color:#27ae60;font-weight:bold;">{price}</div>
        <div style="font-size:13px;color:#7f8c8d;">{hood} · {listing.source} · {listing.area_m2 or '?'}m²</div>
        {changes_html}
        <div style="font-size:12px;color:#95a5a6;margin-top:4px;">{snippet}</div>
    </div>
    """


def _listing_text(listing: StoredListing) -> str:
    """Render a single listing as plain text."""
    price = _format_price(listing)
    hood = listing.neighborhood_match or "Buurt onbekend"
    return (
        f"  {listing.title or 'Geen titel'}\n"
        f"  {price} | {hood} | {listing.source} | {listing.area_m2 or '?'}m²\n"
        f"  {listing.url}\n"
    )


def _build_html(
    date_str: str,
    new_listings: list[StoredListing],
    changed_listings: list[StoredListing],
    removed_listings: list[StoredListing],
) -> str:
    """Build the full HTML email body."""
    n_new = len(new_listings)
    n_changed = len(changed_listings)
    n_removed = len(removed_listings)

    sections = []

    # Summary
    sections.append(f"""
    <div style="background:#ecf0f1;padding:15px;border-radius:8px;margin-bottom:20px;">
        <h2 style="margin:0 0 8px;">Amsterdam Rental Monitor — {date_str}</h2>
        <div style="font-size:16px;">
            <span style="color:#27ae60;">🆕 {n_new} new</span> ·
            <span style="color:#f39c12;">🔄 {n_changed} changed</span> ·
            <span style="color:#e74c3c;">❌ {n_removed} removed</span>
        </div>
    </div>
    """)

    # NEW
    if new_listings:
        items = "".join(_listing_html(l) for l in new_listings[:20])
        more = f"<div style='color:#95a5a6;'>...en {n_new - 20} meer</div>" if n_new > 20 else ""
        sections.append(f"""
        <h3 style="color:#27ae60;">🆕 Nieuw ({n_new})</h3>
        {items}
        {more}
        """)

    # CHANGED
    if changed_listings:
        items = "".join(_listing_html(l, show_changes=True) for l in changed_listings[:20])
        sections.append(f"""
        <h3 style="color:#f39c12;">🔄 Gewijzigd ({n_changed})</h3>
        {items}
        """)

    # REMOVED
    if removed_listings:
        items = "".join(_listing_html(l) for l in removed_listings[:20])
        sections.append(f"""
        <h3 style="color:#e74c3c;">❌ Verwijderd ({n_removed})</h3>
        {items}
        """)

    if not new_listings and not changed_listings and not removed_listings:
        sections.append("<p>Geen wijzigingen vandaag.</p>")

    body_content = "\n".join(sections)

    return f"""
    <!DOCTYPE html>
    <html>
    <head><meta charset="utf-8"></head>
    <body style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;max-width:600px;margin:0 auto;padding:20px;color:#2c3e50;">
        {body_content}
        <hr style="margin-top:30px;border:none;border-top:1px solid #eee;">
        <p style="font-size:11px;color:#bdc3c7;">Amsterdam Rental Monitor — automated daily digest</p>
    </body>
    </html>
    """


def _build_text(
    date_str: str,
    new_listings: list[StoredListing],
    changed_listings: list[StoredListing],
    removed_listings: list[StoredListing],
) -> str:
    """Build the plain text email body."""
    n_new = len(new_listings)
    n_changed = len(changed_listings)
    n_removed = len(removed_listings)

    lines = [
        f"Amsterdam Rental Monitor — {date_str}",
        f"{n_new} new | {n_changed} changed | {n_removed} removed",
        "",
    ]

    if new_listings:
        lines.append(f"=== NIEUW ({n_new}) ===")
        for l in new_listings[:20]:
            lines.append(_listing_text(l))
        if n_new > 20:
            lines.append(f"  ...en {n_new - 20} meer\n")

    if changed_listings:
        lines.append(f"=== GEWIJZIGD ({n_changed}) ===")
        for l in changed_listings[:20]:
            lines.append(_listing_text(l))

    if removed_listings:
        lines.append(f"=== VERWIJDERD ({n_removed}) ===")
        for l in removed_listings[:20]:
            lines.append(_listing_text(l))

    if not new_listings and not changed_listings and not removed_listings:
        lines.append("Geen wijzigingen vandaag.")

    lines.extend(["", "---", "Amsterdam Rental Monitor — automated daily digest"])
    return "\n".join(lines)
