from src.collectors.kamernet import KamernetCollector
import logging

logging.basicConfig(level=logging.INFO)

with KamernetCollector() as c:
    listings = c.collect()
    print(f"Kamernet found: {len(listings)}")
    for l in listings[:3]:
        print(f"{l.title} - {l.price_raw} - {l.raw_location_text} - {l.url}")
