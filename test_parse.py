from bs4 import BeautifulSoup
import re

with open("funda_dump.html", "r") as f:
    html = f.read()

soup = BeautifulSoup(html, "lxml")

print("\n--- Method 1: finding anchor tags ---")
# Funda now uses hrefs like /detail/huur/amsterdam/appartement-.../
items = soup.select("a[href*='/detail/huur/']")
print(f"Anchors found: {len(items)}")

# Try to find the common parent div for these
unique_listings = set()
for a in items:
    # Just save the unique URLs to see how many there are
    href = a.get("href")
    if href:
        unique_listings.add(href)

print(f"Unique listings found: {len(unique_listings)}")

# Look at the first listing's parent structure
if items:
    item = items[0]
    # Go up a few levels to find the "card" container
    parent = item.parent.parent.parent
    if parent:
        print(f"\nParent container classes: {parent.get('class')}")
