from bs4 import BeautifulSoup
import re

with open("funda_dump.html", "r") as f:
    html = f.read()

soup = BeautifulSoup(html, "lxml")

# A common pattern for Funda cards now:
# a div with class "border-b pb-6" or similar padding
cards = soup.select("div.border-b.pb-4, div.border-b.pb-6, div[class*='pb-4']")
print(f"Cards found: {len(cards)}")

for card in cards:
    a = card.select_one("a[href*='/detail/huur/']")
    if not a:
        continue
        
    href = a.get("href")
    title = a.get_text(separator=" ", strip=True)
    
    # Remove some extra tags like "Nieuw" or "Uitgelicht" if they got inside the anchor
    title = re.sub(r'^(Nieuw|Uitgelicht|In prijs verlaagd|Blikvanger|Open huis|Top-listing)\s+', '', title)
        
    price_text = card.select_one(".font-semibold, [class*='text-xl'], [class*='price']")
    price = ""
    if price_text:
        m = re.search(r"€\s*([\d.,]+)", price_text.get_text())
        if m:
            price = m.group(1)
            
    # area
    area = ""
    txt = card.get_text()
    m2 = re.search(r"(\d+)\s*m²", txt)
    if m2:
        area = m2.group(1)
        
    print(f"URL: {href} | Title: {title} | Price: {price} | Area: {area}")

