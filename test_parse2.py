from bs4 import BeautifulSoup
import re

with open("funda_dump.html", "r") as f:
    html = f.read()

soup = BeautifulSoup(html, "lxml")

# The listing container seems to have "data-test-id='search-result-item'" removed.
# But we found unique hrefs! Let's find all the a tags with "data-test-id='street-name-house-number'" first, or '/detail/huur/'
anchors = soup.select("a[href*='/detail/huur/']")
processed = set()

for a in anchors:
    href = a.get("href")
    if href in processed:
        continue
    processed.add(href)

    # Let's say the parent container is this anchor itself, and look for text around it
    # Actually, a better way is to find a common wrapper block.
    wrapper = a.find_parent("div", class_=re.compile("border-b|p-4|flex-col"))

    title = a.get_text(strip=True)
    if not title:
        continue
    
    # price is nearby
    price = ""
    if wrapper:
        text_content = wrapper.get_text(separator=" ", strip=True)
        m = re.search(r"€\s*([\d.,]+)", text_content)
        if m:
            price = m.group(1)
        
        # area
        area = ""
        m2 = re.search(r"(\d+)\s*m²", text_content)
        if m2:
            area = m2.group(1)
            
        print(f"Title: {title} | Price: {price} | Area: {area}")

