from curl_cffi import requests
from bs4 import BeautifulSoup

headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "nl-NL,nl;q=0.9,en;q=0.8",
}

session = requests.Session(impersonate="chrome", headers=headers)
resp = session.get("https://kamernet.nl/huren/appartement-amsterdam")

soup = BeautifulSoup(resp.text, "lxml")
# Find anchors that look like they go to a rental detail 
anchors = soup.select("a[href*='/huren/']")
print(f"Total anchors with '/huren/': {len(anchors)}")

unique = set()
for a in anchors:
    href = a.get("href")
    if '/huren/appartement-amsterdam/' in href or '/huren/kamer-amsterdam/' in href:
        unique.add(href)
        
print(f"Unique listings: {len(unique)}")
if unique:
    print("Sample href:", list(unique)[0])
