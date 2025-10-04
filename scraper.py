import requests
from bs4 import BeautifulSoup
import pandas as pd
import time

base_url = "https://www.yellow.com.mt/hotels/?page={}"
hotel_data = []

print("🔍 Starting hotel scrape...")

for page in range(1, 50):  # Will go through all pages
    url = base_url.format(page)
    print(f"Scraping page {page}: {url}")
    response = requests.get(url)
    if response.status_code != 200:
        print(f"⚠️ Failed to load page {page}")
        break

    soup = BeautifulSoup(response.text, "html.parser")
    listings = soup.select("div[data-testid='business-list-card']")
    if not listings:
        print("✅ No more listings found, stopping.")
        break

    for listing in listings:
        name_tag = listing.select_one("h2 a")
        if name_tag:
            name = name_tag.text.strip()
            link = "https://www.yellow.com.mt" + name_tag.get("href")
        else:
            name = "N/A"
            link = "N/A"

        address_tag = listing.select_one("p")
        address = address_tag.text.strip() if address_tag else "N/A"

        hotel_data.append({
            "name": name,
            "address": address,
            "url": link
        })

    time.sleep(1)

df = pd.DataFrame(hotel_data)
df.to_csv("hotels.csv", index=False)
print(f"✅ Done! Scraped {len(df)} hotels.")
