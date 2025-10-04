import requests
from bs4 import BeautifulSoup
import pandas as pd
import time

BASE = "https://www.yellow.com.mt"
LIST_URL = f"{BASE}/hotels/?page="
HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36"
}
OUT_CSV = "hotels_scraped.csv"
MAX_PAGES = 10   # Safety cap in case pagination changes
SLEEP_BETWEEN = 1.5

def scrape_hotels():
    all_hotels = []

    for page in range(1, MAX_PAGES + 1):
        url = f"{LIST_URL}{page}"
        print(f"📄 Scraping page {page} → {url}")
        resp = requests.get(url, headers=HEADERS)
        if resp.status_code != 200:
            print(f"⚠️ Page {page} returned {resp.status_code}, stopping.")
            break

        soup = BeautifulSoup(resp.text, "html.parser")
        cards = soup.select("a[href*='/business/']")

        if not cards:
            print(f"⚠️ No hotel links found on page {page}, stopping.")
            break

        for a in cards:
            link = a.get("href")
            if not link:
                continue
            if not link.startswith("http"):
                link = BASE + link

            # Visit detail page
            try:
                detail_resp = requests.get(link, headers=HEADERS)
                detail_soup = BeautifulSoup(detail_resp.text, "html.parser")

                name_tag = detail_soup.select_one("h1")
                name = name_tag.text.strip() if name_tag else ""

                address_tag = detail_soup.select_one("div.BusinessAddressstyle__BusinessAddress-sc-__sc-10jde9-0")
                address = address_tag.text.strip() if address_tag else ""

                phone_tag = detail_soup.select_one("a[href^='tel:']")
                phone = phone_tag.text.strip() if phone_tag else ""

                website_tag = detail_soup.select_one("a[href^='http']")
                website = website_tag["href"].strip() if website_tag else ""

                all_hotels.append({
                    "name": name,
                    "address": address,
                    "phone": phone,
                    "website": website,
                    "link": link
                })
                print(f"✅ {name}")

            except Exception as e:
                print(f"❌ Error scraping {link}: {e}")
                continue

            time.sleep(0.5)  # Delay between detail pages

        time.sleep(SLEEP_BETWEEN)  # Delay between pages

    # Save to CSV
    df = pd.DataFrame(all_hotels)
    df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    print(f"✅ DONE — Scraped {len(all_hotels)} hotels → {OUT_CSV}")

if __name__ == "__main__":
    scrape_hotels()
