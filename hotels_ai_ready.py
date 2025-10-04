import requests
from bs4 import BeautifulSoup
import pandas as pd
import time

# ---------------- CONFIG ---------------- #
BASE = "https://www.yellow.com.mt"
LIST_URL = BASE + "/hotels/?page="
HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36"
}
MAX_PAGES = 6  # enough to cover ~90 hotels
SLEEP_BETWEEN = 1.2  # seconds delay per page to be polite

OUT_CSV = "hotels_ai_ready.csv"

# ---------------- SCRAPER ---------------- #
def scrape_hotels():
    all_hotels = []
    for page in range(1, MAX_PAGES + 1):
        url = f"{LIST_URL}{page}"
        print(f"Scraping page {page} -> {url}")
        resp = requests.get(url, headers=HEADERS)

        if resp.status_code != 200:
            print(f"⚠️ Failed to load page {page}: {resp.status_code}")
            continue

        soup = BeautifulSoup(resp.text, "html.parser")

        # selector for hotel cards on Yellow.mt
        cards = soup.select("a.BusinessCardV2style__CardHeaderLink-sc-__sc-1k6t9dc-7")

        if not cards:
            print(f"⚠️ No listings found on page {page}, stopping.")
            break

        for a in cards:
            name = a.get_text(strip=True)
            href = a.get("href")
            if href and not href.startswith("http"):
                href = BASE + href

            # follow the link to get details
            details = scrape_hotel_details(href)
            all_hotels.append({
                "name": name,
                "url": href,
                "address": details.get("address", ""),
                "phone": details.get("phone", ""),
                "email": details.get("email", "")
            })

        print(f"✅ Page {page}: found {len(cards)} hotels")
        time.sleep(SLEEP_BETWEEN)

    # save to CSV
    df = pd.DataFrame(all_hotels)
    df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    print(f"\n✅ Wrote {len(all_hotels)} hotels to {OUT_CSV}")


# ---------------- DETAIL SCRAPER ---------------- #
def scrape_hotel_details(url):
    info = {}
    try:
        resp = requests.get(url, headers=HEADERS, timeout=20)
        if resp.status_code != 200:
            return info

        soup = BeautifulSoup(resp.text, "html.parser")

        # Try to get contact info (Yellow.mt structure)
        address_tag = soup.select_one("div.Addressstyle__AddressLine-sc-__sc-14lcyur-2")
        phone_tag = soup.select_one("a[href^='tel:']")
        email_tag = soup.select_one("a[href^='mailto:']")

        if address_tag:
            info["address"] = address_tag.get_text(strip=True)
        if phone_tag:
            info["phone"] = phone_tag.get_text(strip=True)
        if email_tag:
            info["email"] = email_tag.get_text(strip=True)

    except Exception as e:
        print(f"⚠️ Error scraping {url}: {e}")

    return info


# ---------------- MAIN ---------------- #
if __name__ == "__main__":
    print("🚀 Starting full Malta Hotels scrape...")
    scrape_hotels()
    print("✅ Done.")
