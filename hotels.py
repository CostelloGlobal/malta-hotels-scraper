import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import csv

BASE_URL = "https://www.yellow.com.mt/hotels/?page={}"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

def get_hotel_links():
    hotel_links = []
    page = 1

    while True:
        print(f"Scraping Yellow Malta Hotel listings – Page {page}...")
        url = BASE_URL.format(page)
        response = requests.get(url, headers=HEADERS)

        if response.status_code != 200:
            print(f"⚠️  Failed to load page {page}, status {response.status_code}")
            break

        soup = BeautifulSoup(response.text, "html.parser")

        # ✅ Updated selector (broader pattern)
        cards = soup.select("a.business-name, div.business-card a, h2 a, .business-listing a")

        if not cards:
            print("No more listings on this page. Done.")
            break

        for card in cards:
            href = card.get("href")
            if href and href.startswith("/"):
                hotel_links.append("https://www.yellow.com.mt" + href)

        page += 1
        time.sleep(1.5)  # respectful delay between pages

    print(f"✅ Found {len(hotel_links)} total hotel links.")
    return hotel_links


def scrape_hotel_details(hotel_links):
    hotels_data = []

    for i, link in enumerate(hotel_links, start=1):
        print(f"Scraping hotel {i}/{len(hotel_links)}: {link}")
        try:
            r = requests.get(link, headers=HEADERS)
            soup = BeautifulSoup(r.text, "html.parser")

            name = soup.find("h1")
            phone = soup.find("a", {"data-testid": "business-phone"})
            address = soup.find("p", {"data-testid": "business-address"})
            website = soup.find("a", {"data-testid": "business-website"})

            hotels_data.append({
                "Name": name.text.strip() if name else "",
                "Phone": phone.text.strip() if phone else "",
                "Address": address.text.strip() if address else "",
                "Website": website.get("href").strip() if website else "",
                "URL": link
            })

            time.sleep(1)
        except Exception as e:
            print(f"Error scraping {link}: {e}")

    return hotels_data


if __name__ == "__main__":
    print("🚀 Starting full Malta Hotels scrape...")
    hotel_links = get_hotel_links()

    if not hotel_links:
        print("⚠️ No hotels found — check the selector or site layout.")
    else:
        hotels_data = scrape_hotel_details(hotel_links)

        # Save results to CSV
        df = pd.DataFrame(hotels_data)
        df.to_csv("hotels_enriched.csv", index=False, quoting=csv.QUOTE_ALL)
        print(f"✅ Done. Saved {len(hotels_data)} hotels to hotels_enriched.csv")
