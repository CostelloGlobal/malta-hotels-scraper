import requests
import pandas as pd
import time
from bs4 import BeautifulSoup
import openai
import os

# Configuration
BASE_URL = "https://www.yellow.com.mt/hotels/?page={}"
HEADERS = {"User-Agent": "Mozilla/5.0"}
openai.api_key = os.getenv("OPENAI_API_KEY")

# Scraper
def scrape_hotels(max_pages=1, max_hotels=5):
    all_hotels = []
    page = 1
    total_scraped = 0

    while page <= max_pages:
        url = BASE_URL.format(page)
        print(f"🟡 Scraping page {page}: {url}")
        res = requests.get(url, headers=HEADERS)
        soup = BeautifulSoup(res.text, "html.parser")

        cards = soup.select("a.business-name, div.business-card a, h2 a, .business-listing a")
        if not cards:
            print("✅ No more listings found.")
            break

        for card in cards:
            name = card.get_text(strip=True)
            link = card.get("href")
            if not link or not name:
                continue

            hotel_page = f"https://www.yellow.com.mt{link}"
            hotel_res = requests.get(hotel_page, headers=HEADERS)
            hotel_soup = BeautifulSoup(hotel_res.text, "html.parser")

            address = hotel_soup.select_one(".address")
            address_text = address.get_text(strip=True) if address else "Address not found"

            # Generate AI description
            try:
                response = openai.ChatCompletion.create(
                    model="gpt-3.5-turbo",
                    messages=[
                        {"role": "system", "content": "You are a helpful assistant that enriches Malta hotel listings for SEO and marketing."},
                        {"role": "user", "content": f"Write a marketing-focused HTML description for hotel '{name}' located at '{address_text}' in Malta. Keep it factual, sensory, and formatted for VisitMalta.co.uk style."}
                    ]
                )
                ai_description = response["choices"][0]["message"]["content"]
            except Exception as e:
                print(f"❌ OpenAI generation failed for {name}: {e}")
                ai_description = "Description not available."

            all_hotels.append({
                "name": name,
                "url": hotel_page,
                "address": address_text,
                "description_html": ai_description
            })

            total_scraped += 1
            print(f"✅ Scraped {name}")

            if total_scraped >= max_hotels:
                print("⏹️ Reached test limit — stopping early.")
                break

        if total_scraped >= max_hotels:
            break

        page += 1
        time.sleep(2)

    # Save results
    df = pd.DataFrame(all_hotels)
    df.to_csv("hotels_enriched_test.csv", index=False, encoding="utf-8")
    print(f"🏁 Done! Saved {len(all_hotels)} hotels to hotels_enriched_test.csv")

# Run script
if __name__ == "__main__":
    scrape_hotels(max_pages=1, max_hotels=5)
