import requests
import pandas as pd
import time
import os
from bs4 import BeautifulSoup
import openai

# --- Configuration ---
BASE_URL = "https://www.yellow.com.mt/hotels/?page={}"
HEADERS = {"User-Agent": "Mozilla/5.0"}
openai.api_key = os.getenv("OPENAI_API_KEY")

# --- Scraper ---
def scrape_hotels():
    all_hotels = []
    page = 1

    while True:
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

            description_tag = hotel_soup.find("meta", {"name": "description"})
            description = description_tag["content"] if description_tag else ""

            try:
                response = openai.ChatCompletion.create(
                    model="gpt-3.5-turbo",
                    messages=[
                        {"role": "system", "content": "You are a helpful assistant that enriches hotel listings with useful summaries."},
                        {"role": "user", "content": f"Enrich this hotel listing: {name}, {description}"}
                    ]
                )
                ai_description = response["choices"][0]["message"]["content"]
            except Exception as e:
                ai_description = f"AI generation failed: {e}"

            all_hotels.append({
                "name": name,
                "url": hotel_page,
                "description": description,
                "ai_summary": ai_description
            })

        page += 1
        time.sleep(2)

    df = pd.DataFrame(all_hotels)
    df.to_csv("hotels_enriched.csv", index=False)
    print("✅ Scraping complete — results saved to hotels_enriched.csv")

if __name__ == "__main__":
    scrape_hotels()
