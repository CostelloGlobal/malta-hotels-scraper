import requests
import pandas as pd
import time
from bs4 import BeautifulSoup
import openai
import os

# ============ CONFIG ============
BASE_URL = "https://www.yellow.com.mt/hotels/?page={}"
HEADERS = {"User-Agent": "Mozilla/5.0"}
openai.api_key = os.getenv("OPENAI_API_KEY")

# ============ SCRAPER ============
def scrape_hotels(limit=5):
    hotels = []
    page = 1

    while len(hotels) < limit:
        url = BASE_URL.format(page)
        print(f"🟡 Scraping page {page}: {url}")
        res = requests.get(url, headers=HEADERS)
        soup = BeautifulSoup(res.text, "html.parser")

        cards = soup.select("a.business-name, div.business-card a, h2 a, .business-listing a")
        if not cards:
            print("✅ No more listings found.")
            break

        for card in cards:
            if len(hotels) >= limit:
                break

            name = card.get_text(strip=True)
            link = card.get("href")
            if not link or not name:
                continue

            hotel_page = f"https://www.yellow.com.mt{link}"
            print(f"🏨 Scraping: {name}")

            hotel_res = requests.get(hotel_page, headers=HEADERS)
            hotel_soup = BeautifulSoup(hotel_res.text, "html.parser")
            address = hotel_soup.select_one(".address")
            address_text = address.get_text(strip=True) if address else "Address not found"

            try:
                response = openai.ChatCompletion.create(
                    model="gpt-3.5-turbo",
                    messages=[
                        {"role": "system", "content": "Write a 100-word marketing description in HTML for a Maltese hotel using sensory and emotional tone. No markdown."},
                        {"role": "user", "content": f"Hotel: {name}, Address: {address_text}, Country: Malta"}
                    ]
                )
                ai_description = response["choices"][0]["message"]["content"]
            except Exception as e:
                print(f"❌ OpenAI failed for {name}: {e}")
                ai_description = "<p>No description available.</p>"

            hotels.append({
                "name": name,
                "address": address_text,
                "description_html": ai_description,
                "url": hotel_page
            })
            time.sleep(2)

        page += 1
        time.sleep(3)

    df = pd.DataFrame(hotels)
    df.to_csv("hotels_test_output.csv", index=False, encoding="utf-8")
    print(f"✅ Done! {len(hotels)} hotels saved to hotels_test_output.csv")

# ============ RUN ============
if __name__ == "__main__":
    scrape_hotels(limit=5)
