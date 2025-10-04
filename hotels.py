import requests
import pandas as pd
import time
from bs4 import BeautifulSoup
import openai
import os

# 🔧 Configuration
BASE_URL = "https://www.yellow.com.mt/hotels/?page={}"
HEADERS = {"User-Agent": "Mozilla/5.0"}
openai.api_key = os.getenv("OPENAI_API_KEY")

# 🔁 Scraper
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

            try:
                address = hotel_soup.select_one(".address, .business-address").get_text(strip=True)
            except:
                address = ""

            try:
                area = hotel_soup.select_one(".locality, .area").get_text(strip=True)
            except:
                area = ""

            try:
                stars = hotel_soup.select_one(".rating, .stars").get_text(strip=True)
            except:
                stars = ""

            # Generate creative marketing description
            prompt = f"""
            You are a Malta hotel content specialist creating compelling, SEO-optimised hotel profiles for VisitMalta.co.uk.
            Write a high-quality marketing description in HTML for:
            Name: {name}
            Address: {address}
            Area: {area}
            Stars: {stars}

            Follow the provided structure and tone:
            - Emotional, narrative opening (“Where Malta Comes Alive”)
            - Sensory, descriptive storytelling
            - HTML headings and tags (no markdown)
            Output must follow the exact HTML structure from client brief.
            """

            try:
                completion = openai.ChatCompletion.create(
                    model="gpt-4o-mini",
                    messages=[{"role": "system", "content": "You are a luxury hotel content writer for VisitMalta.co.uk."},
                              {"role": "user", "content": prompt}]
                )
                description_html = completion.choices[0].message.content.strip()
            except Exception as e:
                print(f"❌ OpenAI generation failed: {e}")
                description_html = ""

            all_hotels.append({
                "name": name,
                "full_address": address,
                "location": "Malta",
                "area": area,
                "stars": stars,
                "licence_ref": "",
                "bedrooms": "",
                "apartments": "",
                "description_html": description_html
            })

            time.sleep(2)  # polite delay

        page += 1
        time.sleep(1)

    return all_hotels


# 💾 Save CSV
def save_hotels(hotels):
    df = pd.DataFrame(hotels)
    df.to_csv("hotels_enriched.csv", index=False)
    print(f"✅ Saved {len(hotels)} hotels to hotels_enriched.csv")


if __name__ == "__main__":
    hotels = scrape_hotels()
    save_hotels(hotels)
