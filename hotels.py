import requests
import pandas as pd
import time
from bs4 import BeautifulSoup
import openai
import os

# ==========================
# CONFIGURATION
# ==========================

BASE_URL = "https://www.yellow.com.mt/hotels/?page={}"
HEADERS = {"User-Agent": "Mozilla/5.0"}
openai.api_key = os.getenv("OPENAI_API_KEY")

# ==========================
# SCRAPER FUNCTION
# ==========================

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
            print("✅ No more listings found — stopping.")
            break

        for card in cards:
            name = card.get_text(strip=True)
            link = card.get("href")

            if not link or not name:
                continue

            hotel_page = f"https://www.yellow.com.mt{link}"
            print(f"🏨 Scraping details for: {name}")

            hotel_res = requests.get(hotel_page, headers=HEADERS)
            hotel_soup = BeautifulSoup(hotel_res.text, "html.parser")

            address = hotel_soup.select_one(".address")
            address_text = address.get_text(strip=True) if address else "Address not found"

            # ==========================
            # AI ENRICHMENT (MARKETING COPY)
            # ==========================
            try:
                response = openai.ChatCompletion.create(
                    model="gpt-3.5-turbo",
                    messages=[
                        {
                            "role": "system",
                            "content": (
                                "You are a Malta hotel content specialist writing SEO-optimised, emotional, sensory-rich HTML descriptions "
                                "for VisitMalta.co.uk. Use vivid imagery, accurate details, and emotional marketing tone. "
                                "Every description must strictly follow HTML formatting, with closed tags, no markdown."
                            ),
                        },
                        {
                            "role": "user",
                            "content": f"""
Write a full HTML hotel profile for:
Hotel name: {name}
Address: {address_text}
Country: Malta

Follow this structure:
<h3>{name} | Boutique Stay in Malta</h3>
<p><strong>[Opening tagline about emotion, views, or atmosphere]</strong></p>
<p>[Sensory, emotional paragraph about the hotel’s vibe, character, and surroundings]</p>
<h4>Hotel Features</h4>
<ul>
<li>[Three features typical of Maltese hotels in this category]</li>
<li>[Include cultural or design reference if possible]</li>
</ul>
<h4>Location</h4>
<p><strong>📍 Address:</strong> {address_text}</p>
<p>Within walking distance of key attractions in Malta.</p>
<h4>Perfect For</h4>
<ul>
<li>Travellers who want authentic Maltese charm</li>
<li>Those seeking proximity to beaches and nightlife</li>
</ul>
<p><strong>Ready to experience Malta?</strong><br>[BOOK NOW - KM Malta Airlines Packages]</p>
""",
                        },
                    ],
                )
                ai_description = response["choices"][0]["message"]["content"]
            except Exception as e:
                print(f"❌ OpenAI generation failed for {name}: {e}")
                ai_description = "<p>Description unavailable.</p>"

            # Save structured data
            all_hotels.append({
                "name": name,
                "url": hotel_page,
                "address": address_text,
                "description_html": ai_description
            })

            # Delay between hotels to avoid blocking
            time.sleep(2)

        page += 1
        time.sleep(3)

    # ==========================
    # EXPORT RESULTS
    # ==========================
    df = pd.DataFrame(all_hotels)
    df.to_csv("hotels_enriched.csv", index=False, encoding="utf-8")
    print(f"🏁 Done! Saved {len(all_hotels)} hotels to hotels_enriched.csv")

# ==========================
# RUN SCRIPT
# ==========================
if __name__ == "__main__":
    scrape_hotels()
