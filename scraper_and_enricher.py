import requests
from bs4 import BeautifulSoup
import pandas as pd
import openai
import os
import time

# 🔐 Load API key from GitHub secrets
openai.api_key = os.getenv("OPENAI_API_KEY")

BASE_URL = "https://www.yellow.com.mt/hotels-in-malta/"
headers = {"User-Agent": "Mozilla/5.0"}

def scrape_hotels():
    print("Scraping hotel listings...")
    hotels = []
    for page in range(1, 6):  # Adjust page count as needed
        url = f"{BASE_URL}?page={page}"
        r = requests.get(url, headers=headers)
        soup = BeautifulSoup(r.text, "html.parser")
        listings = soup.select(".ListingCard")
        if not listings:
            break
        for card in listings:
            name = card.select_one(".ListingName").text.strip() if card.select_one(".ListingName") else ""
            address = card.select_one(".ListingAddress").text.strip() if card.select_one(".ListingAddress") else ""
            phone = card.select_one(".ListingPhone").text.strip() if card.select_one(".ListingPhone") else ""
            hotels.append({
                "name": name,
                "full_address": address,
                "phone": phone
            })
        time.sleep(2)
    return hotels

def enrich_with_openai(hotels):
    print("Enriching hotel data with AI descriptions...")
    results = []
    for hotel in hotels:
        prompt = f"""
        You are a Malta hotel content specialist. Write a compelling, sensory-rich, SEO-optimised hotel profile for VisitMalta.co.uk in pure HTML.
        Use the following data:
        Name: {hotel['name']}
        Address: {hotel['full_address']}
        Do not invent details — keep it factual but vivid and emotional.
        """

        try:
            response = openai.ChatCompletion.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "You are a professional Malta hotel content writer."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.8
            )
            description_html = response.choices[0].message.content.strip()
        except Exception as e:
            description_html = f"Error generating content: {e}"

        results.append({
            "name": hotel["name"],
            "full_address": hotel["full_address"],
            "description_html": description_html
        })
        time.sleep(5)
    return results

def save_to_csv(hotels):
    df = pd.DataFrame(hotels)
    df.to_csv("hotels_enriched.csv", index=False, encoding="utf-8-sig")
    print("✅ hotels_enriched.csv created successfully!")

if __name__ == "__main__":
    hotels = scrape_hotels()
    enriched = enrich_with_openai(hotels)
    save_to_csv(enriched)
