import os, re, time, json
from urllib.parse import urljoin, urlparse
import requests
import pandas as pd
from bs4 import BeautifulSoup

# =================== CONFIG ===================
BASE = "https://www.yellow.com.mt"
LIST_URL = BASE + "/hotels/?page="
HEADERS = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36"}
OUT_CSV = "hotels_ai_ready.csv"

SLEEP_LIST = 0.6
SLEEP_DETAIL = 0.9
MAX_PAGES = 10  # safety cap

# ============ OpenAI (AI layer) ============
try:
    from openai import OpenAI
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
    client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None
except Exception:
    client = None

USE_GPT = client is not None
MODEL = "gpt-4o-mini"

# ============ MARKETING PROMPT ============
MARKETING_PROMPT = """
YOUR MISSION: You are a Malta hotel content specialist creating compelling, SEO-optimised hotel profiles that match VisitMalta.co.uk’s exceptional marketing style.

YOUR ROLE: Create accurate, emotionally engaging, marketing-optimised hotel descriptions for each Malta hotel below. Every description must match the client’s tone and structure perfectly.

DATA SOURCE: Malta Tourism Authority (MTA) licence holder information.

STYLE GUIDE:
• Emotional, narrative-driven openings (“Where Malta Comes Alive”)
• Sensory, specific details (luzzu fishing boats, golden stone walls at sunset)
• Strong metaphors (“front-row seat,” “sanctuary of marble elegance”)
• Location storytelling that makes readers FEEL the experience
• Balance between local energy and hotel tranquillity

HTML OUTPUT STRUCTURE:
<h3>{Hotel Name} | {Star Rating} in {Location}</h3>
<p><strong>[Compelling Opening Line]</strong></p>
<p>[Emotional paragraph with sensory storytelling.]</p>

<p><strong>The Vibe:</strong> [Atmosphere]<br>
<strong>Perfect For:</strong> [Target audience]<br>
<strong>Key Location Benefits:</strong></p>
<ul>
<li>[Specific location detail]</li>
<li>[Specific location detail]</li>
<li>[Specific location detail]</li>
</ul>

<h4>Hotel Features & Atmosphere</h4>
<p>[Descriptive overview before listing amenities]</p>

<h4>Amenities & Services</h4>
<p><strong>Hotel Facilities</strong></p>
<ul>
<li>[Appropriate facility for star rating]</li>
<li>[Appropriate facility for star rating]</li>
</ul>

<p><strong>Room Features</strong></p>
<ul>
<li>[Standard room feature]</li>
<li>[Standard room feature]</li>
</ul>

<h4>Location & Accessibility</h4>
<p><strong>📍 Address:</strong> [Full Address]</p>
"""

# ============ SCRAPER ============
def scrape_hotels():
    hotels = []
    for page in range(1, MAX_PAGES + 1):
        url = f"{LIST_URL}{page}"
        print(f"🔎 Scraping list page {page}...")
        resp = requests.get(url, headers=HEADERS)
        if resp.status_code != 200:
            break
        soup = BeautifulSoup(resp.text, "html.parser")
        cards = soup.select("a.business-card")
        if not cards:
            break

        for card in cards:
            name = card.select_one(".business-name")
            link = card.get("href")
            if not name or not link:
                continue
            full_link = urljoin(BASE, link)
            name = name.text.strip()
            print(f"→ {name}")
            hotels.append({"name": name, "url": full_link})

        time.sleep(SLEEP_LIST)
    return hotels

# ============ HOTEL DETAIL PARSER ============
def parse_hotel_detail(url):
    resp = requests.get(url, headers=HEADERS)
    if resp.status_code != 200:
        return {}
    soup = BeautifulSoup(resp.text, "html.parser")
    addr = soup.select_one(".address")
    desc = soup.select_one(".description")
    area = soup.select_one(".location-name")
    return {
        "full_address": addr.text.strip() if addr else "",
        "description_raw": desc.text.strip() if desc else "",
        "location": area.text.strip() if area else "",
    }

# ============ AI DESCRIPTION GENERATOR ============
def generate_description(name, addr, location, raw_text):
    if not USE_GPT:
        return raw_text or ""
    prompt = f"{MARKETING_PROMPT}\n\nGenerate a complete HTML hotel profile for:\nHotel name: {name}\nAddress: {addr}\nLocation: {location}\n\nDetails from source:\n{raw_text}\n\nWrite full HTML output below."
    try:
        resp = client.chat.completions.create(
            model=MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7,
            max_tokens=1200,
        )
        return resp.choices[0].message.content.strip()
    except Exception as e:
        print(f"⚠️ AI failed for {name}: {e}")
        return raw_text or ""

# ============ MAIN ============
def main():
    hotels = scrape_hotels()
    rows = []
    for h in hotels:
        print(f"🏨 Processing: {h['name']}")
        detail = parse_hotel_detail(h["url"])
        html = generate_description(
            h["name"],
            detail.get("full_address", ""),
            detail.get("location", ""),
            detail.get("description_raw", "")
        )
        rows.append({
            "name": h["name"],
            "full_address": detail.get("full_address", ""),
            "location": detail.get("location", ""),
            "area": "",
            "stars": "",
            "licence_ref": "",
            "bedrooms": "",
            "apartments": "",
            "description_html": html
        })
        time.sleep(SLEEP_DETAIL)

    df = pd.DataFrame(rows)
    df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    print(f"✅ Wrote {len(df)} hotels to {OUT_CSV}")

if __name__ == "__main__":
    main()
