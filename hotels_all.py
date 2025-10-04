import os
import re
import time
import json
import requests
import pandas as pd
from bs4 import BeautifulSoup
import openai

# ==============================
# CONFIGURATION
# ==============================
LIST_URL = "https://www.yellow.com.mt/hotels/?page={}"
HEADERS = {"User-Agent": "Mozilla/5.0"}
OUT_CSV = "hotels_enriched.csv"

SLEEP_LIST = 0.8
SLEEP_DETAIL = 0.8

openai.api_key = os.getenv("OPENAI_API_KEY", "")
MODEL = "gpt-3.5-turbo"

# ==============================
# SCRAPER UTILITIES
# ==============================

def norm(s):
    return re.sub(r"\s+", " ", (s or "").strip())

def abs_url(href):
    if not href:
        return ""
    if href.startswith("http"):
        return href
    if href.startswith("/"):
        return "https://www.yellow.com.mt" + href
    return "https://www.yellow.com.mt/" + href

def collect_list_links(html):
    soup = BeautifulSoup(html, "html.parser")
    anchors = soup.select("a.business-name, div.business-card a, h2 a, .business-listing a")
    links = []
    for a in anchors:
        href = abs_url(a.get("href"))
        if "/hotels/" in href and "/search/" not in href:
            links.append(href)
    return list(dict.fromkeys(links))

def parse_jsonld(soup):
    name, addr, phone, site = "", "", "", ""
    for script in soup.find_all("script", {"type": "application/ld+json"}):
        try:
            data = json.loads(script.string)
            if isinstance(data, list):
                for d in data:
                    if isinstance(d, dict):
                        name = name or d.get("name", "")
                        if isinstance(d.get("address"), dict):
                            addr = addr or ", ".join([d["address"].get(k, "") for k in ["streetAddress","addressLocality","postalCode","addressCountry"]])
                        phone = phone or d.get("telephone", "")
                        site = site or d.get("url", "")
            elif isinstance(data, dict):
                name = name or data.get("name", "")
        except Exception:
            continue
    return norm(name), norm(addr), norm(phone), norm(site)

def fallback_contact(soup):
    addr = ""
    tel = ""
    site = ""
    contact = soup.select_one(".contact-information, .business-contact, .address")
    if contact:
        addr = norm(contact.get_text(" ", strip=True))
    t = soup.select_one("a[href^='tel:']")
    if t:
        tel = re.sub(r"^tel:", "", t["href"])
    for a in soup.find_all("a", href=True):
        if a["href"].startswith("http") and "yellow.com.mt" not in a["href"]:
            site = a["href"]
            break
    return addr, tel, site

def split_area(addr):
    if not addr or "," not in addr:
        return "", ""
    parts = [p.strip() for p in addr.split(",")]
    location = parts[-1]
    area = parts[-2] if len(parts) > 1 else ""
    return area, location

def get_detail(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=30)
        soup = BeautifulSoup(r.text, "html.parser")
    except Exception:
        return {}
    name, addr, phone, site = parse_jsonld(soup)
    if not addr:
        addr2, phone2, site2 = fallback_contact(soup)
        addr = addr or addr2
        phone = phone or phone2
        site = site or site2
    area, location = split_area(addr)
    return {
        "name": name,
        "full_address": addr,
        "location": location,
        "area": area,
        "stars": "",
        "licence_ref": "",
        "bedrooms": "",
        "apartments": "",
        "url": url
    }

# ==============================
# AI ENRICHMENT PROMPT
# ==============================

PROMPT_TEMPLATE = """
YOUR MISSION: You are a Malta hotel content specialist creating compelling, SEO-optimized hotel profiles that match the client's exceptional marketing style.

YOUR ROLE: You are a Malta hotel content specialist creating accurate, marketing-optimized hotel profiles for VisitMalta.co.uk so that the content creation helps the SEO and AI listings with CGPT, DeepSeek, Claude and Gemini.

DATA SOURCE: Malta Tourism Authority (MTA) licence holder screenshots.

CRITICAL REQUIREMENTS:
· 100% accuracy guaranteed - only facts from MTA listed addresses
· No invented amenities or features

CLIENT'S PROVEN MARKETING STYLE (MUST MATCH):
· Emotional, narrative-driven openings ("Where Malta Comes Alive")
· Sensory, specific details (luzzu fishing boats, golden stone walls at sunset)
· Strong metaphors ("front-row seat," "sanctuary of marble elegance")
· Location storytelling that makes readers FEEL the experience
· Balance between local energy and hotel tranquility

1. MARKETING QUALITY STANDARD:
· Write in compelling, emotional marketing language that sells the experience
· Use descriptive, sensory language that makes readers visualize their stay
· Create unique, engaging openings for each hotel (no templates)
· Balance location benefits with hotel comfort
· Target specific traveler types appropriately

OUTPUT: SINGLE MASTER CSV WITH COLUMNS:
name, full_address, location, area, stars, licence_ref, bedrooms, apartments, description_html

CONTENT CREATION - EXACT HTML STRUCTURE:
{html_template}

VARIETY STRATEGY - USE DIFFERENT OPENINGS:
· "Where [Location] Comes Alive"
· "The Heart of [Area]'s [Character]"
· "[Hotel Name] - Your [Adjective] Retreat in [Location]"
· "Discover [Location]'s [Quality] at [Hotel Name]"

SEO REQUIREMENTS:
· Naturally include: "Malta," location names, "hotel," star rating
· Use semantic keywords: "accommodation," "stay," "booking," "Mediterranean"
· Include area context: "St Julian's nightlife," "Sliema shopping," "Valletta views"

MANDATORY:
· Every description must feel UNIQUE and specific
· Use ACTUAL location features from the area
· Include sensory language (sights, sounds, atmospheres)
· Create emotional connection before mentioning amenities
· Use PURE HTML ONLY - NO MARKDOWN (no ###, **, -, ⭐)
· Ensure all HTML tags properly closed

FINAL OUTPUT:
Provide ONLY the HTML block ready for insertion into the description_html column.
"""

HTML_TEMPLATE = """
<h3>{name} | Hotel in {location}</h3>

<p><strong>[Compelling Opening Tagline]</strong></p>

<p>[Emotional, sensory opening paragraph telling the STORY of staying there.]</p>

<p><strong>The Vibe:</strong> [Atmosphere]<br>
<strong>Perfect For:</strong> [Target audience]<br>
<strong>Key Location Benefits:</strong></p>
<ul>
<li>[Benefit 1]</li>
<li>[Benefit 2]</li>
<li>[Benefit 3]</li>
</ul>

<h4>Hotel Features & Atmosphere</h4>
<p>[Emotional connection paragraph]</p>

<h4>Amenities & Services</h4>
<p><strong>Hotel Facilities</strong></p>
<ul><li>[Generic facility]</li><li>[Generic facility]</li></ul>
<p><strong>Room Features</strong></p>
<ul><li>[Standard feature]</li><li>[Standard feature]</li></ul>

<h4>Location & Accessibility</h4>
<p><strong>📍 Address:</strong> {full_address}</p>
<p><strong>Within Walking Distance:</strong></p>
<ul><li>[Attraction]</li><li>[Attraction]</li></ul>
<p><strong>Transportation:</strong></p>
<ul><li>[Option]</li><li>[Option]</li></ul>

<h4>Guest Experiences</h4>
<p><strong>What Visitors Love</strong></p>
<ul><li>[Positive point]</li><li>[Positive point]</li></ul>
<p><strong>Local Insight</strong></p>
<ul><li>[Tip]</li><li>[Tip]</li></ul>

<p><strong>Ready to experience?</strong><br>
[BOOK NOW - KM Malta Airlines Packages]</p>
"""

def generate_description(row):
    if not openai.api_key:
        return ""
    prompt = PROMPT_TEMPLATE.format(html_template=HTML_TEMPLATE)
    prompt = prompt + f"\n\nHotel Data:\nName: {row['name']}\nAddress: {row['full_address']}\nLocation: {row['location']}\nArea: {row['area']}\n"
    try:
        resp = openai.ChatCompletion.create(
            model=MODEL,
            temperature=0.7,
            messages=[
                {"role": "system", "content": "You are a Malta tourism marketing writer."},
                {"role": "user", "content": prompt}
            ]
        )
        return resp["choices"][0]["message"]["content"].strip()
    except Exception as e:
        return f"<p>Error: {e}</p>"

# ==============================
# MAIN SCRAPER
# ==============================

def scrape_all():
    all_links = []
    page = 1
    while True:
        url = LIST_URL.format(page)
        r = requests.get(url, headers=HEADERS)
        links = collect_list_links(r.text)
        if not links:
            break
        all_links += links
        page += 1
        time.sleep(SLEEP_LIST)
    print(f"Found {len(all_links)} hotels.")
    rows = []
    for i, link in enumerate(all_links, 1):
        info = get_detail(link)
        html = generate_description(info)
        rows.append({**info, "description_html": html})
        if i % 10 == 0:
            print(f"{i} done")
        time.sleep(SLEEP_DETAIL)
    df = pd.DataFrame(rows)
    df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    print(f"✅ Saved {len(rows)} hotels to {OUT_CSV}")

if __name__ == "__main__":
    scrape_all()
