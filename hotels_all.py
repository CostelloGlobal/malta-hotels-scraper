import os, re, time, json, requests
import pandas as pd
from bs4 import BeautifulSoup
import openai

# ======================
# CONFIG
# ======================
BASE_URL = "https://www.yellow.com.mt/hotels/?page={}"
HEADERS = {"User-Agent": "Mozilla/5.0"}
OUT_CSV = "hotels_enriched.csv"

# polite pacing (tweak if needed)
SLEEP_LIST = 0.3       # between list pages
SLEEP_DETAIL = 0.4     # between hotel detail requests

# safety stop so it never runs forever even if site structure changes
HARD_STOP_PAGES = 200

# OpenAI (v0.28.x)
openai.api_key = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = "gpt-3.5-turbo"

# Broad selector, then we filter by URL pattern
CARD_SELECTOR = "a.business-name, div.business-card a, h2 a, .business-listing a"

# ======================
# PROMPT (your marketing brief)
# ======================
MARKETING_BRIEF = """
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
<h3>Hotel Name | Star Rating in Location</h3>
<p><strong>[Compelling Opening Tagline]</strong></p>
<p>[Emotional, sensory opening paragraph telling the STORY of staying there. Use specific details, metaphors, and make readers visualize themselves at the hotel.]</p>
<p><strong>The Vibe:</strong> [Atmosphere description]<br>
<strong>Perfect For:</strong> [Target audience]<br>
<strong>Key Location Benefits:</strong></p>
<ul>
<li>[Specific, sensory location benefit]</li>
<li>[Specific, sensory location benefit]</li>
<li>[Specific, sensory location benefit]</li>
</ul>
<h4>Hotel Features & Atmosphere</h4>
<p>[Description that creates emotional connection before mentioning amenities]</p>
<h4>Amenities & Services</h4>
<p><strong>Hotel Facilities</strong></p>
<ul>
<li>[Generic facility appropriate for star rating]</li>
<li>[Generic facility appropriate for star rating]</li>
</ul>
<p><strong>Room Features</strong></p>
<ul>
<li>[Standard room feature]</li>
<li>[Standard room feature]</li>
</ul>
<h4>Location & Accessibility</h4>
<p><strong>📍 Address:</strong> [Full Address]</p>
<p><strong>Within Walking Distance:</strong></p>
<ul>
<li>[Actual attraction] ([realistic time] walk)</li>
<li>[Actual attraction] ([realistic time] walk)</li>
</ul>
<p><strong>Transportation:</strong></p>
<ul>
<li>[Actual transport option]</li>
<li>[Actual transport option]</li>
</ul>
<h4>Guest Experiences</h4>
<p><strong>What Visitors Love</strong></p>
<ul>
<li>[Realistic positive point with emotional language]</li>
<li>[Realistic positive point with emotional language]</li>
</ul>
<p><strong>Local Insight</strong></p>
<ul>
<li>[Useful local tip with sensory details]</li>
<li>[Useful local tip with sensory details]</li>
</ul>
<p><strong>Ready to [experience]?</strong><br>
[BOOK NOW - KM Malta Airlines Packages]</p>

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
"""

# ======================
# HELPERS
# ======================
def norm(s):
    return " ".join((s or "").split())

def abs_url(href):
    if not href:
        return ""
    if href.startswith("http"):
        return href
    if href.startswith("//"):
        return "https:" + href
    if href.startswith("/"):
        return "https://www.yellow.com.mt" + href
    return "https://www.yellow.com.mt/" + href.lstrip("./")

def is_hotel_link(href):
    if not href:
        return False
    href = href.lower()
    # strict filter: we only keep real business pages in the Hotels category
    if "yellow.com.mt" in href and "/hotels/" in href and "mailto:" not in href and "tel:" not in href:
        return True
    return False

def get(url, kind="page"):
    r = requests.get(url, headers=HEADERS, timeout=25)
    r.raise_for_status()
    return r

def extract_address_area(soup):
    # best-effort parse — keeps blanks if not found (never invents)
    text = soup.get_text(" ", strip=True)
    address = ""
    area = ""

    # Try microdata/address tag
    addr_tag = soup.find(["address"])
    if addr_tag:
        address = norm(addr_tag.get_text(" ", strip=True))

    # fallback: common patterns on Yellow
    if not address:
        m = re.search(r"Address\s*:?\s*(.+?)(?:\s{2,}|Tel|Phone|Website)", text, flags=re.I)
        if m:
            address = norm(m.group(1))

    # area heuristic: last token after comma in address (e.g., "St Julian's")
    if address and "," in address:
        area = norm(address.split(",")[-1])

    return address, area

def ai_enrich(name, address, area, url):
    """Generate description_html using the pinned OpenAI v0.28.x client."""
    sys = MARKETING_BRIEF.strip()
    user = json.dumps({
        "name": name,
        "full_address": address,
        "area": area,
        "source_url": url
    }, ensure_ascii=False)

    for attempt in range(4):
        try:
            resp = openai.ChatCompletion.create(
                model=OPENAI_MODEL,
                temperature=0.7,
                messages=[
                    {"role": "system", "content": sys},
                    {"role": "user", "content": f"Create the HTML description for this hotel using ONLY factual address/location details you can infer safely. If unsure, leave fields generic but true-to-rating/area. Data: {user}"}
                ]
            )
            return resp["choices"][0]["message"]["content"]
        except Exception as e:
            wait = 2 * (attempt + 1)
            print(f"⚠️ OpenAI retry {attempt+1}: {e} (sleep {wait}s)")
            time.sleep(wait)
    return ""

# ======================
# MAIN SCRAPER
# ======================
def scrape_all():
    seen_links = set()
    rows = []
    total_found = 0

    for page in range(1, HARD_STOP_PAGES + 1):
        url = BASE_URL.format(page)
        print(f"🟡 Page {page}: {url}")
        try:
            res = get(url, "list")
        except Exception as e:
            print(f"⚠️ List request failed (page {page}): {e}")
            break

        soup = BeautifulSoup(res.text, "html.parser")
        anchors = soup.select(CARD_SELECTOR)

        # filter + normalize
        page_links = []
        for a in anchors:
            href = abs_url(a.get("href", ""))
            if not is_hotel_link(href):
                continue
            txt = norm(a.get_text())
            page_links.append((txt, href))

        # de-dup (page + global)
        new_this_page = []
        for name, href in page_links:
            if href not in seen_links:
                seen_links.add(href)
                new_this_page.append((name, href))

        print(f"   → found {len(page_links)} candidate links, {len(new_this_page)} new after filtering.")

        if not new_this_page:
            print("✅ No NEW hotel links on this page. Stopping.")
            break

        # visit each hotel once
        for (name, link) in new_this_page:
            try:
                hres = get(link, "detail")
                hsoup = BeautifulSoup(hres.text, "html.parser")
                address, area = extract_address_area(hsoup)

                # never invent stars/licence/bedrooms/apartments — keep blank unless you add reliable parsing later
                description_html = ai_enrich(name or "", address or "", area or "", link)

                rows.append({
                    "name": name or "",
                    "full_address": address or "",
                    "location": "",                 # left blank (no invention)
                    "area": area or "",
                    "stars": "",
                    "licence_ref": "",
                    "bedrooms": "",
                    "apartments": "",
                    "description_html": description_html or "",
                    "url": link
                })

                total_found += 1
                print(f"      ✓ {total_found}. {name or '(no name)'}")

            except Exception as e:
                print(f"      ✗ Failed {link}: {e}")

            time.sleep(SLEEP_DETAIL)

        time.sleep(SLEEP_LIST)

    # write CSV
    if rows:
        df = pd.DataFrame(rows, columns=[
            "name","full_address","location","area","stars",
            "licence_ref","bedrooms","apartments","description_html","url"
        ])
        df.to_csv(OUT_CSV, index=False, encoding="utf-8")
        print(f"✅ Wrote {len(rows)} rows to {OUT_CSV}")
    else:
        # still write headers so the artifact exists
        pd.DataFrame(columns=[
            "name","full_address","location","area","stars",
            "licence_ref","bedrooms","apartments","description_html","url"
        ]).to_csv(OUT_CSV, index=False, encoding="utf-8")
        print("⚠️ No rows scraped. Wrote empty CSV headers.")

if __name__ == "__main__":
    scrape_all()
