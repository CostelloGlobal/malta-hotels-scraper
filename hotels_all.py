import os
import re
import time
import json
from urllib.parse import urljoin, urlparse
import requests
import pandas as pd
from bs4 import BeautifulSoup
import openai

# ==============================
# CONFIG
# ==============================
BASE = "https://www.yellow.com.mt"
LIST_URL = BASE + "/hotels/?page={}"
HEADERS = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36"}
OUT_CSV = "hotels_enriched.csv"

SLEEP_LIST = 0.5
SLEEP_DETAIL = 0.8
MAX_PAGES = 20

# ——— GPT (v0.28.1 syntax) ———
openai.api_key = os.getenv("OPENAI_API_KEY", "")
USE_GPT = bool(openai.api_key) and True  # set False to skip enrichment

MARKETING_PROMPT = """
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

VARIETY STRATEGY:
Use different openings like: "Where [Location] Comes Alive" / "The Heart of [Area]'s [Character]" / "[Hotel Name] - Your [Adjective] Retreat in [Location]" / "Discover [Location]'s [Quality] at [Hotel Name]"

SEO REQUIREMENTS:
Include “Malta”, location names, “hotel”, star rating; semantic keywords: accommodation, stay, booking, Mediterranean; area context like St Julian's nightlife, Sliema shopping, Valletta views.

MANDATORY:
Every description must be UNIQUE and specific; only use actual location features; sensory language; emotional before amenities; PURE HTML ONLY (no markdown); close all tags.

If any factual fields are missing (stars, licence_ref, bedrooms, apartments), leave them blank — do NOT invent.
"""

# ==============================
# HELPERS
# ==============================
def norm_url(href: str) -> str:
    if not href:
        return ""
    full = urljoin(BASE, href)
    full = full.split("#")[0].split("?")[0].rstrip("/")
    return full

def is_internal_yellow(href: str) -> bool:
    try:
        u = urlparse(norm_url(href))
        return u.netloc.endswith("yellow.com.mt")
    except Exception:
        return False

def looks_like_detail_path(href: str) -> bool:
    """
    Accept internal detail pages like:
      https://www.yellow.com.mt/<slug>-hotel.../
    Reject phone/map/share/book/etc.
    """
    p = norm_url(href)
    bad = ("/book", "/call", "/map", "/share", "/directions", "/reviews")
    if any(b in p for b in bad):
        return False
    # Needs at least two path segments to be a profile
    try:
        path = urlparse(p).path.strip("/")
        return path.count("/") >= 1
    except Exception:
        return False

def extract_listing_links(soup: BeautifulSoup) -> list[str]:
    links = []
    # Primary: real card container
    cards = soup.select("div[data-testid='business-list-card']")
    # Fallbacks in case YP tweaks attributes
    if not cards:
        cards = soup.select("div.business-card, .business-listing, article, li")

    for card in cards:
        chosen = None
        for a in card.select("a[href]"):
            href = a.get("href", "").strip()
            if not href:
                continue
            if not is_internal_yellow(href):
                # skip external (Booking.com, etc.)
                continue
            if looks_like_detail_path(href):
                chosen = norm_url(href)
                break
        if not chosen:
            # last resort — first internal link
            for a in card.select("a[href]"):
                href = a.get("href", "").strip()
                if is_internal_yellow(href):
                    chosen = norm_url(href)
                    break
        if chosen:
            links.append(chosen)
    # unique, keep order
    seen = set()
    uniq = []
    for u in links:
        if u not in seen:
            seen.add(u)
            uniq.append(u)
    return uniq

def text_of(el):
    return re.sub(r"\s+", " ", el.get_text(" ", strip=True)) if el else ""

def scrape_detail(url: str) -> dict:
    time.sleep(SLEEP_DETAIL)
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    s = BeautifulSoup(r.text, "html.parser")

    name = text_of(s.select_one("h1")) or text_of(s.select_one("h2"))
    # Try common address blocks
    addr = ""
    address_el = s.select_one("address")
    if not address_el:
        address_el = s.select_one("[itemprop='address']") or s.select_one(".address, .biz-address")
    addr = text_of(address_el)

    # Try to get locality/area hints from chips/breadcrumbs
    location = ""
    bread = s.select(".breadcrumbs a, nav.breadcrumb a, .breadcrumb a")
    if bread:
        crumbs = [text_of(b) for b in bread if text_of(b)]
        location = ", ".join(crumbs[-2:]) if len(crumbs) >= 2 else ", ".join(crumbs)

    # Cheap star guess from icon text if present (do NOT invent)
    stars = ""
    star_el = s.find(string=re.compile(r"\b[1-5]\s*star", re.I))
    if star_el:
        m = re.search(r"\b([1-5])\s*star", star_el, flags=re.I)
        if m:
            stars = m.group(1)

    data = {
        "name": name,
        "full_address": addr,
        "location": location,
        "area": "",
        "stars": stars,
        "licence_ref": "",
        "bedrooms": "",
        "apartments": "",
        "url": url,
    }
    return data

def enrich_with_gpt(row: dict) -> str:
    # Build the minimal facts block we actually have
    facts = {
        "name": row.get("name", ""),
        "address": row.get("full_address", ""),
        "location": row.get("location", ""),
        "area": row.get("area", ""),
        "stars": row.get("stars", ""),
        "licence_ref": row.get("licence_ref", ""),
        "bedrooms": row.get("bedrooms", ""),
        "apartments": row.get("apartments", ""),
    }
    user_msg = (
        "Create the description_html exactly per the HTML structure, "
        "using ONLY these known facts. Leave any unknown field blank in the HTML, "
        "and do NOT invent amenities or numbers.\n\n"
        f"FACTS (JSON): {json.dumps(facts, ensure_ascii=False)}"
    )

    try:
        resp = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": MARKETING_PROMPT},
                {"role": "user", "content": user_msg},
            ],
            temperature=0.7,
        )
        return resp["choices"][0]["message"]["content"].strip()
    except Exception as e:
        return ""

# ==============================
# MAIN
# ==============================
def main():
    all_links = []
    seen = set()
    page = 1

    while page <= MAX_PAGES:
        url = LIST_URL.format(page)
        print(f"🟡 Page {page}: {url}")
        res = requests.get(url, headers=HEADERS, timeout=30)
        if res.status_code >= 400:
            print(f"❌ Failed page {page} ({res.status_code}). Stopping.")
            break
        soup = BeautifulSoup(res.text, "html.parser")
        page_links = extract_listing_links(soup)
        # Filter out ones we've already seen
        new_links = [u for u in page_links if u not in seen]

        print(f"   • found {len(page_links)} candidate links, {len(new_links)} new after filtering.")
        for i, u in enumerate(new_links, 1):
            print(f"     {i}. {u.split('/')[-1].replace('-', ' ').title()}")

        if not new_links:
            # If page had zero new items, try one more page; if again zero, stop
            if page > 1:
                print("✅ No NEW hotel links on this page. Stopping.")
                break
        for u in new_links:
            seen.add(u)
            all_links.append(u)

        page += 1
        time.sleep(SLEEP_LIST)

    if not all_links:
        print("❌ No hotel links found. Check selectors.")
        return

    rows = []
    for idx, link in enumerate(all_links, 1):
        print(f"🔎 [{idx}/{len(all_links)}] {link}")
        try:
            row = scrape_detail(link)
            if USE_GPT:
                row["description_html"] = enrich_with_gpt(row)
            else:
                row["description_html"] = ""
            rows.append(row)
        except Exception as e:
            print(f"   ⚠️ Skipped {link}: {e}")

    df = pd.DataFrame(rows, columns=[
        "name","full_address","location","area","stars","licence_ref","bedrooms","apartments","description_html","url"
    ])
    df.to_csv(OUT_CSV, index=False, encoding="utf-8")
    print(f"✅ Wrote {len(df)} rows to {OUT_CSV}")

if __name__ == "__main__":
    main()
