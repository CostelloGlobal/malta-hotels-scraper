import os, time, json, re
import requests
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

# =========================
# CONFIG
# =========================
BASE = "https://www.yellow.com.mt"
LIST_URL = BASE + "/hotels/?page={}"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (iPad; CPU OS 17_0 like Mac OS X) "
                  "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1"
}
OUT_CSV = "hotels_enriched.csv"
SLEEP_BETWEEN_REQ = 1.0  # seconds between requests

# =========================
# OpenAI (v1.x client)
# =========================
from openai import OpenAI
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
if not OPENAI_API_KEY:
    raise RuntimeError("OPENAI_API_KEY is not set in environment/secrets.")
client = OpenAI(api_key=OPENAI_API_KEY)
MODEL = "gpt-4o-mini"


def clean_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def absolutize(href: str) -> str:
    if not href:
        return ""
    return urljoin(BASE, href)


def get_listing_links(html: str) -> list[str]:
    """Return absolute detail links found on a listing page.
    We use broad selectors to survive site layout changes."""
    soup = BeautifulSoup(html, "html.parser")

    selectors = [
        "a.business-name",
        "div.business-card a",
        "h2 a",
        ".business-listing a",
        "article a",
    ]
    links = []
    for sel in selectors:
        for a in soup.select(sel):
            href = a.get("href", "")
            if not href:
                continue
            # Only keep internal business pages (start with '/')
            if href.startswith("/"):
                url = absolutize(href)
                # de-dup as we go
                if url not in links:
                    links.append(url)
    return links


def extract_from_jsonld(soup: BeautifulSoup) -> dict:
    """Try to pull name/address/website/phone from JSON-LD if present."""
    out = {"name": "", "address": "", "website": "", "phone": ""}

    for tag in soup.find_all("script", attrs={"type": "application/ld+json"}):
        try:
            data = json.loads(tag.string or "{}")
        except Exception:
            continue

        candidates = data if isinstance(data, list) else [data]
        for obj in candidates:
            if not isinstance(obj, dict):
                continue

            # name
            if not out["name"]:
                n = obj.get("name")
                if isinstance(n, str):
                    out["name"] = clean_space(n)

            # address
            addr = obj.get("address")
            if isinstance(addr, dict) and not out["address"]:
                bits = [
                    addr.get("streetAddress", ""),
                    addr.get("addressLocality", ""),
                    addr.get("postalCode", ""),
                    addr.get("addressCountry", ""),
                ]
                out["address"] = clean_space(", ".join([b for b in bits if b]))

            # url / sameAs
            if not out["website"]:
                cand = obj.get("url") or obj.get("sameAs")
                if isinstance(cand, str):
                    out["website"] = cand

            # telephone
            if not out["phone"]:
                tel = obj.get("telephone")
                if isinstance(tel, str):
                    out["phone"] = clean_space(tel)

    return out


def extract_phone_text(soup: BeautifulSoup) -> str:
    # tel: links
    tel = soup.select_one('a[href^="tel:"]')
    if tel:
        return clean_space(re.sub(r"^tel:", "", tel.get("href", "")))

    # fallback: visible +356 patterns
    text = soup.get_text(" ", strip=True)
    m = re.search(r"\+?356[\s-]?\d{3}[\s-]?\d{4}", text)
    if m:
        return m.group(0)
    return ""


def extract_website(soup: BeautifulSoup) -> str:
    # buttons/anchors that say Website
    for a in soup.find_all("a"):
        label = (a.get_text(" ", strip=True) or "").lower()
        if "website" in label or "visit website" in label:
            href = a.get("href", "")
            if href and "yellow.com.mt" not in urlparse(href).netloc:
                return href
    return ""


def get_detail(url: str) -> dict:
    """Fetch a hotel detail page and extract core facts."""
    r = requests.get(url, headers=HEADERS, timeout=30)
    soup = BeautifulSoup(r.text, "html.parser")

    # From JSON-LD first
    j = extract_from_jsonld(soup)
    name = j["name"] or ""
    address = j["address"] or ""
    website = j["website"] or ""
    phone = j["phone"] or ""

    # Fallbacks
    if not name:
        h1 = soup.find("h1")
        if h1:
            name = clean_space(h1.get_text(" ", strip=True))

    if not address:
        block = soup.select_one(".contact-information, .business-contact, .address, [itemprop='address']")
        if block:
            address = clean_space(block.get_text(" ", strip=True))

    if not website:
        website = extract_website(soup)

    if not phone:
        phone = extract_phone_text(soup)

    # Split area/location heuristically (safer than inventing)
    area, location = "", ""
    if address and "," in address:
        parts = [p.strip() for p in address.split(",") if p.strip()]
        location = parts[-1] if parts else ""
        area = parts[-2] if len(parts) > 1 else ""

    return {
        "name": name,
        "full_address": address,
        "location": location,
        "area": area,
        "phone": phone,
        "website": website,
        "url": url,
    }


def build_prompt(row: dict) -> str:
    """Your exact enrichment brief – model must return ONLY the HTML block."""
    return f"""
YOUR MISSION: You are a Malta hotel content specialist creating compelling, SEO-optimized hotel profiles that match the client's exceptional marketing style.

YOUR ROLE: You are a Malta hotel content specialist creating accurate, marketing-optimized hotel profiles for VisitMalta.co.uk so that the content creation helps the SEO and AI listings with CGPT, DeepSeek, Claude and Gemini.

DATA SOURCE: Malta Tourism Authority (MTA) licence holder screenshots.
IMPORTANT: Use ONLY the facts provided below (name, full_address, location, area). If any field is missing, leave it out—do NOT invent amenities, ratings, facilities, or specifics.

FACTS:
- name: {row.get('name','')}
- full_address: {row.get('full_address','')}
- location: {row.get('location','')}
- area: {row.get('area','')}

CRITICAL REQUIREMENTS:
- 100% accuracy: Do NOT invent amenities, star ratings, room counts, pools, gyms, spas, views, or restaurant names if not stated above.
- You may describe ambience and location feel in general, Malta-appropriate terms (e.g., Mediterranean light, limestone streets), but avoid claiming facilities.
- Use PURE HTML only (no markdown). Close all tags.

CLIENT'S PROVEN MARKETING STYLE:
- Emotional, narrative-driven openings ("Where Malta Comes Alive")
- Sensory, specific details (e.g., luzzu fishing boats, golden stone walls at sunset)
- Strong metaphors ("front-row seat," "sanctuary of marble elegance")
- Location storytelling that makes readers FEEL the experience
- Balance between local energy and hotel tranquility

OUTPUT COLUMNS (we will place your HTML into description_html):
name, full_address, location, area, stars, licence_ref, bedrooms, apartments, description_html

CONTENT CREATION - EXACT HTML STRUCTURE (return ONLY this HTML):

<h3>{row.get('name','')} | Hotel in {row.get('location','')}</h3>

<p><strong>[Compelling Opening Tagline]</strong></p>

<p>[Emotional, sensory opening paragraph telling the STORY of staying there in Malta. Use Malta-true mood and scenery without inventing facilities.]</p>

<p><strong>The Vibe:</strong> [Atmosphere without facility claims]<br>
<strong>Perfect For:</strong> [Types of traveller, phrased safely]<br>
<strong>Key Location Benefits:</strong></p>
<ul>
<li>[Sensory, place-based benefit grounded in address/area]</li>
<li>[Sensory, place-based benefit grounded in address/area]</li>
<li>[Sensory, place-based benefit grounded in address/area]</li>
</ul>

<h4>Hotel Features & Atmosphere</h4>
<p>[Focus on feel and style; do NOT assert amenities that aren't given]</p>

<h4>Amenities & Services</h4>
<p><strong>Hotel Facilities</strong></p>
<ul>
<li>[Keep generic and safe—e.g., “welcoming common areas” if you cannot confirm specifics]</li>
<li>[Avoid listing pools/gyms/spas unless in facts]</li>
</ul>
<p><strong>Room Features</strong></p>
<ul>
<li>[Safe, non-specific phrasing—e.g., “comfortable accommodation”]</li>
<li>[Do not claim views, sizes, or fixtures you can't confirm]</li>
</ul>

<h4>Location & Accessibility</h4>
<p><strong>📍 Address:</strong> {row.get('full_address','')}</p>
<p><strong>Within Walking Distance:</strong></p>
<ul>
<li>[Use realistic local context; avoid naming exact attractions unless obvious from area]</li>
<li>[Keep times generic if unsure: “a short walk”]</li>
</ul>
<p><strong>Transportation:</strong></p>
<ul>
<li>[General Malta options: buses/taxis common in tourist areas]</li>
<li>[Keep phrasing safe and accurate without specifics]</li>
</ul>

<h4>Guest Experiences</h4>
<p><strong>What Visitors Love</strong></p>
<ul>
<li>[General, emotional positives—setting, hospitality, convenience]</li>
<li>[Do not cite ratings or reviews]</li>
</ul>
<p><strong>Local Insight</strong></p>
<ul>
<li>[Sensory local tip in the area, phrased safely]</li>
<li>[Another sensory local tip]</li>
</ul>

<p><strong>Ready to discover Malta?</strong><br>
[BOOK NOW - KM Malta Airlines Packages]</p>
""".strip()


def generate_description(row: dict) -> str:
    system = (
        "You are a Malta hotel content specialist and meticulous fact-checker. "
        "Never invent amenities or specifics; keep language vivid but safe."
    )
    try:
        resp = client.chat.completions.create(
            model=MODEL,
            temperature=0.7,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": build_prompt(row)},
            ],
        )
        html = resp.choices[0].message.content.strip()
        # strip accidental fences if any
        html = re.sub(r"^```html|```$", "", html, flags=re.IGNORECASE | re.MULTILINE).strip()
        return html
    except Exception as e:
        return f"<p>Error generating description: {str(e)}</p>"


def scrape_all() -> list[dict]:
    print("🔍 Scraping Yellow Malta hotel listings…")
    detail_urls = []
    page = 1
    while True:
        url = LIST_URL.format(page)
        print(f" • Page {page}: {url}")
        r = requests.get(url, headers=HEADERS, timeout=30)
        if r.status_code != 200:
            print(f"   ⚠️ Page returned {r.status_code}. Stopping.")
            break

        links = get_listing_links(r.text)
        if not links:
            print("   ✅ No more listings on this page. Done.")
            break

        # accumulate
        for u in links:
            if u not in detail_urls:
                detail_urls.append(u)

        page += 1
        time.sleep(SLEEP_BETWEEN_REQ)

    detail_urls = sorted(set(detail_urls))
    print(f"🔗 Found {len(detail_urls)} hotel pages to visit.")

    rows = []
    for i, durl in enumerate(detail_urls, 1):
        try:
            row = get_detail(durl)
        except Exception as e:
            print(f"   ⚠️ Error on {durl}: {e}")
            row = {
                "name": "",
                "full_address": "",
                "location": "",
                "area": "",
                "phone": "",
                "website": "",
                "url": durl,
            }
        rows.append(row)
        if i % 10 == 0 or i == len(detail_urls):
            print(f"   …{i}/{len(detail_urls)} details collected")
        time.sleep(SLEEP_BETWEEN_REQ)
    return rows


def main():
    # 1) Scrape
    raw_rows = scrape_all()
    if not raw_rows:
        cols = [
            "name",
            "full_address",
            "location",
            "area",
            "stars",
            "licence_ref",
            "bedrooms",
            "apartments",
            "description_html",
        ]
        pd.DataFrame(columns=cols).to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
        print("⚠️ No rows scraped. Wrote empty CSV headers.")
        return

    # 2) Enrich – your marketing HTML (no inventions)
    print("🧠 Generating marketing descriptions…")
    final_rows = []
    for i, r in enumerate(raw_rows, 1):
        html = generate_description(r)
        final_rows.append(
            {
                "name": r["name"],
                "full_address": r["full_address"],
                "location": r["location"],
                "area": r["area"],
                "stars": "",  # unknown
                "licence_ref": "",  # unknown
                "bedrooms": "",  # unknown
                "apartments": "",  # unknown
                "description_html": html,
            }
        )
        if i % 5 == 0 or i == len(raw_rows):
            print(f"   …{i}/{len(raw_rows)} descriptions")
        time.sleep(0.2)

    # 3) Save CSV for Make/Hive
    df = pd.DataFrame(
        final_rows,
        columns=[
            "name",
            "full_address",
            "location",
            "area",
            "stars",
            "licence_ref",
            "bedrooms",
            "apartments",
            "description_html",
        ],
    )
    df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    print(f"✅ Done. Wrote {len(df)} rows to {OUT_CSV}")


if __name__ == "__main__":
    main()
