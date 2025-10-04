import os, time, json, re
import requests
import pandas as pd
from bs4 import BeautifulSoup

# ---------- CONFIG ----------
LIST_URL = "https://www.yellow.com.mt/hotels/?page={}"
HEADERS = {"User-Agent": "Mozilla/5.0"}
OUT_CSV = "hotels_enriched.csv"
MAX_PAGES = 80          # safety upper-bound; stops when no more results
SLEEP_BETWEEN_REQ = 1.0 # be polite to the site
# ---------------------------

# ---------- OpenAI (v1.x client) ----------
from openai import OpenAI
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
client = OpenAI(api_key=OPENAI_API_KEY)
MODEL = "gpt-4o-mini"
# ------------------------------------------


def unique(seq):
    seen = set()
    out = []
    for x in seq:
        key = (x.get("name","").strip(), x.get("url","").strip())
        if key in seen: 
            continue
        seen.add(key)
        out.append(x)
    return out


def get_detail(url: str):
    """
    Fetch a business detail page and extract:
      - name
      - full_address (prefer JSON-LD PostalAddress if present)
    """
    try:
        r = requests.get(url, headers=HEADERS, timeout=30)
        soup = BeautifulSoup(r.text, "html.parser")

        # Name (prefer <h1>)
        name = ""
        h1 = soup.find("h1")
        if h1:
            name = h1.get_text(strip=True)

        # Address: try JSON-LD first
        full_address = ""
        for tag in soup.find_all("script", attrs={"type": "application/ld+json"}):
            try:
                data = json.loads(tag.string or "{}")
                # JSON-LD can be a list or dict
                candidates = data if isinstance(data, list) else [data]
                for obj in candidates:
                    if isinstance(obj, dict) and "address" in obj:
                        addr = obj["address"]
                        if isinstance(addr, dict):
                            # Build a clean single-line address
                            bits = [
                                addr.get("streetAddress",""),
                                addr.get("addressLocality",""),
                                addr.get("postalCode",""),
                                addr.get("addressCountry","")
                            ]
                            full_address = ", ".join([b for b in bits if b])
                            break
                if full_address:
                    break
            except Exception:
                continue

        # Fallback to a visible contact block if JSON-LD absent
        if not full_address:
            contact = soup.select_one(".contact-information, .business-contact, .address")
            if contact:
                full_address = " ".join(contact.get_text(" ", strip=True).split())

        # last tidy
        full_address = re.sub(r"\s+", " ", full_address).strip()

        return name or "", full_address or ""
    except Exception:
        return "", ""


def split_area_location(full_address: str):
    """
    Heuristic: area = second-to-last comma part, location = last comma part (e.g. 'Sliema, Malta').
    If we can’t reliably split, leave blanks (safer than inventing).
    """
    if not full_address or "," not in full_address:
        return "", ""
    parts = [p.strip() for p in full_address.split(",") if p.strip()]
    location = parts[-1] if parts else ""
    area = parts[-2] if len(parts) > 1 else ""
    return area, location


def scrape_all_hotels():
    """
    Crawl the hotels listing pages, collect detail URLs, then pull details from each page.
    """
    rows = []
    print("🔍 Scraping Yellow Malta hotel listings…")

    # 1) collect all business links from paginated list
    detail_links = []
    for page in range(1, MAX_PAGES + 1):
        url = LIST_URL.format(page)
        print(f" • Page {page}: {url}")
        resp = requests.get(url, headers=HEADERS, timeout=30)
        if resp.status_code != 200:
            print(f"   ⚠️ Page returned {resp.status_code}, stopping.")
            break

        soup = BeautifulSoup(resp.text, "html.parser")
        # Each card has an h2 > a to detail page
        cards = soup.select("div[data-testid='business-list-card'] h2 a")
        if not cards:
            print("   ✅ No more listings on this page. Done.")
            break

        for a in cards:
            href = a.get("href", "")
            if not href: 
                continue
            if href.startswith("/"):
                href = "https://www.yellow.com.mt" + href
            if href.endswith("/"):
                detail_links.append(href)
        time.sleep(SLEEP_BETWEEN_REQ)

    detail_links = sorted(set(detail_links))
    print(f"🔗 Found {len(detail_links)} hotel pages to visit.")

    # 2) visit each detail page to get clean name and address
    for i, durl in enumerate(detail_links, 1):
        name, full_address = get_detail(durl)
        name = name or ""  # avoid None
        full_address = full_address or ""
        area, location = split_area_location(full_address)

        rows.append({
            "name": name,
            "full_address": full_address,
            "location": location,
            "area": area,
            "stars": "",
            "licence_ref": "",
            "bedrooms": "",
            "apartments": "",
            "url": durl
        })

        if i % 10 == 0 or i == len(detail_links):
            print(f"   …{i}/{len(detail_links)} details collected")
        time.sleep(SLEEP_BETWEEN_REQ)

    return rows


def build_prompt(row: dict) -> str:
    """
    Your exact brief, embedded so the model returns ONLY the HTML block,
    with zero invented amenities or claims.
    """
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
    """
    Calls OpenAI with your system + user prompts and returns ONLY the HTML block.
    """
    system = (
        "You are a Malta hotel content specialist and meticulous fact-checker. "
        "Never invent amenities or specifics; keep language vivid but safe."
    )
    user = build_prompt(row)

    try:
        resp = client.chat.completions.create(
            model=MODEL,
            temperature=0.7,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user}
            ]
        )
        html = resp.choices[0].message.content.strip()
        # Guarantee HTML only (strip accidental markdown fences if any)
        html = re.sub(r"^```html|```$", "", html, flags=re.IGNORECASE|re.MULTILINE).strip()
        return html
    except Exception as e:
        return f"<p>Error generating description: {str(e)}</p>"


def main():
    # 1) Scrape
    rows = scrape_all_hotels()
    if not rows:
        # still write empty csv with headers for Make/Hive
        cols = ["name","full_address","location","area","stars","licence_ref","bedrooms","apartments","description_html"]
        pd.DataFrame(columns=cols).to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
        print("⚠️ No rows scraped. Wrote empty CSV headers.")
        return

    # 2) Enrich with your marketing HTML (safely)
    print("🧠 Generating marketing descriptions (no invented amenities)…")
    enriched = []
    for i, r in enumerate(rows, 1):
        html = generate_description(r)
        enriched.append({
            "name": r["name"],
            "full_address": r["full_address"],
            "location": r["location"],
            "area": r["area"],
            "stars": "",          # unknown: do not invent
            "licence_ref": "",    # unknown: do not invent
            "bedrooms": "",       # unknown: do not invent
            "apartments": "",     # unknown: do not invent
            "description_html": html
        })
        if i % 5 == 0 or i == len(rows):
            print(f"   …{i}/{len(rows)} descriptions")

        # gentle pacing to avoid rate limits
        time.sleep(0.2)

    # 3) Save final CSV in the exact Make/Hive schema
    df = pd.DataFrame(enriched, columns=[
        "name","full_address","location","area","stars","licence_ref","bedrooms","apartments","description_html"
    ])
    df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    print(f"✅ Done. Wrote {len(df)} rows to {OUT_CSV}")


if __name__ == "__main__":
    main()
