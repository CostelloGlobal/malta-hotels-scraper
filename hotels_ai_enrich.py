import os, re, time, json, csv
import pandas as pd
import requests
from bs4 import BeautifulSoup

# -----------------------------
# Config
# -----------------------------
BASE = "https://www.yellow.com.mt"
LIST_URL = f"{BASE}/hotels/?page="
HEADERS = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36"}
SLEEP_BETWEEN = 0.7

IN_FILES = ["hotels_scraped.csv", "hotels_enriched.csv", "hotels_all_output.csv"]
OUT_CSV = "hotels_ai_ready.csv"

# -----------------------------
# Utilities
# -----------------------------
def slugify(text):
    text = text.lower()
    text = re.sub(r"[’'`]", "", text)
    text = re.sub(r"[^a-z0-9]+", "-", text).strip("-")
    text = re.sub(r"-{2,}", "-", text)
    return text

def detect_locality(address):
    if not address:
        return ""
    # very light heuristic: last comma-part often is locality
    parts = [p.strip() for p in address.split(",") if p.strip()]
    return parts[-1] if parts else ""

def read_input_csv():
    for f in IN_FILES:
        if os.path.exists(f):
            try:
                df = pd.read_csv(f)
                if len(df):
                    print(f"✅ Using input file: {f} ({len(df)} rows)")
                    return df
            except Exception as e:
                print(f"⚠️ Could not read {f}: {e}")
    print("ℹ️ No existing input CSV found in repo; running a quick scrape to build hotels_scraped.csv …")
    df = quick_scrape()
    return df

def quick_scrape():
    """Lightweight scraper to ensure we have rows to enrich (name, address, phone, website, link)."""
    all_rows = []
    page = 1
    while True:
        url = f"{LIST_URL}{page}"
        print(f"• Scraping list page {page} -> {url}")
        resp = requests.get(url, headers=HEADERS)
        if resp.status_code != 200:
            print(f"  ⚠️ Page {page} returned {resp.status_code}. Stopping.")
            break
        soup = BeautifulSoup(resp.text, "html.parser")
        cards = soup.select("a.BusinessCardV2style__CardHeaderLink-sc-__sc-1k6t9dc-7")
        if not cards:
            print(f"  ⚠️ No hotel links on page {page}. Stopping.")
            break

        for a in cards:
            link = a.get("href") or ""
            name = (a.text or "").strip()
            if not link:
                continue
            if not link.startswith("http"):
                link = BASE + link

            # visit detail
            try:
                d = requests.get(link, headers=HEADERS, timeout=20)
                ds = BeautifulSoup(d.text, "html.parser")

                # name (fallback to H1 on detail)
                h1 = ds.select_one("h1")
                if h1 and h1.text.strip():
                    name = h1.text.strip()

                addr = ""
                addr_tag = ds.select_one("div.BusinessAddressstyle__BusinessAddress-sc-__sc-10jde9o-0")
                if addr_tag:
                    addr = addr_tag.get_text(" ", strip=True)

                phone = ""
                tel_a = ds.select_one("a[href^='tel:']")
                if tel_a:
                    phone = tel_a.get_text(strip=True)

                website = ""
                web_a = ds.select_one("a[href^='http']")
                if web_a:
                    website = (web_a.get("href") or "").strip()

                all_rows.append({
                    "name": name,
                    "full_address": addr,
                    "phone": phone,
                    "website": website,
                    "link": link
                })
                print(f"  ✅ {name}")
                time.sleep(0.3)
            except Exception as e:
                print(f"  ❌ Error scraping detail: {e}")
                continue

        page += 1
        time.sleep(SLEEP_BETWEEN)

    df = pd.DataFrame(all_rows)
    df.to_csv("hotels_scraped.csv", index=False, encoding="utf-8-sig")
    print(f"✅ Wrote {len(df)} rows to hotels_scraped.csv")
    return df

# -----------------------------
# OpenAI client (legacy SDK 0.28.x)
# -----------------------------
USE_AI = False
try:
    import openai
    openai.api_key = os.getenv("OPENAI_API_KEY", "")
    USE_AI = bool(openai.api_key)
except Exception:
    USE_AI = False

MODEL = "gpt-4o-mini"

SYSTEM_PROMPT = """You are a Malta hotel content specialist creating compelling, SEO-optimized hotel profiles that match the client's exceptional marketing style.

DATA SOURCE: Only use details provided (name, address/locality). Do NOT invent amenities, star ratings, or features. If a fact isn’t provided, avoid it.

STYLE (must match):
- Emotional, narrative-driven openings ("Where Malta Comes Alive")
- Sensory, specific Malta details (luzzu boats, golden stone walls at sunset) without inventing hotel facilities
- Strong metaphors
- Location storytelling that makes readers FEEL the experience
- Balance local energy with hotel calm

OUTPUT: PURE HTML ONLY. EXACT structure below. Close all tags. No Markdown.

HTML STRUCTURE:
<h3>Hotel Name | Star Rating in Location</h3>

<p><strong>[Compelling Opening Tagline]</strong></p>

<p>[Emotional, sensory opening paragraph telling the STORY of staying there. Focus on place/ambience and proximity tone without promising facilities you don't know.]</p>

<p><strong>The Vibe:</strong> [Atmosphere description]<br>
<strong>Perfect For:</strong> [Target audience]<br>
<strong>Key Location Benefits:</strong></p>
<ul>
<li>[Specific-but-safe local colour drawn from Malta generally; avoid naming attractions you cannot confirm]</li>
<li>[Another evocative local detail or neighbourhood mood]</li>
<li>[A third evocative yet non-claim detail]</li>
</ul>

<h4>Hotel Features & Atmosphere</h4>
<p>[Create emotional connection without listing unverified amenities.]</p>

<h4>Amenities & Services</h4>
<p><strong>Hotel Facilities</strong></p>
<ul>
<li>Facilities operated per MTA licence; see official sources for current details</li>
<li>Contact the hotel directly for verified on-site services</li>
</ul>
<p><strong>Room Features</strong></p>
<ul>
<li>Room types and layouts vary by property</li>
<li>Confirm specifics before booking</li>
</ul>

<h4>Location & Accessibility</h4>
<p><strong>📍 Address:</strong> [Full Address]</p>
<p><strong>Within Walking Distance:</strong></p>
<ul>
<li>Local cafés and seafront ambience (typical for Malta’s coastal towns)</li>
<li>Bus connections within the locality (verify routes before travel)</li>
</ul>
<p><strong>Transportation:</strong></p>
<ul>
<li>Malta Public Transport services operate in the area</li>
<li>Taxis and ride-hailing commonly available</li>
</ul>

<h4>Guest Experiences</h4>
<p><strong>What Visitors Love</strong></p>
<ul>
<li>Immersive Maltese atmosphere and warm hospitality</li>
<li>Convenient base for exploring nearby coastline and heritage</li>
</ul>
<p><strong>Local Insight</strong></p>
<ul>
<li>Golden limestone streets glow at sunset—perfect for gentle evening walks</li>
<li>Watch colourful luzzu boats bobbing in neighbouring bays</li>
</ul>

<p><strong>Ready to experience Malta?</strong><br>
BOOK NOW - KM Malta Airlines Packages</p>
"""

def build_html_with_ai(name, address, locality):
    if not USE_AI:
        # Safe placeholder if no API key
        header_loc = f"{name} | in {locality}" if locality else name
        return f"""<h3>{header_loc}</h3>
<p><strong>A doorway to Malta’s sunshine and sea breezes.</strong></p>
<p>Stay in the heart of Malta’s island rhythm. From golden limestone streets to gentle harbour views, this setting makes every step feel postcard-worthy.</p>
<p><strong>The Vibe:</strong> Relaxed island energy<br><strong>Perfect For:</strong> Couples & culture-seekers<br><strong>Key Location Benefits:</strong></p>
<ul><li>Atmospheric Maltese streets</li><li>Sea-breezy promenades</li><li>Simple access to buses & taxis</li></ul>
<h4>Hotel Features & Atmosphere</h4>
<p>Inviting spaces designed for unrushed days and golden-hour evenings.</p>
<h4>Amenities & Services</h4>
<p><strong>Hotel Facilities</strong></p>
<ul><li>Facilities operated per MTA licence; see official sources</li><li>Contact the hotel for current services</li></ul>
<p><strong>Room Features</strong></p>
<ul><li>Room types vary by property</li><li>Confirm specifics when booking</li></ul>
<h4>Location & Accessibility</h4>
<p><strong>📍 Address:</strong> {address}</p>
<p><strong>Within Walking Distance:</strong></p>
<ul><li>Neighbourhood cafés and bays</li><li>Local bus stops (verify routes)</li></ul>
<p><strong>Transportation:</strong></p>
<ul><li>Malta Public Transport</li><li>Taxis / ride-hailing</li></ul>
<h4>Guest Experiences</h4>
<p><strong>What Visitors Love</strong></p>
<ul><li>Warm Maltese welcome</li><li>Easy base for coast & heritage</li></ul>
<p><strong>Local Insight</strong></p>
<ul><li>Sunset on limestone streets is magical</li><li>Spot colourful luzzu boats in nearby bays</li></ul>
<p><strong>Ready to experience Malta?</strong><br>BOOK NOW - KM Malta Airlines Packages</p>"""

    # With AI
    user = {
        "role": "user",
        "content": (
            f"Hotel data:\n"
            f"- Name: {name}\n"
            f"- Full Address: {address}\n"
            f"- Locality/Area: {locality}\n\n"
            "IMPORTANT: Do not invent amenities, star ratings, or attractions. "
            "Use PURE HTML ONLY and the exact structure from the system prompt. "
            "If a field (e.g., stars) is unknown, omit it in the header."
        )
    }
    try:
        resp = openai.ChatCompletion.create(
            model=MODEL,
            temperature=0.7,
            messages=[{"role":"system","content":SYSTEM_PROMPT}, user]
        )
        html = resp["choices"][0]["message"]["content"].strip()
        return html
    except Exception as e:
        print(f"⚠️ AI error: {e}. Falling back.")
        return build_html_with_ai(name, address, locality="")  # fallback

def make_seo_bits(name, locality):
    title = f"{name} – {locality}, Malta | Stay & Book"
    if len(title) > 60:
        title = title[:57] + "..."
    desc = f"Discover {name} in {locality}, Malta—an inviting base for exploring coastline, culture and Mediterranean evenings."
    if len(desc) > 160:
        desc = desc[:157] + "..."
    return title, desc

# -----------------------------
# Main
# -----------------------------
def main():
    df = read_input_csv()
    if df is None or df.empty:
        print("No hotels to enrich.")
        pd.DataFrame([], columns=[
            "name","full_address","phone","website","link",
            "slug","seo_title","seo_description","description_html"
        ]).to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
        print(f"✅ Wrote 0 rows to {OUT_CSV}")
        return

    # Normalise expected columns
    for col in ["name","full_address","address","phone","website","link"]:
        if col not in df.columns:
            df[col] = ""

    rows = []
    for _, r in df.iterrows():
        name = str(r.get("name","")).strip() or str(r.get("Name","")).strip()
        address = str(r.get("full_address","")).strip() or str(r.get("address","")).strip()
        website = str(r.get("website","")).strip()
        link = str(r.get("link","")).strip()

        loc = detect_locality(address)
        slug = slugify(f"{name} {loc} malta hotel")

        html = build_html_with_ai(name, address, loc)
        seo_title, seo_description = make_seo_bits(name, loc)

        rows.append({
            "name": name,
            "full_address": address,
            "location": loc,
            "area": "",
            "stars": "",
            "licence_ref": "",
            "bedrooms": "",
            "apartments": "",
            "phone": r.get("phone",""),
            "website": website,
            "source_link": link,
            "slug": slug,
            "seo_title": seo_title,
            "seo_description": seo_description,
            "description_html": html
        })
        print(f"📝 Enriched: {name}")
        time.sleep(0.2)

    out = pd.DataFrame(rows)
    out.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    print(f"✅ Wrote {len(out)} rows to {OUT_CSV}")

if __name__ == "__main__":
    main()
