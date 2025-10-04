import os, time, json, re
import requests
import pandas as pd
from bs4 import BeautifulSoup
from openai import OpenAI

# ---------- CONFIG ----------
LIST_URL = "https://www.yellow.com.mt/hotels/?page={}"
HEADERS = {"User-Agent": "Mozilla/5.0"}
OUT_CSV = "hotels_enriched.csv"
SLEEP_BETWEEN_REQ = 1.0  # polite delay between requests
# ----------------------------

# ---------- OpenAI Setup ----------
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
client = OpenAI(api_key=OPENAI_API_KEY)
MODEL = "gpt-4o-mini"
# ----------------------------------

def get_detail(url: str):
    """Extract hotel name + full address from a single detail page."""
    try:
        r = requests.get(url, headers=HEADERS, timeout=30)
        soup = BeautifulSoup(r.text, "html.parser")

        # Name
        name = soup.find("h1")
        name = name.get_text(strip=True) if name else ""

        # Address from JSON-LD
        full_address = ""
        for tag in soup.find_all("script", attrs={"type": "application/ld+json"}):
            try:
                data = json.loads(tag.string or "{}")
                data = data if isinstance(data, list) else [data]
                for obj in data:
                    if isinstance(obj, dict) and "address" in obj:
                        addr = obj["address"]
                        if isinstance(addr, dict):
                            bits = [
                                addr.get("streetAddress", ""),
                                addr.get("addressLocality", ""),
                                addr.get("postalCode", ""),
                                addr.get("addressCountry", ""),
                            ]
                            full_address = ", ".join([b for b in bits if b])
                            break
                if full_address:
                    break
            except Exception:
                continue

        # Fallback to visible address
        if not full_address:
            contact = soup.select_one(".contact-information, .address, .business-contact")
            if contact:
                full_address = " ".join(contact.get_text(" ", strip=True).split())

        full_address = re.sub(r"\s+", " ", full_address).strip()
        return name, full_address

    except Exception:
        return "", ""

def split_area_location(full_address: str):
    """Try to split address into area and location."""
    if not full_address or "," not in full_address:
        return "", ""
    parts = [p.strip() for p in full_address.split(",") if p.strip()]
    location = parts[-1] if parts else ""
    area = parts[-2] if len(parts) > 1 else ""
    return area, location

def scrape_all_hotels():
    """Scrape all hotels from Yellow.com.mt with no fixed page limit."""
    rows = []
    detail_links = []
    page = 1

    print("🔍 Starting unlimited hotel scrape from Yellow.com.mt ...")

    while True:
        url = LIST_URL.format(page)
        print(f" • Scanning Page {page}: {url}")
        resp = requests.get(url, headers=HEADERS, timeout=30)
        if resp.status_code != 200:
            print(f"   ⚠️ Page {page} returned {resp.status_code}, stopping.")
            break

        soup = BeautifulSoup(resp.text, "html.parser")
        cards = soup.select("a.business-name, div.business-card a, h2 a, .business-listing a")

        if not cards:
            print("   ✅ No more listings on this page. Stopping.")
            break

        for a in cards:
            href = a.get("href")
            if href and href.startswith("/"):
                href = "https://www.yellow.com.mt" + href
            if href:
                detail_links.append(href)

        print(f"   ➕ Found {len(cards)} listings on page {page}")
        page += 1
        time.sleep(SLEEP_BETWEEN_REQ)

    detail_links = sorted(set(detail_links))
    print(f"🔗 Total {len(detail_links)} unique hotel URLs found.")

    # Visit each hotel page
    for i, durl in enumerate(detail_links, 1):
        name, full_address = get_detail(durl)
        area, location = split_area_location(full_address)
        rows.append({
            "name": name,
            "full_address": full_address,
            "area": area,
            "location": location,
            "url": durl
        })
        if i % 10 == 0 or i == len(detail_links):
            print(f"   …{i}/{len(detail_links)} hotels processed")
        time.sleep(SLEEP_BETWEEN_REQ)

    print(f"✅ Scraping complete: {len(rows)} hotels collected.")
    return rows

def build_prompt(row):
    return f"""
You are a Malta hotel content writer for VisitMalta.co.uk. Write an SEO-optimised, fact-safe HTML hotel description using only:
- name: {row.get('name','')}
- full_address: {row.get('full_address','')}
- area: {row.get('area','')}
- location: {row.get('location','')}
Do not invent amenities or details.
""".strip()

def generate_description(row):
    try:
        resp = client.chat.completions.create(
            model=MODEL,
            temperature=0.7,
            messages=[
                {"role": "system", "content": "Write HTML hotel listings, never invent data."},
                {"role": "user", "content": build_prompt(row)}
            ]
        )
        html = resp.choices[0].message.content.strip()
        return re.sub(r"^```html|```$", "", html, flags=re.MULTILINE).strip()
    except Exception as e:
        return f"<p>Error generating description: {e}</p>"

def main():
    rows = scrape_all_hotels()
    if not rows:
        pd.DataFrame(columns=["name","full_address","area","location","url","description_html"]) \
          .to_csv(OUT_CSV, index=False)
        print("⚠️ No hotels found — wrote empty CSV.")
        return

    print("🧠 Enriching data with OpenAI descriptions ...")
    enriched = []
    for i, r in enumerate(rows, 1):
        html = generate_description(r)
        enriched.append({**r, "description_html": html})
        if i % 5 == 0 or i == len(rows):
            print(f"   …{i}/{len(rows)} descriptions complete")
        time.sleep(0.3)

    pd.DataFrame(enriched).to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    print(f"✅ Done. Wrote {len(enriched)} enriched hotel entries to {OUT_CSV}")

if __name__ == "__main__":
    main()
