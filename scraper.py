import os, re, csv, asyncio, unicodedata
from pathlib import Path
from urllib.parse import urlparse
from playwright.async_api import async_playwright

# ------------ CONFIG ------------
START_URL_TEMPLATE = "https://www.yellow.com.mt/hotels/malta/?page={page}"
OUTPUT_CSV = "hotels.csv"
IMAGES_DIR = Path("images")
# --------------------------------

IMAGES_DIR.mkdir(exist_ok=True)

def slugify(text: str) -> str:
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode()
    text = re.sub(r"[^a-zA-Z0-9\\-_. ]+", "", text).strip().lower()
    text = re.sub(r"\\s+", "-", text)
    return text[:60] or "image"

async def scrape():
    rows = []
    seen = set()
    page_num = 1

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        while True:
            list_url = START_URL_TEMPLATE.format(page=page_num)
            print(f"Scraping list: {list_url}")
            await page.goto(list_url, timeout=60000)
            await page.wait_for_timeout(1500)

            links = await page.eval_on_selector_all(
                "a[href*='_hotels+']",
                "els => els.map(e => e.href)"
            )
            if not links:
                print(f"No more results on page {page_num}.")
                break

            for link in links:
                if link in seen:
                    continue
                seen.add(link)
                try:
                    await page.goto(link, timeout=60000)
                    await page.wait_for_timeout(1200)

                    name = (await page.text_content("h1") or "").strip() or "Unknown Hotel"

                    address = ""
                    addr_node = await page.query_selector(".contact-information, .business-contact, .address")
                    if addr_node:
                        address = (await addr_node.inner_text()).strip()

                    # Rough split for location/area from the tail of the address (best-effort)
                    location = area = ""
                    if "," in address:
                        parts = [x.strip() for x in address.split(",") if x.strip()]
                        if len(parts) >= 1:
                            location = parts[-1]
                        if len(parts) >= 2:
                            area = parts[-2]

                    # Try to grab a representative image (if present)
                    image_filename = ""
                    imgs = await page.query_selector_all("div.business-photos img, div.gallery img, .swiper-slide img, meta[property='og:image']")
                    img_url = ""
                    if imgs:
                        if await imgs[0].get_attribute("content"):
                            img_url = await imgs[0].get_attribute("content")
                        else:
                            img_url = await imgs[0].get_attribute("src")
                    if img_url and img_url.startswith("http"):
                        try:
                            ext = os.path.splitext(urlparse(img_url).path)[1] or ".jpg"
                            fn = f"{slugify(name)}{ext.lower()[:5]}"
                            img_path = IMAGES_DIR / fn
                            resp = await context.request.get(img_url)
                            if resp.ok:
                                with open(img_path, "wb") as f:
                                    f.write(await resp.body())
                                image_filename = f"images/{fn}"
                        except Exception:
                            pass

                    # Fields we don't reliably have from Yellow (left blank to be filled later from MTA)
                    stars = ""
                    licence_ref = ""
                    bedrooms = ""
                    apartments = ""

                    # Temporary placeholder; enrichment step will overwrite
                    placeholder_html = (
                        f"<h3>{name} | Hotel in {location}</h3>"
                        f"<p><strong>{name}</strong> — full description to follow.</p>"
                    )

                    rows.append({
                        "name": name,
                        "full_address": address,
                        "location": location,
                        "area": area,
                        "stars": stars,
                        "licence_ref": licence_ref,
                        "bedrooms": bedrooms,
                        "apartments": apartments,
                        "description_html": placeholder_html
                    })
                    print(f"[ok] {len(rows)}: {name}")

                except Exception as e:
                    print(f"[skip] {link} -> {e}")

            page_num += 1

        await browser.close()

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["name","full_address","location","area","stars","licence_ref","bedrooms","apartments","description_html"]
        )
        writer.writeheader()
        writer.writerows(rows)

    print(f"✅ Scrape complete. {len(rows)} hotels written to {OUTPUT_CSV}")

if __name__ == "__main__":
    asyncio.run(scrape())
