import os, re, csv, asyncio, unicodedata
from pathlib import Path
from urllib.parse import urlparse
from playwright.async_api import async_playwright

# ------------ CONFIG ------------
START_URL_TEMPLATE = "https://www.yellow.com.mt/hotels/malta/?page={page}"
OUTPUT_CSV = "hotels.csv"
IMAGES_DIR = Path("images")
CATEGORY = "Hotel"
# --------------------------------

IMAGES_DIR.mkdir(exist_ok=True)

def slugify(text: str) -> str:
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode()
    text = re.sub(r"[^a-zA-Z0-9\-_. ]+", "", text).strip().lower()
    text = re.sub(r"\s+", "-", text)
    return text[:60] or "image"

async def scrape():
    hotels = []
    seen = set()
    page_num = 1

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        while True:
            list_url = START_URL_TEMPLATE.format(page=page_num)
            await page.goto(list_url, timeout=60000)
            await page.wait_for_timeout(2000)

            links = await page.eval_on_selector_all(
                "a[href*='_hotels+']",
                "els => els.map(e => e.href)"
            )
            if not links:
                print(f"⚠️ No more hotel links found on page {page_num}. Stopping.")
                break

            for link in links:
                if link in seen:
                    continue
                seen.add(link)

                try:
                    await page.goto(link, timeout=60000)
                    await page.wait_for_timeout(2000)

                    # Name
                    name = await page.text_content("h1") or "Unknown Hotel"

                    # Address
                    address = ""
                    addr_node = await page.query_selector(".contact-information, .business-contact, .address")
                    if addr_node:
                        address = (await addr_node.inner_text()).strip()

                    # Image
                    img_url = ""
                    imgs = await page.query_selector_all("div.business-photos img, div.gallery img, .swiper-slide img")
                    if imgs:
                        img_url = await imgs[0].get_attribute("src")

                    # Download image
                    image_filename = ""
                    if img_url and img_url.startswith("http"):
                        slug = slugify(name)
                        ext = os.path.splitext(urlparse(img_url).path)[1] or ".jpg"
                        image_filename = f"{slug}{ext.lower()[:5]}"
                        img_path = IMAGES_DIR / image_filename
                        try:
                            img_bytes = await (await context.request.get(img_url)).body()
                            with open(img_path, "wb") as f:
                                f.write(img_bytes)
                        except:
                            pass

                    hotels.append({
                        "Name": name.strip(),
                        "Category": CATEGORY,
                        "Address": address.strip(),
                        "Phone": "",     # intentionally blank
                        "Website": "",   # intentionally blank
                        "ImageFilename": f"images/{image_filename}" if image_filename else ""
                    })

                    print(f"[ok] {len(hotels)}: {name}")

                except Exception as e:
                    print(f"[skip] {link} -> {e}")

            page_num += 1

        await browser.close()

    # Save CSV
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["Name","Category","Address","Phone","Website","ImageFilename"])
        writer.writeheader()
        writer.writerows(hotels)

    print(f"\n✅ Done. Wrote {len(hotels)} hotels to {OUTPUT_CSV}")
    print(f"🖼️ Images saved in: {IMAGES_DIR.resolve()}")

if __name__ == "__main__":
    asyncio.run(scrape())
