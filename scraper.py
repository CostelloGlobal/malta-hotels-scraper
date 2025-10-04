import csv, asyncio
from playwright.async_api import async_playwright

START_URL_TEMPLATE = "https://www.yellow.com.mt/hotels/malta/?page={page}"
OUTPUT_CSV = "raw_hotels.csv"

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
            print(f"Scraping {list_url}")
            await page.goto(list_url, timeout=60000)
            await page.wait_for_timeout(2000)

            # collect hotel links
            links = await page.eval_on_selector_all(
                "a[href*='_hotels+']",
                "els => els.map(e => e.href)"
            )
            if not links:
                break

            for link in links:
                if link in seen:
                    continue
                seen.add(link)

                try:
                    await page.goto(link, timeout=60000)
                    await page.wait_for_timeout(1500)

                    name = await page.text_content("h1") or ""
                    address = await page.text_content(".contact-information, .business-contact, .address") or ""

                    hotels.append({
                        "Name": name.strip(),
                        "Address": address.strip(),
                        "SourceURL": link
                    })

                    print(f"✅ {name}")

                except Exception as e:
                    print(f"[skip] {link}: {e}")

            page_num += 1

        await browser.close()

    # Save to CSV
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["Name", "Address", "SourceURL"])
        writer.writeheader()
        writer.writerows(hotels)

    print(f"\n✅ Done. Wrote {len(hotels)} hotels to {OUTPUT_CSV}")

if __name__ == "__main__":
    asyncio.run(scrape())
