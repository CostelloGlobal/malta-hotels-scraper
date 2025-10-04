import os, csv, asyncio
from pathlib import Path
from playwright.async_api import async_playwright

# ------------ CONFIG ------------
START_URL = "https://www.yellow.com.mt/hotels/malta/"
MAX_HOTELS = 50
OUTPUT_CSV = "hotels.csv"
CATEGORY = "Hotel"
# --------------------------------

async def scrape():
    hotels = []
    seen = set()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        await page.goto(START_URL, timeout=60000)
        await page.wait_for_timeout(2000)

        # Get hotel links
        links = await page.eval_on_selector_all(
            "a.business-name", "els => els.map(e => e.href)"
        )

        for link in links:
            if len(hotels) >= MAX_HOTELS:
                break
            if link in seen:
                continue
            seen.add(link)

            try:
                await page.goto(link, timeout=60000)
                await page.wait_for_timeout(2000)

                # Name
                name = await page.text_content("h1") or ""

                # Address
                address = await page.text_content(".address") or ""

                # Phone
                phone = await page.text_content(".phone") or ""

                # Email
                email = ""
                email_node = await page.query_selector("a[href^='mailto:']")
                if email_node:
                    email = (await email_node.get_attribute("href")).replace("mailto:", "")

                # Website
                website = ""
                web_node = await page.query_selector("a[href^='http']")
                if web_node:
                    website = await web_node.get_attribute("href")

                hotels.append({
                    "Name": name.strip(),
                    "Category": CATEGORY,
                    "Address": address.strip(),
                    "Phone": phone.strip(),
                    "Email": email.strip(),
                    "Website": website.strip() if website else ""
                })

                print(f"[ok] {len(hotels):>2}/{MAX_HOTELS}: {name}")

            except Exception as e:
                print(f"[skip] {link} -> {e}")

        await browser.close()

    # Save CSV
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["Name","Category","Address","Phone","Email","Website"])
        writer.writeheader()
        writer.writerows(hotels)

    print(f"\n✅ Done. Wrote {len(hotels)} hotels to {OUTPUT_CSV}")

if __name__ == "__main__":
    asyncio.run(scrape())
