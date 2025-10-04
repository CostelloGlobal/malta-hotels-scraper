page = 1
while True:
    url = LIST_URL.format(page)
    print(f"🟡 Page {page}: {url}")
    res = requests.get(url, headers=HEADERS, timeout=30)
    soup = BeautifulSoup(res.text, "html.parser")

    # Updated selector that matches the current Yellow.mt structure
    cards = soup.select("a.business-name, div.business-card a, h2 a, .business-listing a")

    if not cards:
        print("✅ No more listings on this page. Done.")
        break

    for a in cards:
        href = a.get("href", "")
        if not href:
            continue
        if href.startswith("/"):
            href = "https://www.yellow.com.mt" + href
        if href.endswith("/"):
            detail_links.append(href)

    page += 1
    time.sleep(SLEEP_BETWEEN_REQ)
