import os
import time
import pandas as pd
from openai import OpenAI

# ========================================
# CONFIGURATION
# ========================================
INPUT_CSV = "hotels_scraped.csv"          # <-- change if your scraped file name differs
OUTPUT_CSV = "hotels_ai_ready.csv"
MODEL = "gpt-4o-mini"                     # fast + cost-efficient
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# ========================================
# PROMPT TEMPLATE
# ========================================
PROMPT_TEMPLATE = """
You are a Malta hotel content specialist creating compelling, SEO-optimised hotel profiles 
for VisitMalta.co.uk. You must match the client’s approved style and HTML structure exactly.

CRITICAL REQUIREMENTS:
- 100% factual accuracy (only use data provided, no invented amenities or features)
- Use emotional, sensory, story-driven marketing language
- Write in perfect UK English (not US)
- Close all HTML tags properly
- NO markdown formatting
- Tone: professional, descriptive, immersive

HTML TEMPLATE:
<h3>{name} | {stars}-Star Hotel in {location}</h3>

<p><strong>{opening_tagline}</strong></p>

<p>{story_paragraph}</p>

<p><strong>The Vibe:</strong> {vibe}<br>
<strong>Perfect For:</strong> {perfect_for}<br>
<strong>Key Location Benefits:</strong></p>
<ul>
<li>{benefit1}</li>
<li>{benefit2}</li>
<li>{benefit3}</li>
</ul>

<h4>Hotel Features & Atmosphere</h4>
<p>{features_paragraph}</p>

<h4>Amenities & Services</h4>
<p><strong>Hotel Facilities</strong></p>
<ul>
<li>{facility1}</li>
<li>{facility2}</li>
</ul>
<p><strong>Room Features</strong></p>
<ul>
<li>{room_feature1}</li>
<li>{room_feature2}</li>
</ul>

<h4>Location & Accessibility</h4>
<p><strong>📍 Address:</strong> {address}</p>
<p><strong>Within Walking Distance:</strong></p>
<ul>
<li>{nearby1}</li>
<li>{nearby2}</li>
</ul>
<p><strong>Transportation:</strong></p>
<ul>
<li>{transport1}</li>
<li>{transport2}</li>
</ul>

<h4>Guest Experiences</h4>
<p><strong>What Visitors Love</strong></p>
<ul>
<li>{love1}</li>
<li>{love2}</li>
</ul>
<p><strong>Local Insight</strong></p>
<ul>
<li>{tip1}</li>
<li>{tip2}</li>
</ul>

<p><strong>Ready to experience {name}?</strong><br>
[BOOK NOW - KM Malta Airlines Packages]</p>
"""

# ========================================
# ENRICH FUNCTION
# ========================================
def enrich_hotels():
    try:
        df = pd.read_csv(INPUT_CSV)
    except FileNotFoundError:
        raise SystemExit(f"❌ Input file not found: {INPUT_CSV}")

    print(f"✅ Loaded {len(df)} hotels from {INPUT_CSV}")

    enriched = []
    for idx, row in df.iterrows():
        name = row.get("name", "").strip()
        location = row.get("location", "").strip()
        address = row.get("address", "").strip()
        stars = str(row.get("stars", "")).strip()

        if not name:
            print(f"⚠️ Skipping row {idx}: missing name")
            continue

        prompt = PROMPT_TEMPLATE.format(
            name=name,
            stars=stars or "4",
            location=location or "Malta",
            opening_tagline="Where Malta Comes Alive",
            story_paragraph=f"Experience the spirit of {location or 'Malta'} at {name}, "
                            f"where heritage meets modern Mediterranean elegance.",
            vibe="Relaxed, authentic, and sun-soaked.",
            perfect_for="Travellers seeking a genuine Maltese escape.",
            benefit1="Golden limestone streets at your doorstep.",
            benefit2="Nearby blue bays perfect for swimming.",
            benefit3="Easy access to Valletta’s culture and charm.",
            features_paragraph="This hotel blends local charm with comfort, capturing Malta’s timeless atmosphere.",
            facility1="Rooftop pool with harbour views",
            facility2="Restaurant serving Maltese specialities",
            room_feature1="Private balcony",
            room_feature2="Complimentary Wi-Fi",
            nearby1="Valletta Waterfront (10-minute walk)",
            nearby2="Upper Barrakka Gardens (12-minute walk)",
            transport1="Regular bus stop outside the hotel",
            transport2="Airport transfer service available",
            love1="Guests adore the warm Maltese hospitality.",
            love2="Praised for panoramic terrace views.",
            tip1="Order a Cisk beer at sunset overlooking the Grand Harbour.",
            tip2="Visit early morning for quiet strolls through historic alleys.",
            address=address or "Malta"
        )

        try:
            response = client.chat.completions.create(
                model=MODEL,
                messages=[
                    {"role": "system", "content": "You are a professional Malta tourism copywriter."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.8,
                max_tokens=1200
            )
            description_html = response.choices[0].message.content.strip()
            enriched.append({
                "name": name,
                "full_address": address,
                "location": location,
                "stars": stars,
                "description_html": description_html
            })
            print(f"✨ Enriched: {name}")
        except Exception as e:
            print(f"⚠️ Error enriching {name}: {e}")
            time.sleep(2)
            continue

        time.sleep(1.5)

    pd.DataFrame(enriched).to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    print(f"🎯 Done! Enriched {len(enriched)} hotels → {OUTPUT_CSV}")

# ========================================
# RUN
# ========================================
if __name__ == "__main__":
    enrich_hotels()
