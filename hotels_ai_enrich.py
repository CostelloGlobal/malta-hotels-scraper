import os
import pandas as pd
from openai import OpenAI

# -----------------------
# CONFIGURATION
# -----------------------
INPUT_CSV = "hotels_all_output.csv"   # your master scrape file
OUTPUT_CSV = "hotels_ai_ready.csv"    # enriched AI content file
MODEL = "gpt-4o-mini"                 # efficient, creative model
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# -----------------------
# PROMPT TEMPLATE
# -----------------------
PROMPT_TEMPLATE = """
You are a Malta hotel content specialist creating compelling, SEO-optimised hotel profiles for VisitMalta.co.uk.

CRITICAL REQUIREMENTS:
- 100% factual accuracy from given data (no invented amenities or names)
- Must follow the exact HTML structure below
- Use sensory, emotional language and vivid detail
- Tone: professional, descriptive, immersive
- Use only English (UK spelling)
- Ensure all HTML tags close properly
- Do NOT include Markdown

Write the description in this HTML structure using the provided hotel data.

HTML TEMPLATE:
<h3>{name} | {stars}-Star Hotel in {location}</h3>

<p><strong>[Compelling Opening Tagline]</strong></p>

<p>[Emotional, sensory opening paragraph telling the STORY of staying there. Use specific details, metaphors, and make readers visualise themselves at the hotel.]</p>

<p><strong>The Vibe:</strong> [Atmosphere description]<br>
<strong>Perfect For:</strong> [Target audience]<br>
<strong>Key Location Benefits:</strong></p>
<ul>
<li>[Specific, sensory location benefit]</li>
<li>[Specific, sensory location benefit]</li>
<li>[Specific, sensory location benefit]</li>
</ul>

<h4>Hotel Features & Atmosphere</h4>
<p>[Description that creates emotional connection before mentioning amenities]</p>

<h4>Amenities & Services</h4>
<p><strong>Hotel Facilities</strong></p>
<ul>
<li>[Facility 1]</li>
<li>[Facility 2]</li>
</ul>
<p><strong>Room Features</strong></p>
<ul>
<li>[Room feature 1]</li>
<li>[Room feature 2]</li>
</ul>

<h4>Location & Accessibility</h4>
<p><strong>📍 Address:</strong> {full_address}</p>

<h4>Guest Experiences</h4>
<p><strong>What Visitors Love</strong></p>
<ul>
<li>[Positive, realistic emotional point]</li>
<li>[Positive, realistic emotional point]</li>
</ul>

<p><strong>Ready to experience {location}?</strong><br>
[BOOK NOW - KM Malta Airlines Packages]</p>

DATA PROVIDED:
Name: {name}
Address: {full_address}
Location: {location}
Area: {area}
Stars: {stars}
Licence: {licence_ref}
Bedrooms: {bedrooms}
Apartments: {apartments}
"""

# -----------------------
# FUNCTION
# -----------------------

def enrich_hotels():
    if not os.path.exists(INPUT_CSV):
        print(f"⚠️ No '{INPUT_CSV}' found. Skipping enrichment.")
        return

    df = pd.read_csv(INPUT_CSV)
    enriched_rows = []

    for idx, row in df.iterrows():
        try:
            print(f"✨ Enriching: {row.get('name', 'Unknown Hotel')} ({idx+1}/{len(df)})")

            prompt = PROMPT_TEMPLATE.format(
                name=row.get("name", ""),
                full_address=row.get("full_address", ""),
                location=row.get("location", ""),
                area=row.get("area", ""),
                stars=row.get("stars", ""),
                licence_ref=row.get("licence_ref", ""),
                bedrooms=row.get("bedrooms", ""),
                apartments=row.get("apartments", "")
            )

            completion = client.chat.completions.create(
                model=MODEL,
                messages=[
                    {"role": "system", "content": "You are a professional Malta hotel copywriter and SEO content expert."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.8,
                max_tokens=1200
            )

            html_desc = completion.choices[0].message.content.strip()
            row["description_html"] = html_desc
            enriched_rows.append(row)

        except Exception as e:
            print(f"❌ Error enriching {row.get('name', '')}: {e}")
            continue

    if not enriched_rows:
        print("⚠️ No hotels were enriched — check input file.")
        return

    enriched_df = pd.DataFrame(enriched_rows)
    enriched_df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    print(f"✅ Enrichment complete! Output saved as: {OUTPUT_CSV}")


if __name__ == "__main__":
    enrich_hotels()
