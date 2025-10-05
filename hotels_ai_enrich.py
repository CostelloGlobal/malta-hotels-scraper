import os
import pandas as pd
from openai import OpenAI

# ================================
# CONFIGURATION
# ================================
INPUT_CSV = "hotels_all_output.csv"      # your scraped source
OUTPUT_CSV = "hotels_ai_ready.csv"       # enriched AI output
MODEL = "gpt-4o-mini"
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# ================================
# PROMPT TEMPLATE
# ================================
PROMPT_TEMPLATE = """
You are a Malta hotel content specialist creating compelling, SEO-optimised hotel descriptions for VisitMalta.co.uk.

CRITICAL REQUIREMENTS:
- 100% factual accuracy from provided CSV data
- Follow the exact HTML structure
- Use sensory, emotional language and vivid detail
- Professional tone, UK English spelling
- All HTML tags must close properly
- NO markdown

HTML TEMPLATE:
<h3>{name} | {stars}-Star Hotel in {location}</h3>

<p><strong>{tagline}</strong></p>

<p>{story}</p>

<h4>The Vibe</h4>
<p>{vibe}</p>

<h4>Amenities & Services</h4>
<ul>
<li>{amenity1}</li>
<li>{amenity2}</li>
</ul>

<h4>Location</h4>
<p><strong>📍 Address:</strong> {address}</p>
"""

# ================================
# MAIN ENRICHMENT FUNCTION
# ================================
def enrich_hotels():
    df = pd.read_csv(INPUT_CSV)
    enriched_rows = []

    for _, row in df.iterrows():
        name = row.get("name", "")
        location = row.get("location", "")
        stars = row.get("stars", "")
        address = row.get("full_address", "")

        prompt = f"""
Hotel Name: {name}
Location: {location}
Stars: {stars}
Address: {address}

{PROMPT_TEMPLATE}
        """

        try:
            response = client.chat.completions.create(
                model=MODEL,
                messages=[
                    {"role": "system", "content": "You are a professional Malta hotel copywriter."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.8,
                max_tokens=1500
            )
            html_description = response.choices[0].message.content.strip()
        except Exception as e:
            html_description = f"Error generating content: {e}"

        enriched_rows.append({
            "name": name,
            "full_address": address,
            "location": location,
            "stars": stars,
            "description_html": html_description
        })

    pd.DataFrame(enriched_rows).to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    print(f"✅ AI enrichment complete: {len(enriched_rows)} hotels saved to {OUTPUT_CSV}")

if __name__ == "__main__":
    enrich_hotels()
