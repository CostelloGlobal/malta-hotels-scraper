import os
import pandas as pd
import openai

# ==============================
# CONFIGURATION
# ==============================
INPUT_CSV = "hotels_all_output.csv"   # master scrape file
OUTPUT_CSV = "hotels_ai_ready.csv"    # enriched AI content output
MODEL = "gpt-4o-mini"                 # efficient creative model

openai.api_key = os.getenv("OPENAI_API_KEY")

# ==============================
# PROMPT TEMPLATE
# ==============================
PROMPT_TEMPLATE = """
You are a Malta hotel content specialist creating compelling, SEO-optimised hotel descriptions for VisitMalta.co.uk. 
Use the data provided for each hotel to craft a rich, emotional, sensory narrative that matches the client’s approved marketing style.

CRITICAL REQUIREMENTS:
- 100% factual accuracy (no invented amenities or locations)
- Follow the exact HTML structure below
- Use sensory, emotional, narrative-driven language
- Include area and atmosphere details specific to Malta
- Use only English (UK)
- Ensure all HTML tags close properly
- Do NOT include Markdown, only pure HTML
- Be concise, engaging, and human in tone

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
<li>[Feature 1]</li>
<li>[Feature 2]</li>
</ul>

<h4>Location & Accessibility</h4>
<p><strong>📍 Address:</strong> {full_address}</p>
<p><strong>Within Walking Distance:</strong></p>
<ul>
<li>[Nearby attraction 1]</li>
<li>[Nearby attraction 2]</li>
</ul>
<p><strong>Transportation:</strong></p>
<ul>
<li>[Transport option 1]</li>
<li>[Transport option 2]</li>
</ul>

<h4>Guest Experiences</h4>
<p><strong>What Visitors Love</strong></p>
<ul>
<li>[Realistic positive point with emotional tone]</li>
<li>[Realistic positive point with emotional tone]</li>
</ul>
<p><strong>Local Insight</strong></p>
<ul>
<li>[Useful local tip with sensory detail]</li>
<li>[Useful local tip with sensory detail]</li>
</ul>

<p><strong>Ready to [experience]?</strong><br>
[BOOK NOW - KM Malta Airlines Packages]</p>
"""

# ==============================
# ENRICHMENT FUNCTION
# ==============================
def enrich_hotels():
    df = pd.read_csv(INPUT_CSV)
    descriptions = []

    for i, row in df.iterrows():
        print(f"✨ Enriching: {row.get('name', 'Unknown Hotel')}")

        # Combine row data into a single prompt input
        row_text = (
            f"Hotel Name: {row.get('name', '')}\n"
            f"Address: {row.get('full_address', '')}\n"
            f"Location: {row.get('location', '')}\n"
            f"Area: {row.get('area', '')}\n"
            f"Stars: {row.get('stars', '')}\n"
            f"Licence: {row.get('licence_ref', '')}\n"
            f"Bedrooms: {row.get('bedrooms', '')}\n"
            f"Apartments: {row.get('apartments', '')}\n"
        )

        try:
            response = openai.ChatCompletion.create(
                model=MODEL,
                messages=[
                    {"role": "system", "content": PROMPT_TEMPLATE},
                    {"role": "user", "content": row_text},
                ],
                temperature=0.8,
                max_tokens=1000,
            )

            ai_description = response["choices"][0]["message"]["content"].strip()
            descriptions.append(ai_description)

        except Exception as e:
            print(f"⚠️ Error enriching {row.get('name', '')}: {e}")
            descriptions.append("")

    df["description_html"] = descriptions
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    print(f"✅ Done — enriched descriptions saved to {OUTPUT_CSV}")

# ==============================
# MAIN EXECUTION
# ==============================
if __name__ == "__main__":
    enrich_hotels()
