import requests
from openai import OpenAI
import json
import config
from utils import save_opportunity

def scrape_costco_deals():
    """Scrape Costco deals page and extract products"""
    
    print("🔍 Scraping Costco deals page...")
    
    # Fetch the page
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
    }
    
    response = requests.get(config.COSTCO_DEALS_URL, headers=headers)
    html_content = response.text
    
    print(f"✅ Downloaded page ({len(html_content)} chars)")
    
    # Use OpenAI to extract products
    products = extract_products_with_ai(html_content)
    
    return products

def extract_products_with_ai(html_content):
    """Use OpenAI to extract product data from HTML"""
    
    print("🤖 Extracting products with AI...")
    
    client = OpenAI(api_key=config.OPENAI_API_KEY)
    
    # Truncate HTML if too long (GPT-4o-mini has limits)
    if len(html_content) > 100000:
        html_content = html_content[:100000]
    
    prompt = f"""
Extract products from this Costco deals page HTML.

For the first {config.MAX_PRODUCTS_TO_SCRAPE} products you find, extract:
- name: Product name
- costco_sku: Item number (usually 6-7 digits)
- costco_price: Price as a number (no $ sign)
- costco_url: Full product URL
- category: Product category (Electronics, Kitchen, etc)
- in_stock: true/false

Return ONLY valid JSON array. No markdown, no explanation.

Example:
[
  {{
    "name": "Dyson V11 Cordless Vacuum",
    "costco_sku": "1234567",
    "costco_price": 299.99,
    "costco_url": "https://www.costco.com/product.html",
    "category": "Appliances",
    "in_stock": true
  }}
]

HTML:
{html_content}
"""
    
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You are a data extraction assistant. Return only valid JSON."},
            {"role": "user", "content": prompt}
        ],
        temperature=0
    )
    
    # Parse response
    ai_response = response.content[0].text
    
    # Remove markdown if present
    if '```json' in ai_response:
        ai_response = ai_response.split('```json')[1].split('```')[0]
    elif '```' in ai_response:
        ai_response = ai_response.split('```')[1].split('```')[0]
    
    products = json.loads(ai_response.strip())
    
    print(f"✅ Extracted {len(products)} products")
    
    return products

def main():
    """Main scraper function"""
    
    print("=" * 50)
    print("🎯 GetDealScout - Costco Scraper")
    print("=" * 50)
    
    # Scrape Costco
    products = scrape_costco_deals()
    
    # Save to Airtable
    print(f"\n💾 Saving {len(products)} products to Airtable...")
    
    for product in products:
        try:
            save_opportunity(product)
        except Exception as e:
            print(f"❌ Error saving {product.get('name', 'Unknown')}: {e}")
    
    print("\n" + "=" * 50)
    print(f"✅ Complete! Scraped {len(products)} products")
    print("=" * 50)

if __name__ == "__main__":
    main()
