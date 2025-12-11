#!/usr/bin/env python3
"""
Fetch Costco deals and save to Airtable

This script:
1. Fetches deals from Costco's API
2. Parses product data
3. Saves to Airtable Products table
"""

import requests
import json
from datetime import datetime
import config
import utils

# Costco API Configuration
COSTCO_API_URL = "https://search.costco.com/api/apps/www_costco_com/query/www_costco_com_search"
COSTCO_API_KEY = "273db6be-f015-4de7-b0d6-dd4746ccd5c3"

def fetch_costco_deals(max_products=100, sort_by='item_page_views desc'):
    """
    Fetch deals from Costco's search API
    
    Args:
        max_products: Maximum number of products to fetch
        sort_by: Sort order (default: most popular)
        
    Returns:
        List of product dictionaries
    """
    
    print(f"🛒 Fetching up to {max_products} Costco deals...")
    
    headers = {
        'x-api-key': COSTCO_API_KEY,
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
        'Accept': 'application/json',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://www.costco.com/',
        'Origin': 'https://www.costco.com'
    }
    
    all_products = []
    
    # Fetch in batches of 24 (API pagination)
    for start in range(0, max_products, 24):
        params = {
            'expoption': 'lucidworks',
            'q': 'OFF',
            'locale': 'en-US',
            'start': start,
            'rows': min(24, max_products - start),
            'sort': sort_by
        }
        
        try:
            response = requests.get(COSTCO_API_URL, params=params, headers=headers, timeout=15)
            response.raise_for_status()
            
            data = response.json()
            products = data['response']['docs']
            
            print(f"   Fetched batch: {start}-{start + len(products)} (Total available: {data['response']['numFound']})")
            
            all_products.extend(products)
            
            if len(products) < params['rows']:
                break
                
        except requests.exceptions.RequestException as e:
            print(f"❌ Error fetching batch at {start}: {e}")
            break
    
    print(f"✅ Total products fetched: {len(all_products)}")
    return all_products

def parse_product(item):
    """
    Parse a Costco API product into our Airtable schema
    
    Args:
        item: Raw product dict from Costco API
        
    Returns:
        Dict formatted for Airtable Products table
    """
    
    discount_text = item.get('item_product_marketing_statement', '')
    
    discount_amount = None
    if 'OFF' in discount_text:
        if '-' in discount_text:
            try:
                discount_amount = float(discount_text.split('$')[1].split()[0])
            except:
                pass
        else:
            try:
                discount_amount = float(discount_text.replace('$', '').replace('OFF', '').strip())
            except:
                pass
    
    return {
        'Product Name': item.get('item_product_name', 'Unknown'),
        'Costco SKU': item.get('item_number'),
        'Costco Price': item.get('item_location_pricing_salePrice'),
        'Costco Original Price': item.get('item_location_pricing_listPrice'),
        'Costco Discount': discount_amount,
        'In Stock': item.get('item_location_availability') == 'in stock',
        'Brand': item.get('Brand_attr', [None])[0] if item.get('Brand_attr') else None,
        'Rating': item.get('item_ratings'),
        'Image URL': item.get('item_collateral_primaryimage'),
        'Costco URL': f"https://www.costco.com/p/-/{item.get('item_number')}" if item.get('item_number') else None,
        'Last Updated': datetime.now().isoformat(),
        'Status': 'New'
    }

def save_to_airtable(products):
    """
    Save products to Airtable
    
    Args:
        products: List of product dicts (Airtable formatted)
        
    Returns:
        Number of products saved
    """
    
    print(f"\n💾 Saving {len(products)} products to Airtable...")
    
    saved_count = 0
    skipped_count = 0
    error_count = 0
    
    for product in products:
        try:
            costco_sku = product.get('Costco SKU')
            if costco_sku:
                existing = utils.find_product_by_costco_sku(costco_sku)
                
                if existing:
                    print(f"   ⏭️  Skipping {costco_sku} (already exists)")
                    skipped_count += 1
                    continue
            
            utils.create_product(product)
            print(f"   ✅ Saved: {product['Product Name'][:50]}... (${product['Costco Price']})")
            saved_count += 1
            
        except Exception as e:
            print(f"   ❌ Error saving product: {e}")
            error_count += 1
    
    print(f"\n📊 Summary:")
    print(f"   ✅ Saved: {saved_count}")
    print(f"   ⏭️  Skipped (duplicates): {skipped_count}")
    print(f"   ❌ Errors: {error_count}")
    
    return saved_count

def main():
    """Main execution"""
    
    print("=" * 70)
    print("🛒 COSTCO TO AIRTABLE SYNC")
    print("=" * 70)
    print()
    
    raw_products = fetch_costco_deals(max_products=50)
    
    if not raw_products:
        print("❌ No products fetched. Exiting.")
        return
    
    print()
    
    print("📋 Parsing products...")
    parsed_products = [parse_product(item) for item in raw_products]
    print(f"✅ Parsed {len(parsed_products)} products")
    
    print()
    
    saved = save_to_airtable(parsed_products)
    
    print()
    print("=" * 70)
    print(f"✅ SYNC COMPLETE! Saved {saved} new products to Airtable")
    print("=" * 70)

if __name__ == "__main__":
    main()
