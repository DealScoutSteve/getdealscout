#!/usr/bin/env python3
"""
Fetch Costco deals and save to Airtable

This script:
1. Fetches deals from Costco's API
2. Parses product data
3. Saves to Airtable Products table
4. Tracks price history
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
    
    # Extract product name and brand
    product_name = item.get('item_product_name', 'Unknown')
    brand = item.get('Brand_attr', [None])[0] if item.get('Brand_attr') else None
    
    # Determine product group (for better organization)
    product_group = 'Unknown'
    if brand:
        product_group = brand
    
    return {
        'Product Name': product_name,
        'Product Group': product_group,
        'Brand': brand,
        'Costco SKU': item.get('item_number'),
        'Costco Price': item.get('item_location_pricing_salePrice'),
        'Costco Original Price': item.get('item_location_pricing_listPrice'),
        'Costco Discount': discount_amount,
        'Costco URL': f"https://www.costco.com/{item.get('item_number')}.product.{item.get('item_number')}.html" if item.get('item_number') else None,
        'In Stock': item.get('item_location_availability') == 'in stock',
        'Rating': item.get('item_ratings'),
        'Image URL': item.get('item_collateral_primaryimage'),
        'Category': item.get('item_primary_category', ['Unknown'])[0] if item.get('item_primary_category') else 'Unknown',
        'Date Found': datetime.now().isoformat(),
        'Last Updated': datetime.now().isoformat()
    }

def save_to_airtable(products):
    """
    Save products to Airtable with historical price tracking
    
    Args:
        products: List of product dicts (Airtable formatted)
        
    Returns:
        Number of products saved/updated
    """
    
    print(f"\n💾 Saving {len(products)} products to Airtable...")
    
    saved_count = 0
    updated_count = 0
    skipped_count = 0
    error_count = 0
    
    for product in products:
        try:
            costco_sku = product.get('Costco SKU')
            if not costco_sku:
                print(f"   ⚠️  Skipping product without SKU")
                skipped_count += 1
                continue
            
            # Check if product already exists
            existing = utils.find_product_by_costco_sku(costco_sku)
            
            if existing:
                # Product exists - check if price changed
                existing_price = existing['fields'].get('Costco Price')
                new_price = product.get('Costco Price')
                
                if existing_price != new_price:
                    # Price changed - update and log to history
                    utils.update_product(existing['id'], {
                        'Costco Price': new_price,
                        'Costco Original Price': product.get('Costco Original Price'),
                        'Costco Discount': product.get('Costco Discount'),
                        'In Stock': product.get('In Stock'),
                        'Last Updated': datetime.now().isoformat()
                    })
                    
                    # Log price change to Price History table
                    utils.log_price_history(
                        product_id=existing['id'],
                        costco_sku=costco_sku,
                        product_name=product['Product Name'],
                        old_price=existing_price,
                        new_price=new_price
                    )
                    
                    print(f"   🔄 Updated: {product['Product Name'][:50]}... (${existing_price} → ${new_price})")
                    updated_count += 1
                else:
                    # No price change - just update Last Updated
                    utils.update_product(existing['id'], {
                        'Last Updated': datetime.now().isoformat(),
                        'In Stock': product.get('In Stock')
                    })
                    print(f"   ⏭️  No change: {costco_sku}")
                    skipped_count += 1
            else:
                # New product - create it
                utils.create_product(product)
                print(f"   ✅ Saved: {product['Product Name'][:50]}... (${product['Costco Price']})")
                saved_count += 1
            
        except Exception as e:
            print(f"   ❌ Error saving product: {e}")
            error_count += 1
    
    print(f"\n📊 Summary:")
    print(f"   ✅ New products: {saved_count}")
    print(f"   🔄 Updated prices: {updated_count}")
    print(f"   ⏭️  No changes: {skipped_count}")
    print(f"   ❌ Errors: {error_count}")
    
    return saved_count + updated_count

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
    print(f"✅ SYNC COMPLETE! Processed {saved} products")
    print("=" * 70)

if __name__ == "__main__":
    main()
