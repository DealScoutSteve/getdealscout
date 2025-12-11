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
            'q': 'OFF',  # Search for deals
            'locale': 'en-US',
            'start': start,
            'rows': min(24, max_products - start),  # Don't exceed max_products
            'sort': sort_by
        }
        
        try:
            response = requests.get(COSTCO_API_URL, params=params, headers=headers, timeout=15)
            response.raise_for_status()
            
            data = response.json()
            products = data['response']['docs']
            
            print(f"   Fetched batch: {start}-{start + len(products)} (Total available: {data['response']['numFound']})")
            
            all_products.extend(products)
            
            # Stop if we got fewer results than requested (end of results)
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
