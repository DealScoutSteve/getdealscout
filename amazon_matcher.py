#!/usr/bin/env python3
"""
Match Costco products with Amazon using Keepa Product API
"""

import requests
import time
import json
from datetime import datetime
import config
import utils

KEEPA_API_KEY = config.KEEPA_API_KEY
KEEPA_BASE_URL = "https://api.keepa.com"

# Validation Thresholds
SALES_RANK_EXCELLENT = 10000
SALES_RANK_GOOD = 50000
SALES_RANK_POOR = 100000
MIN_PROFIT = 10.0
MIN_OFFERS = 2

def search_amazon_product(product_name, brand=None):
    """
    Search Amazon using Keepa's /search endpoint (Product Search)
    
    Docs: https://keepa.com/#!discuss/t/product-searches/109
    Cost: 10 tokens per page (up to 10 results)
    """
    
    # Build search query
    search_query = product_name
    if brand and brand.lower() not in product_name.lower():
        search_query = f"{brand} {product_name}"
    
    search_query = search_query.replace(',', '').replace('  ', ' ')[:100]
    
    print(f"   🔍 Query: '{search_query}'")
    
    # Use /search endpoint with correct parameters
    url = f"{KEEPA_BASE_URL}/search"
    params = {
        'key': KEEPA_API_KEY,
        'domain': 1,  # US Amazon
        'type': 'product',  # Search for products (not categories)
        'term': search_query,
        'stats': 90,  # Include 90-day stats
        'page': 0  # First page only (10 results max)
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        
        print(f"   📡 Status: {response.status_code}")
        
        response.raise_for_status()
        
        data = response.json()
        
        print(f"   📦 Keys: {list(data.keys())}")
        
        # Check for errors
        if 'error' in data:
            print(f"   ❌ Keepa error: {data['error'].get('message', data['error'])}")
            return None
        
        # /search returns products array with full product objects
        if 'products' in data and len(data['products']) > 0:
            print(f"   ✅ Found {len(data['products'])} results")
            # Return first (best) match
            product = data['products'][0]
            return parse_keepa_product(product)
        else:
            print(f"   ❌ No products returned")
            return None
            
    except requests.exceptions.HTTPError as e:
        print(f"   ❌ HTTP Error: {e}")
        try:
            print(f"   Response: {response.text[:300]}")
        except:
            pass
        return None
    except Exception as e:
        print(f"   ⚠️  Error: {e}")
        return None

def parse_keepa_product(keepa_data):
    """Parse Keepa product data"""
    
    # Get Buy Box price
    amazon_price = None
    price_history = []
    
    if keepa_data.get('csv') and len(keepa_data['csv']) > 0:
        if len(keepa_data['csv']) > 1 and keepa_data['csv'][1]:
            price_data = keepa_data['csv'][1]
            
            if len(price_data) > 0:
                latest_price = price_data[-1]
                if latest_price > 0:
                    amazon_price = latest_price / 100.0
            
            price_history = [p / 100.0 for p in price_data if p > 0]
    
    # Get FBA fees
    fba_fees = 0
    if keepa_data.get('fbaFees'):
        pick_pack = keepa_data['fbaFees'].get('pickAndPackFee', 0)
        storage = keepa_data['fbaFees'].get('storageFee', 0)
        fba_fees = (pick_pack + storage) / 100.0
    
    # Get sales rank
    sales_rank = None
    if keepa_data.get('csv') and len(keepa_data['csv']) > 3:
        if keepa_data['csv'][3]:
            latest_rank = keepa_data['csv'][3][-1]
            if latest_rank > 0:
                sales_rank = latest_rank
    
    # Get offer count
    offer_count = keepa_data.get('offerCountFBA', 0)
    
    # Get category
    category = 'Unknown'
    if keepa_data.get('categoryTree') and len(keepa_data['categoryTree']) > 0:
        category = keepa_data['categoryTree'][0].get('name', 'Unknown')
    
    return {
        'asin': keepa_data.get('asin'),
        'title': keepa_data.get('title'),
        'amazon_price': amazon_price,
        'amazon_url': f"https://www.amazon.com/dp/{keepa_data.get('asin')}" if keepa_data.get('asin') else None,
        'fba_fees': fba_fees,
        'sales_rank': sales_rank,
        'category': category,
        'offer_count': offer_count,
        'price_history': price_history
    }

def calculate_profit(costco_price, amazon_price, fba_fees):
    """Calculate profit and ROI"""
    
    if not costco_price or not amazon_price:
        return {'profit': None, 'roi': None}
    
    amazon_referral = amazon_price * 0.15
    profit = amazon_price - costco_price - fba_fees - amazon_referral
    roi = (profit / costco_price) * 100 if costco_price > 0 else 0
    
    return {
        'profit': round(profit, 2),
        'roi': round(roi, 2)
    }

def validate_opportunity(amazon_data, profit):
    """4-layer validation with confidence scoring"""
    
    warnings = []
    confidence = 50
    
    # Layer 1: Sales Rank
    sales_rank = amazon_data.get('sales_rank')
    if sales_rank:
        if sales_rank < SALES_RANK_EXCELLENT:
            confidence += 25
            warnings.append(f"🔥 Hot seller (rank {sales_rank:,})")
        elif sales_rank < SALES_RANK_GOOD:
            confidence += 15
            warnings.append(f"✅ Good sales (rank {sales_rank:,})")
        elif sales_rank < SALES_RANK_POOR:
            confidence += 5
            warnings.append(f"⚠️ Moderate sales (rank {sales_rank:,})")
        else:
            confidence -= 20
            warnings.append(f"❌ Slow seller (rank {sales_rank:,})")
    else:
        confidence -= 10
        warnings.append("⚠️ No sales rank data")
    
    # Layer 2: Profit Margin
    if profit:
        if profit >= 50:
            confidence += 20
            warnings.append(f"💰 Excellent profit (${profit:.2f})")
        elif profit >= 25:
            confidence += 15
            warnings.append(f"💵 Good profit (${profit:.2f})")
        elif profit >= MIN_PROFIT:
            confidence += 10
            warnings.append(f"💲 Decent profit (${profit:.2f})")
        else:
            confidence -= 30
            warnings.append(f"❌ Low profit (${profit:.2f})")
    else:
        confidence -= 40
        warnings.append("❌ No profit data")
    
    # Layer 3: Competition
    offer_count = amazon_data.get('offer_count', 0)
    if offer_count >= 5:
        confidence += 10
        warnings.append(f"✅ Multiple sellers ({offer_count})")
    elif offer_count >= MIN_OFFERS:
        confidence += 5
        warnings.append(f"⚠️ Few sellers ({offer_count})")
    else:
        confidence -= 15
        warnings.append(f"❌ Limited sellers ({offer_count})")
    
    # Layer 4: Price Stability
    price_history = amazon_data.get('price_history', [])
    current_price = amazon_data.get('amazon_price')
    
    if len(price_history) >= 10 and current_price:
        avg_price = sum(price_history) / len(price_history)
        price_variance = abs(current_price - avg_price) / avg_price
        
        if price_variance < 0.10:
            confidence += 10
            warnings.append(f"✅ Stable price (${current_price:.2f})")
        elif price_variance < 0.25:
            confidence += 5
            warnings.append(f"⚠️ Price varies")
        else:
            confidence -= 15
            warnings.append(f"❌ Volatile price")
    
    # Determine Status
    if confidence >= 80:
        status = 'Profitable'
    elif confidence >= 60:
        status = 'Potential'
    elif confidence >= 40:
        status = 'Risky'
    else:
        status = 'Skip'
    
    confidence = max(0, min(100, confidence))
    is_valid = confidence >= 60
    
    return is_valid, confidence, status, warnings

def match_products(test_mode=False):
    """Main matching function"""
    
    print("=" * 70)
    print("🔍 AMAZON PRODUCT MATCHING WITH VALIDATION")
    if test_mode:
        print("🧪 TEST MODE - Processing only 5 products")
    print("=" * 70)
    print()
    
    # Get products
    if test_mode:
        all_products = utils.get_all_products()
        products = all_products[:5]
        print("🔧 DEBUG MODE: Testing with first 5 products (any status)")
    else:
        products = utils.get_products_by_status('New')
    
    if not products:
        print("📭 No products to match!")
        return
    
    print(f"📦 Found {len(products)} products to match")
    print()
    
    matched_count = 0
    not_found_count = 0
    profitable_count = 0
    potential_count = 0
    
    for i, record in enumerate(products, 1):
        fields = record['fields']
        
        product_name = fields.get('Cleaned Product Name') or fields.get('Product Name')
        brand = fields.get('Brand')
        costco_price = fields.get('Costco Price', 0)
        
        print(f"[{i}/{len(products)}] {product_name[:60]}...")
        
        amazon_data = search_amazon_product(product_name, brand)
        
        if amazon_data and amazon_data['amazon_price']:
            profit_data = calculate_profit(
                costco_price,
                amazon_data['amazon_price'],
                amazon_data['fba_fees']
            )
            
            is_valid, confidence, status, warnings = validate_opportunity(
                amazon_data,
                profit_data['profit']
            )
            
            utils.update_product(record['id'], {
                'Amazon ASIN': amazon_data['asin'],
                'Amazon Price': amazon_data['amazon_price'],
                'Amazon URL': amazon_data['amazon_url'],
                'FBA Fees': amazon_data['fba_fees'],
                'Sales Rank': amazon_data['sales_rank'],
                'Category': amazon_data['category'],
                'Confidence Score': confidence,  # NEW field (writable number)
                'Status': status,
                'Last Updated': datetime.now().strftime('%Y-%m-%dT%H:%M:%S.000Z')
            })
            
            profit_str = f"${profit_data['profit']:.2f}" if profit_data['profit'] else "N/A"
            roi_str = f"{profit_data['roi']:.1f}%" if profit_data['roi'] else "N/A"
            
            print(f"   💰 Amazon: ${amazon_data['amazon_price']:.2f} | Profit: {profit_str} | ROI: {roi_str}")
            print(f"   📊 Confidence: {confidence}% | Status: {status}")
            
            for warning in warnings[:2]:
                print(f"      {warning}")
            
            matched_count += 1
            if status == 'Profitable':
                profitable_count += 1
            elif status == 'Potential':
                potential_count += 1
                
        else:
            utils.update_product(record['id'], {
                'Status': 'Not Found',
                'Last Updated': datetime.now().strftime('%Y-%m-%dT%H:%M:%S.000Z')
            })
            print(f"   ❌ Not found on Amazon")
            not_found_count += 1
        
        print()
        time.sleep(1.1)
    
    print("=" * 70)
    print("📊 MATCHING SUMMARY")
    print("=" * 70)
    print(f"✅ Total matched: {matched_count}")
    print(f"💰 Profitable (80%+ confidence): {profitable_count}")
    print(f"⚠️  Potential (60-79% confidence): {potential_count}")
    print(f"❌ Not found: {not_found_count}")
    print("=" * 70)
    print()
    print("💡 TIP: Filter Airtable by Status='Profitable' to see best deals!")

if __name__ == "__main__":
    TEST_MODE = True
    match_products(test_mode=TEST_MODE)
