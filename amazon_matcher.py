#!/usr/bin/env python3
"""
Match Costco products with Amazon and calculate arbitrage opportunities

This script:
1. Gets products with Status='New' from Airtable
2. Searches Keepa API for matching Amazon products
3. Validates opportunities (sales rank, price stability, offer count)
4. Calculates profit margins with confidence scoring
5. Updates Airtable with Amazon data and opportunity scores
"""

import requests
import time
from datetime import datetime
import config
import utils

# Keepa API Configuration
KEEPA_API_KEY = config.KEEPA_API_KEY
KEEPA_BASE_URL = "https://api.keepa.com"

# Validation Thresholds
SALES_RANK_EXCELLENT = 10000   # Hot sellers
SALES_RANK_GOOD = 50000         # Decent sales
SALES_RANK_POOR = 100000        # Slow movers
MIN_PROFIT = 10.0               # Minimum profit to consider
MIN_OFFERS = 2                  # Minimum FBA sellers for confidence

def search_amazon_product(product_name, brand=None):
    """
    Search for product on Amazon via Keepa
    
    Args:
        product_name: Product name from Costco
        brand: Brand name (optional, improves accuracy)
        
    Returns:
        Dict with Amazon data or None if not found
    """
    
    # Build search query
    search_query = product_name
    if brand:
        search_query = f"{brand} {product_name}"
    
    # Clean up query (remove special chars that confuse search)
    search_query = search_query.replace(',', '').replace('  ', ' ')[:100]
    
    url = f"{KEEPA_BASE_URL}/product"
    params = {
        'key': KEEPA_API_KEY,
        'domain': 1,  # 1 = Amazon.com (US)
        'type': 'search',
        'term': search_query,
        'stats': 90,  # Get 90-day stats
        'only-live-offers': 1,  # Only products currently for sale
        'offers': 20  # Get offer data for validation
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        # Check if we got results
        if 'products' in data and len(data['products']) > 0:
            # Return first match (best match by relevance)
            product = data['products'][0]
            return parse_keepa_product(product)
        else:
            return None
            
    except Exception as e:
        print(f"   ⚠️  Keepa API error: {e}")
        return None

def parse_keepa_product(keepa_data):
    """
    Parse Keepa API response into usable data
    
    Args:
        keepa_data: Raw product data from Keepa
        
    Returns:
        Dict with parsed Amazon data
    """
    
    # Keepa stores prices in "Keepa Time" and "Keepa Price" formats
    # Prices are in cents, need to divide by 100
    
    # Get current Amazon BUY BOX price (what customers actually pay)
    amazon_price = None
    price_history = []
    
    if keepa_data.get('csv') and len(keepa_data['csv']) > 0:
        # CSV index 1 = Amazon Buy Box price (NEW condition)
        if len(keepa_data['csv']) > 1 and keepa_data['csv'][1]:
            price_data = keepa_data['csv'][1]
            
            # Get latest price
            if len(price_data) > 0:
                latest_price = price_data[-1]
                if latest_price > 0:
                    amazon_price = latest_price / 100.0
            
            # Get price history for validation
            price_history = [p / 100.0 for p in price_data if p > 0]
    
    # Get FBA fees
    fba_fees = 0
    if keepa_data.get('fbaFees'):
        pick_pack = keepa_data['fbaFees'].get('pickAndPackFee', 0)
        storage = keepa_data['fbaFees'].get('storageFee', 0)
        fba_fees = (pick_pack + storage) / 100.0
    
    # Get sales rank (lower = better/faster selling)
    sales_rank = None
    if keepa_data.get('csv') and len(keepa_data['csv']) > 3:
        if keepa_data['csv'][3]:  # Sales rank data
            latest_rank = keepa_data['csv'][3][-1]
            if latest_rank > 0:
                sales_rank = latest_rank
    
    # Get offer count (how many FBA sellers)
    offer_count = keepa_data.get('offerCountFBA', 0)
    
    # Get category
    category = 'Unknown'
    if keepa_data.get('categoryTree') and len(keepa_data['categoryTree']) > 0:
        category = keepa_data['categoryTree'][0].get('name', 'Unknown')
    
    # Get image
    image_url = None
    if keepa_data.get('imagesCSV'):
        images = keepa_data['imagesCSV'].split(',')
        if len(images) > 0:
            image_url = f"https://images-na.ssl-images-amazon.com/images/I/{images[0]}.jpg"
    
    return {
        'asin': keepa_data.get('asin'),
        'title': keepa_data.get('title'),
        'amazon_price': amazon_price,
        'amazon_url': f"https://www.amazon.com/dp/{keepa_data.get('asin')}" if keepa_data.get('asin') else None,
        'fba_fees': fba_fees,
        'sales_rank': sales_rank,
        'category': category,
        'image_url': image_url,
        'offer_count': offer_count,
        'price_history': price_history
    }

def calculate_profit(costco_price, amazon_price, fba_fees):
    """
    Calculate arbitrage profit and ROI
    
    Args:
        costco_price: Cost to buy from Costco
        amazon_price: Selling price on Amazon (Buy Box)
        fba_fees: Amazon FBA fulfillment fees
        
    Returns:
        Dict with profit and ROI
    """
    
    if not costco_price or not amazon_price:
        return {'profit': None, 'roi': None}
    
    # Amazon referral fee (typically 15% for most categories)
    amazon_referral = amazon_price * 0.15
    
    # Calculate profit
    # Profit = Amazon Price - Costco Cost - FBA Fees - Amazon Referral
    profit = amazon_price - costco_price - fba_fees - amazon_referral
    
    # ROI percentage
    roi = (profit / costco_price) * 100 if costco_price > 0 else 0
    
    return {
        'profit': round(profit, 2),
        'roi': round(roi, 2)
    }

def validate_opportunity(amazon_data, profit):
    """
    Validate if this is a real, reliable arbitrage opportunity
    
    Uses multiple signals:
    - Sales rank (does it actually sell?)
    - Profit margin (is it worth it?)
    - Offer count (is price stable?)
    - Price history (is current price typical?)
    
    Args:
        amazon_data: Dict with Amazon product data
        profit: Calculated profit amount
        
    Returns:
        Tuple of (is_valid, confidence_score, status, warnings)
    """
    
    warnings = []
    confidence = 50  # Start at neutral
    
    # === VALIDATION 1: Sales Rank (Product Velocity) ===
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
    
    # === VALIDATION 2: Profit Margin ===
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
    
    # === VALIDATION 3: Competition/Offer Count ===
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
    
    # === VALIDATION 4: Price Stability ===
    price_history = amazon_data.get('price_history', [])
    current_price = amazon_data.get('amazon_price')
    
    if len(price_history) >= 10 and current_price:
        avg_price = sum(price_history) / len(price_history)
        price_variance = abs(current_price - avg_price) / avg_price
        
        if price_variance < 0.10:  # Within 10% of average
            confidence += 10
            warnings.append(f"✅ Stable price (${current_price:.2f})")
        elif price_variance < 0.25:  # Within 25%
            confidence += 5
            warnings.append(f"⚠️ Price varies (${current_price:.2f} vs avg ${avg_price:.2f})")
        else:
            confidence -= 15
            warnings.append(f"❌ Volatile price (${current_price:.2f} vs avg ${avg_price:.2f})")
    
    # === DETERMINE STATUS ===
    if confidence >= 80:
        status = 'Profitable'  # High confidence - BUY!
    elif confidence >= 60:
        status = 'Potential'   # Worth investigating
    elif confidence >= 40:
        status = 'Risky'       # Proceed with caution
    else:
        status = 'Skip'        # Not worth it
    
    # Ensure confidence is 0-100
    confidence = max(0, min(100, confidence))
    
    is_valid = confidence >= 60
    
    return is_valid, confidence, status, warnings

def match_products():
    """
    Main function to match Costco products with Amazon
    """
    
    print("=" * 70)
    print("🔍 AMAZON PRODUCT MATCHING WITH VALIDATION")
    print("=" * 70)
    print()
    
    # Get products that need Amazon matching (Status = 'New')
    products = utils.get_products_by_status('New')
    
    if not products:
        print("📭 No new products to match. All done!")
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
        
        # Search Amazon via Keepa
        amazon_data = search_amazon_product(product_name, brand)
        
        if amazon_data and amazon_data['amazon_price']:
            # Calculate profit
            profit_data = calculate_profit(
                costco_price,
                amazon_data['amazon_price'],
                amazon_data['fba_fees']
            )
            
            # Validate opportunity
            is_valid, confidence, status, warnings = validate_opportunity(
                amazon_data,
                profit_data['profit']
            )
            
            # Update Airtable
            utils.update_product(record['id'], {
                'Amazon ASIN': amazon_data['asin'],
                'Amazon Price': amazon_data['amazon_price'],
                'Amazon URL': amazon_data['amazon_url'],
                'FBA Fees': amazon_data['fba_fees'],
                'Sales Rank': amazon_data['sales_rank'],
                'Category': amazon_data['category'],
                'Profit': profit_data['profit'],
                'ROI %': profit_data['roi'],
                'Opportunity Score': confidence,
                'Status': status,
                'Last Updated': datetime.now().strftime('%Y-%m-%dT%H:%M:%S.000Z')
            })
            
            # Display results
            profit_str = f"${profit_data['profit']:.2f}" if profit_data['profit'] else "N/A"
            roi_str = f"{profit_data['roi']:.1f}%" if profit_data['roi'] else "N/A"
            
            print(f"   💰 Amazon: ${amazon_data['amazon_price']:.2f} | Profit: {profit_str} | ROI: {roi_str}")
            print(f"   📊 Confidence: {confidence}% | Status: {status}")
            
            # Show key warnings
            for warning in warnings[:2]:  # Top 2 most important
                print(f"      {warning}")
            
            matched_count += 1
            if status == 'Profitable':
                profitable_count += 1
            elif status == 'Potential':
                potential_count += 1
                
        else:
            # No match found
            utils.update_product(record['id'], {
                'Status': 'Not Found',
                'Last Updated': datetime.now().strftime('%Y-%m-%dT%H:%M:%S.000Z')
            })
            print(f"   ❌ Not found on Amazon")
            not_found_count += 1
        
        print()
        
        # Rate limiting: Keepa allows ~1 request per second
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
    match_products()
