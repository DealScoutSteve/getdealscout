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

def check_token_status():
    """
    Check Keepa token status to see if we have enough tokens
    
    Endpoint: https://keepa.com/#!discuss/t/retrieve-token-status/1305
    
    Returns:
        dict with token info or None if error
    """
    url = f"{KEEPA_BASE_URL}/token"
    params = {'key': KEEPA_API_KEY}
    
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        tokens_left = data.get('tokensLeft', 0)
        refill_in = data.get('refillIn', 0)  # milliseconds
        refill_rate = data.get('refillRate', 0)  # tokens per minute
        
        print(f"🪙 Token Status:")
        print(f"   Available: {tokens_left}")
        print(f"   Refill rate: {refill_rate}/min")
        print(f"   Next refill: {refill_in/1000:.0f}s")
        
        return {
            'tokens_left': tokens_left,
            'refill_in_ms': refill_in,
            'refill_rate': refill_rate,
            'has_enough': tokens_left >= 10
        }
        
    except Exception as e:
        print(f"⚠️  Could not check token status: {e}")
        return None

def validate_best_amazon_match(costco_product, amazon_results):
    """
    Use AI to evaluate all Amazon results and pick the best match
    
    Args:
        costco_product: Dict with Costco product details (name, brand, price, etc.)
        amazon_results: List of Keepa product objects
        
    Returns:
        Best matching product data or None if no good match
    """
    from openai import OpenAI
    
    client = OpenAI(api_key=config.OPENAI_API_KEY)
    
    # Format results for AI
    results_text = ""
    for i, result in enumerate(amazon_results[:10], 1):
        title = result.get('title', 'No title')
        asin = result.get('asin', 'No ASIN')
        results_text += f"{i}. [{asin}] {title}\n"
    
    # Build comprehensive prompt with full context
    full_name = costco_product['name']
    cleaned_name = costco_product.get('cleaned_name', '')
    brand = costco_product.get('brand', 'Unknown')
    
    prompt = f"""You are an expert at matching retail products between Costco and Amazon.

Costco Product Details:
- Full Name: {full_name}
- Brand: {brand}
- Costco SKU: {costco_product.get('sku', 'N/A')}
{f"- Cleaned Search Name: {cleaned_name}" if cleaned_name else ""}

Amazon Search Results (pick the best match):
{results_text}

Matching Rules:
1. Brand MUST match exactly (case-insensitive)
2. Product type must be the same (laptop vs tablet, toothpaste vs mouthwash, etc.)
3. Prefer EXACT quantity/size matches (5-pack = 5-pack, 16GB = 16GB, NOT single unit)
4. Pay attention to key specs from full name (RAM, storage, screen size, oz, count, etc.)
5. Minor wording differences are OK (capitalization, punctuation, word order)
6. If no good match exists, return confidence 0

Examples of GOOD matches:
- Costco: "HP Laptop 17.3 inch, 16GB RAM, 1TB SSD" → Amazon: "HP 17.3" Laptop with 16GB Memory and 1TB Storage" ✅
- Costco: "Crest Toothpaste, 5.9 oz, 5-pack" → Amazon: "Crest Pro-Health Advanced, 5.9oz, Pack of 5" ✅

Examples of BAD matches:
- Costco: "HP Laptop 16GB" → Amazon: "HP Laptop 8GB" ❌ (wrong spec)
- Costco: "Toothpaste 5-pack" → Amazon: "Toothpaste Single Tube" ❌ (wrong quantity)

Return ONLY valid JSON (no markdown, no code blocks):
{{
    "best_match_index": 1,
    "confidence": 95,
    "reason": "Exact match: same brand, specs, and pack size"
}}
"""
    
    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a product matching expert. Return ONLY valid JSON with no markdown formatting."},
                {"role": "user", "content": prompt}
            ],
            temperature=0
        )
        
        ai_response = response.choices[0].message.content.strip()
        
        # Clean response (remove markdown if present)
        if '```json' in ai_response:
            ai_response = ai_response.split('```json')[1].split('```')[0]
        elif '```' in ai_response:
            ai_response = ai_response.split('```')[1].split('```')[0]
        
        match_data = json.loads(ai_response.strip())
        
        confidence = match_data.get('confidence', 0)
        
        if confidence >= 75:  # High confidence threshold
            best_index = match_data['best_match_index'] - 1  # Convert to 0-indexed
            reason = match_data.get('reason', 'AI validation passed')
            
            print(f"   🤖 AI picked result #{match_data['best_match_index']}: {confidence}% confidence")
            print(f"      Reason: {reason}")
            
            return parse_keepa_product(amazon_results[best_index])
        else:
            print(f"   ❌ AI confidence too low ({confidence}%) - no good match")
            return None
            
    except Exception as e:
        print(f"   ⚠️  AI validation error: {e}")
        # Fallback: use first result
        print(f"   ⚠️  Falling back to first result")
        return parse_keepa_product(amazon_results[0]) if amazon_results else None

def search_amazon_product(product_name, brand=None):
    """
    Search Amazon using Keepa's /search endpoint (Product Search)
    Returns top results for AI validation
    
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
            # Return ALL results for AI validation
            return data['products']
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
    else:
        status = 'Risky'  # Changed from 'Skip' - anything below 60% is risky
    
    confidence = max(0, min(100, confidence))
    is_valid = confidence >= 60
    
    return is_valid, confidence, status, warnings

def match_products(test_mode=False, batch_size=None, max_days_old=14, check_tokens=False):
    """
    Main matching function with smart update scheduling
    
    Args:
        test_mode: Process only 5 products for testing
        batch_size: Max products to process (for token management)
        max_days_old: Re-match products older than this many days (default: 14)
        check_tokens: Check token availability before processing
    """
    from datetime import datetime, timedelta
    
    print("=" * 70)
    print("🔍 AMAZON PRODUCT MATCHING WITH VALIDATION")
    if test_mode:
        print("🧪 TEST MODE - Processing only 5 products")
    elif batch_size == 1:
        print("⚡ CONTINUOUS MODE - Processing 1 product every 10 minutes")
    elif batch_size:
        print(f"📦 BATCH MODE - Processing up to {batch_size} products")
    print("=" * 70)
    print()
    
    # Check token status if requested
    if check_tokens:
        token_status = check_token_status()
        if token_status and not token_status['has_enough']:
            print(f"⏸️  Insufficient tokens ({token_status['tokens_left']}/10 needed)")
            print(f"⏰ Wait {token_status['refill_in_ms']/1000:.0f}s for next token")
            return
        print()
    
    # Get products smartly
    if test_mode:
        all_products = utils.get_all_products()
        products = all_products[:5]
        print("🔧 DEBUG MODE: Testing with first 5 products (any status)")
    else:
        # Priority 1: New products (never matched)
        new_products = utils.get_products_by_status('New')
        
        # Priority 2: Products not updated in last X days
        cutoff_date = datetime.now() - timedelta(days=max_days_old)
        cutoff_str = cutoff_date.strftime('%Y-%m-%dT%H:%M:%S.000Z')
        
        # Get all matched products
        all_matched = utils.get_airtable_table('Products').all(
            formula=f"AND({{Status}} != 'New', OR({{Last Updated}} = BLANK(), IS_BEFORE({{Last Updated}}, '{cutoff_str}')))"
        )
        
        # Sort matched products: Profitable first, then oldest
        profitable_old = [p for p in all_matched if p['fields'].get('Status') == 'Profitable']
        potential_old = [p for p in all_matched if p['fields'].get('Status') == 'Potential']
        other_old = [p for p in all_matched if p['fields'].get('Status') not in ['Profitable', 'Potential']]
        
        # Combine: New → Profitable Old → Potential Old → Other Old
        products = new_products + profitable_old + potential_old + other_old
        
        print(f"📋 Priority queue:")
        print(f"   🆕 New products: {len(new_products)}")
        print(f"   💰 Profitable (>{max_days_old}d old): {len(profitable_old)}")
        print(f"   ⚠️  Potential (>{max_days_old}d old): {len(potential_old)}")
        print(f"   📦 Other (>{max_days_old}d old): {len(other_old)}")
        
        # Apply batch size limit if specified
        if batch_size and len(products) > batch_size:
            print(f"⚠️  Limiting to {batch_size} products (token management)")
            products = products[:batch_size]
    
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
        
        # Get both names for different purposes
        original_name = fields.get('Product Name')
        cleaned_name = fields.get('Cleaned Product Name')
        search_name = cleaned_name or original_name  # Use cleaned for search
        brand = fields.get('Brand')
        costco_price = fields.get('Costco Price', 0)
        costco_sku = fields.get('Costco SKU')
        
        print(f"[{i}/{len(products)}] {search_name[:60]}...")
        
        # Search Amazon via Keepa (returns list of results)
        amazon_results = search_amazon_product(search_name, brand)
        
        # Use AI to validate and pick best match
        # Pass BOTH names for better context!
        if amazon_results:
            amazon_data = validate_best_amazon_match({
                'name': original_name,  # Full Costco name for AI context
                'cleaned_name': cleaned_name,  # Cleaned name for reference
                'brand': brand,
                'price': costco_price,
                'sku': costco_sku
            }, amazon_results)
        else:
            amazon_data = None
        
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
            
            # Check if Amazon price changed (for price history)
            old_amazon_price = fields.get('Amazon Price')
            price_changed = old_amazon_price and old_amazon_price != amazon_data['amazon_price']
            
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
            
            # Create Price History record if price changed
            if price_changed:
                try:
                    utils.create_price_history(
                        costco_sku=fields.get('Costco SKU'),
                        product_name=fields.get('Product Name'),
                        old_price=old_amazon_price,
                        new_price=amazon_data['amazon_price'],
                        product_record_id=record['id']
                    )
                    print(f"   📊 Price changed: ${old_amazon_price:.2f} → ${amazon_data['amazon_price']:.2f}")
                except Exception as e:
                    print(f"   ⚠️  Could not create price history: {e}")
            
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
    import sys
    
    # Parse command line arguments
    TEST_MODE = '--test' in sys.argv
    CHECK_TOKENS = '--check-tokens' in sys.argv
    batch_size = None
    max_days_old = 14  # Default: re-match products older than 14 days
    
    for arg in sys.argv[1:]:
        if arg.startswith('--batch-size='):
            batch_size = int(arg.split('=')[1])
        elif arg.startswith('--max-days-old='):
            max_days_old = int(arg.split('=')[1])
    
    match_products(test_mode=TEST_MODE, batch_size=batch_size, max_days_old=max_days_old, check_tokens=CHECK_TOKENS)
