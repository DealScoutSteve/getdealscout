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

def fetch_product_by_asin(asin):
    """
    Fetch a specific product from Keepa by ASIN (for override functionality)
    
    Args:
        asin: Amazon ASIN to fetch
        
    Returns:
        Parsed product data or None if error
    """
    print(f"   🔍 Fetching ASIN: {asin}")
    
    url = f"{KEEPA_BASE_URL}/product"
    params = {
        'key': KEEPA_API_KEY,
        'domain': 1,  # US Amazon
        'asin': asin,
        'stats': 90  # Include 90-day stats
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        
        print(f"   📡 Status: {response.status_code}")
        
        response.raise_for_status()
        
        data = response.json()
        
        # Check for errors
        if 'error' in data:
            print(f"   ❌ Keepa error: {data['error'].get('message', data['error'])}")
            return None
        
        # Product endpoint returns products array
        if 'products' in data and len(data['products']) > 0:
            print(f"   ✅ Product found")
            product = data['products'][0]
            return parse_keepa_product(product)
        else:
            print(f"   ❌ Product not found")
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

def validate_best_amazon_match(costco_product, amazon_results):
    """
    Use AI to evaluate all Amazon results and pick the best match
    Enhanced with product metadata for higher accuracy
    
    Args:
        costco_product: Dict with Costco product details (name, brand, price, etc.)
        amazon_results: List of Keepa product objects (raw from API)
        
    Returns:
        Best matching product data or None if no good match
    """
    from openai import OpenAI
    
    client = OpenAI(api_key=config.OPENAI_API_KEY)
    
    # Parse all results and format for AI with enhanced metadata
    results_text = ""
    for i, result in enumerate(amazon_results[:10], 1):
        # Parse to get metadata
        parsed = parse_keepa_product(result)
        
        title = parsed.get('title', 'No title')
        asin = parsed.get('asin', 'No ASIN')
        price = parsed.get('amazon_price')
        item_count = parsed.get('item_count', 1)
        weight = parsed.get('package_weight')
        dims = parsed.get('package_dimensions')
        category = parsed.get('category', 'Unknown')
        
        # Build detailed result line
        result_line = f"{i}. [{asin}] {title}"
        
        # Add metadata if available
        meta = []
        if price:
            meta.append(f"${price:.2f}")
        if item_count and item_count > 1:
            meta.append(f"Pack of {item_count}")
        if weight:
            meta.append(f"{weight:.2f} lbs")
        if dims:
            meta.append(f"{dims['length']:.1f}×{dims['width']:.1f}×{dims['height']:.1f} in")
        meta.append(category)
        
        if meta:
            result_line += f" | {' | '.join(meta)}"
        
        results_text += result_line + "\n"
    
    # Build comprehensive prompt with full context
    full_name = costco_product['name']
    cleaned_name = costco_product.get('cleaned_name', '')
    brand = costco_product.get('brand', 'Unknown')
    costco_price = costco_product.get('price', 0)
    
    prompt = f"""You are an expert at matching retail products between Costco and Amazon.

Costco Product Details:
- Full Name: {full_name}
- Brand: {brand}
- Price: ${costco_price:.2f}
- Costco SKU: {costco_product.get('sku', 'N/A')}
{f"- Cleaned Search Name: {cleaned_name}" if cleaned_name else ""}

Amazon Search Results (pick the best match):
{results_text}

Critical Matching Rules:
1. **Brand MUST match exactly** (case-insensitive) - If brand doesn't match, confidence = 0
2. **Product type must be identical** (laptop vs tablet, toothpaste vs mouthwash, etc.)
3. **Pack quantity must match** - Pay CLOSE attention to pack counts:
   - "5-pack" in Costco = "Pack of 5" or "5-Count" on Amazon
   - Single item ≠ Multi-pack
   - If Costco shows pack size, Amazon MUST show same pack size
4. **Key specifications must match**:
   - Storage (16GB vs 32GB vs 64GB, etc.)
   - Screen size (11-inch vs 13-inch)
   - Weight/Volume (5.9 oz vs 3.5 oz)
   - Model numbers if present
5. **Price sanity check** - Amazon price should be in similar range to Costco (not 10× different)
6. **Category match** - Electronics should match Electronics, Health products should match Health, etc.
7. **Minor wording differences are OK** - Capitalization, punctuation, word order variations

Examples of GOOD matches:
- Costco: "HP Laptop 17.3 inch, 16GB RAM, 1TB SSD" → Amazon: "HP 17.3" Laptop with 16GB Memory and 1TB Storage" ✅
- Costco: "Crest Toothpaste, 5.9 oz, 5-pack" → Amazon: "Crest Pro-Health Advanced, 5.9oz, Pack of 5" ✅
- Costco: "iPad Air 11-inch, 128GB" → Amazon: "Apple iPad Air 11-inch (M3, 128GB)" ✅

Examples of BAD matches (return confidence 0):
- Costco: "HP Laptop 16GB" → Amazon: "HP Laptop 8GB" ❌ (wrong spec)
- Costco: "Toothpaste 5-pack" → Amazon: "Toothpaste Single Tube" ❌ (wrong quantity)
- Costco: "11-inch iPad" → Amazon: "13-inch iPad" ❌ (wrong size)
- Costco: "Crest Toothpaste" → Amazon: "Colgate Toothpaste" ❌ (wrong brand)

Return ONLY valid JSON (no markdown, no code blocks):
{{
    "best_match_index": 1,
    "confidence": 95,
    "reason": "Exact match: same brand, specs, and pack size"
}}

If no good match exists (brand mismatch, wrong specs, wrong quantity), return:
{{
    "best_match_index": 0,
    "confidence": 0,
    "reason": "No match found - [specific reason]"
}}
"""
    
    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a product matching expert. Return ONLY valid JSON with no markdown formatting. Be STRICT about pack quantities and specifications."},
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
            
            # Return both product data AND confidence
            product_data = parse_keepa_product(amazon_results[best_index])
            product_data['match_confidence'] = confidence  # Add confidence to return data
            return product_data
        else:
            reason = match_data.get('reason', 'Low confidence match')
            print(f"   ❌ AI confidence too low ({confidence}%)")
            print(f"      Reason: {reason}")
            return None
            
    except Exception as e:
        print(f"   ⚠️  AI validation error: {e}")
        # Fallback: use first result with default confidence
        print(f"   ⚠️  Falling back to first result")
        if amazon_results:
            product_data = parse_keepa_product(amazon_results[0])
            product_data['match_confidence'] = 70  # Default fallback confidence
            return product_data
        return None
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
    """
    Parse Keepa product data with enhanced metadata for AI validation
    """
    
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
    
    # Extract enhanced metadata for AI validation
    package_dimensions = None
    if keepa_data.get('packageHeight') and keepa_data.get('packageLength') and keepa_data.get('packageWidth'):
        # Keepa returns in hundredths of inches
        package_dimensions = {
            'height': keepa_data['packageHeight'] / 100.0,
            'length': keepa_data['packageLength'] / 100.0,
            'width': keepa_data['packageWidth'] / 100.0
        }
    
    package_weight = None
    if keepa_data.get('packageWeight'):
        # Keepa returns in grams, convert to pounds
        package_weight = keepa_data['packageWeight'] / 453.592
    
    item_count = keepa_data.get('itemCount', 1)  # Number of items in pack
    
    return {
        'asin': keepa_data.get('asin'),
        'title': keepa_data.get('title'),
        'amazon_price': amazon_price,
        'amazon_url': f"https://www.amazon.com/dp/{keepa_data.get('asin')}" if keepa_data.get('asin') else None,
        'fba_fees': fba_fees,
        'sales_rank': sales_rank,
        'category': category,
        'offer_count': offer_count,
        'price_history': price_history,
        # Enhanced metadata for AI validation
        'package_dimensions': package_dimensions,
        'package_weight': package_weight,
        'item_count': item_count
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
    Main matching function with smart update scheduling and ASIN override support
    
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
    
    # Get products smartly with new priority queue
    if test_mode:
        all_products = utils.get_all_products()
        products = all_products[:5]
        print("🔧 DEBUG MODE: Testing with first 5 products (any status)")
    else:
        # Priority 1: ASIN Override (highest priority - human provided)
        asin_override_products = utils.get_products_by_status('ASIN Override')
        
        # Priority 2: New products (never matched)
        new_products = utils.get_products_by_status('New')
        
        # Priority 3: Matched products that need re-checking
        cutoff_date = datetime.now() - timedelta(days=max_days_old)
        cutoff_str = cutoff_date.strftime('%Y-%m-%dT%H:%M:%S.000Z')
        
        # Get matched products older than cutoff
        matched_old = utils.get_airtable_table('Products').all(
            formula=f"AND({{Status}} = 'Matched', OR({{Last Updated}} = BLANK(), IS_BEFORE({{Last Updated}}, '{cutoff_str}')))"
        )
        
        # Sort matched products by: low confidence first, then oldest
        # This ensures we re-check uncertain matches before confident ones
        matched_old_sorted = sorted(
            matched_old,
            key=lambda p: (
                p['fields'].get('Match Confidence Score', 100),  # Lower confidence first
                p['fields'].get('Last Updated', '1970-01-01')   # Then oldest
            )
        )
        
        # Combine: ASIN Override → New → Matched (low confidence + old)
        products = asin_override_products + new_products + matched_old_sorted
        
        print(f"📋 Priority queue:")
        print(f"   🔧 ASIN Override: {len(asin_override_products)}")
        print(f"   🆕 New products: {len(new_products)}")
        print(f"   📦 Matched (>{max_days_old}d old, needs re-check): {len(matched_old_sorted)}")
        
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
    
    for i, record in enumerate(products, 1):
        fields = record['fields']
        
        # Get product details
        original_name = fields.get('Product Name')
        cleaned_name = fields.get('Cleaned Product Name')
        search_name = cleaned_name or original_name
        brand = fields.get('Brand')
        costco_price = fields.get('Costco Price', 0)
        costco_sku = fields.get('Costco SKU')
        status = fields.get('Status')
        
        print(f"[{i}/{len(products)}] {search_name[:60]}...")
        
        # Handle ASIN Override status differently
        if status == 'ASIN Override':
            override_asin = fields.get('Override Amazon ASIN')
            
            if not override_asin:
                print(f"   ⚠️  ASIN Override status but no Override ASIN provided")
                continue
            
            print(f"   🔧 ASIN Override: Using {override_asin}")
            
            # Fetch specific ASIN from Keepa
            amazon_data = fetch_product_by_asin(override_asin)
            
            if amazon_data and amazon_data['amazon_price']:
                # ASIN Override assumes 100% confidence (human verified)
                match_confidence = 100
                
                profit_data = calculate_profit(
                    costco_price,
                    amazon_data['amazon_price'],
                    amazon_data['fba_fees']
                )
                
                # Check if price changed for history
                old_amazon_price = fields.get('Amazon Price')
                price_changed = old_amazon_price and old_amazon_price != amazon_data['amazon_price']
                
                # Update product with Override results
                utils.update_product(record['id'], {
                    'Amazon ASIN': amazon_data['asin'],
                    'Amazon Price': amazon_data['amazon_price'],
                    'Amazon URL': amazon_data['amazon_url'],
                    'FBA Fees': amazon_data['fba_fees'],
                    'Sales Rank': amazon_data['sales_rank'],
                    'Category': amazon_data['category'],
                    'Match Confidence Score': match_confidence,  # Renamed field
                    'Status': 'Matched',  # Change from ASIN Override to Matched
                    'Override Amazon ASIN': '',  # Clear the override field
                    'Last Updated': datetime.now().strftime('%Y-%m-%dT%H:%M:%S.000Z')
                })
                
                # Create Price History if price changed
                if price_changed:
                    try:
                        utils.create_price_history(
                            costco_sku=costco_sku,
                            product_name=original_name,
                            old_price=old_amazon_price,
                            new_price=amazon_data['amazon_price'],
                            product_record_id=record['id']
                        )
                        print(f"   📊 Price changed: ${old_amazon_price:.2f} → ${amazon_data['amazon_price']:.2f}")
                    except Exception as e:
                        print(f"   ⚠️  Could not create price history: {e}")
                
                profit_str = f"${profit_data['profit']:.2f}" if profit_data['profit'] else "N/A"
                roi_str = f"{profit_data['roi']:.1f}%" if profit_data['roi'] else "N/A"
                
                print(f"   ✅ ASIN Override processed successfully")
                print(f"   💰 Amazon: ${amazon_data['amazon_price']:.2f} | Profit: {profit_str} | ROI: {roi_str}")
                print(f"   📊 Match Confidence: 100% (human verified)")
                
                matched_count += 1
            else:
                utils.update_product(record['id'], {
                    'Status': 'Not Found',
                    'Override Amazon ASIN': '',  # Clear invalid override
                    'Last Updated': datetime.now().strftime('%Y-%m-%dT%H:%M:%S.000Z')
                })
                print(f"   ❌ Override ASIN not found on Amazon")
                not_found_count += 1
            
        else:
            # Normal matching flow (New or Matched status)
            amazon_results = search_amazon_product(search_name, brand)
            
            # Use AI to validate and pick best match
            if amazon_results:
                amazon_data = validate_best_amazon_match({
                    'name': original_name,
                    'cleaned_name': cleaned_name,
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
                
                # Get Match Confidence from AI validation
                match_confidence = amazon_data.get('match_confidence', 85)  # Use AI confidence
                
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
                    'Match Confidence Score': match_confidence,  # AI match quality score
                    'Status': 'Matched',  # All successful matches are now 'Matched'
                    'Last Updated': datetime.now().strftime('%Y-%m-%dT%H:%M:%S.000Z')
                })
                
                # Create Price History record if price changed
                if price_changed:
                    try:
                        utils.create_price_history(
                            costco_sku=costco_sku,
                            product_name=original_name,
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
                print(f"   📊 Match Confidence: {match_confidence}%")
                
                matched_count += 1
                
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
    print(f"❌ Not found: {not_found_count}")
    print("=" * 70)
    print()
    print("💡 TIP: Check Opportunity Score in Airtable for deal quality!")

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
