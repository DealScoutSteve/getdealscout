from pyairtable import Table
from datetime import datetime
import config

def get_airtable_table(table_name):
    """Get Airtable table connection"""
    return Table(
        config.AIRTABLE_API_KEY,
        config.AIRTABLE_BASE_ID,
        table_name
    )

# PRODUCT FUNCTIONS

def create_product(product_data):
    """
    Create a new product in Airtable
    
    Args:
        product_data: Dict with product fields
        
    Returns:
        Created record
    """
    products_table = get_airtable_table('Products')
    return products_table.create(product_data)

def find_product_by_costco_sku(costco_sku):
    """
    Find a product by Costco SKU
    
    Args:
        costco_sku: Costco item number
        
    Returns:
        Record if found, None otherwise
    """
    products_table = get_airtable_table('Products')
    formula = f"{{Costco SKU}} = '{costco_sku}'"
    results = products_table.all(formula=formula)
    return results[0] if results else None

def find_product_by_amazon_asin(asin):
    """
    Find a product by Amazon ASIN
    
    Args:
        asin: Amazon ASIN
        
    Returns:
        Record if found, None otherwise
    """
    products_table = get_airtable_table('Products')
    formula = f"{{Amazon ASIN}} = '{asin}'"
    results = products_table.all(formula=formula)
    return results[0] if results else None

def update_product(record_id, updates):
    """
    Update a product record
    
    Args:
        record_id: Airtable record ID
        updates: Dict with fields to update
        
    Returns:
        Updated record
    """
    products_table = get_airtable_table('Products')
    return products_table.update(record_id, updates)

def get_products_by_status(status):
    """
    Get products by status
    
    Args:
        status: Status value (e.g., 'New', 'Matched', 'Profitable')
        
    Returns:
        List of matching records
    """
    products_table = get_airtable_table('Products')
    formula = f"{{Status}} = '{status}'"
    return products_table.all(formula=formula)

# PRICE HISTORY FUNCTIONS

def log_price_history(product_id, costco_sku, product_name, old_price, new_price):
    """
    Log a price change to Price History table
    
    Args:
        product_id: Airtable Products record ID
        costco_sku: Costco SKU
        product_name: Product name
        old_price: Previous price
        new_price: New price
        
    Returns:
        Created history record
    """
    history_table = get_airtable_table('Price History')
    
    return history_table.create({
        'Product': [product_id],  # Link to Products table
        'Costco SKU': costco_sku,
        'Product Name': product_name,
        'Old Price': old_price,
        'New Price': new_price,
        'Price Change': new_price - old_price if old_price and new_price else 0,
        'Date': datetime.now().isoformat()
    })

# ORIGINAL FUNCTIONS (keeping for backwards compatibility)

def save_opportunity(opportunity):
    """Save opportunity to Airtable Products table"""
    products_table = get_airtable_table('Products')
    
    record = products_table.create({
        'Product Name': opportunity['name'],
        'Costco SKU': opportunity['costco_sku'],
        'Costco Price': opportunity['costco_price'],
        'Costco URL': opportunity['costco_url'],
        'Amazon SKU': opportunity.get('amazon_asin', ''),
        'Amazon Price': opportunity.get('amazon_price', 0),
        'Amazon URL': opportunity.get('amazon_url', ''),
        'FBA Fees': opportunity.get('fba_fees', 0),
        'Sales Rank': opportunity.get('sales_rank', 0),
        'Category': opportunity.get('category', ''),
        'In Stock': opportunity.get('in_stock', True)
    })
    
    print(f"✅ Saved: {opportunity['name']}")
    return record

def clear_old_products(days=7):
    """Delete products older than X days"""
    from datetime import datetime, timedelta
    
    products_table = get_airtable_table('Products')
    cutoff_date = datetime.now() - timedelta(days=days)
    
    formula = f"IS_BEFORE({{Date Found}}, '{cutoff_date.isoformat()}')"
    
    old_records = products_table.all(formula=formula)
    
    for record in old_records:
        products_table.delete(record['id'])
        print(f"🗑️ Deleted old record: {record['fields'].get('Product Name')}")
    
    print(f"Cleaned up {len(old_records)} old records")

def clean_product_names_batch(products, batch_size=50):
    """
    Clean multiple product names in a single API call
    
    Uses structured output for reliability
    Costs: ~$0.10 per 50 products with GPT-4o-mini
    """
    
    # Group products into batches
    for i in range(0, len(products), batch_size):
        batch = products[i:i + batch_size]
        
        # Build prompt with all products at once
        product_list = "\n".join([
            f"{j+1}. {p['fields']['Product Name']}"
            for j, p in enumerate(batch)
        ])
        
        prompt = f"""Clean these Costco product names for Amazon search.

RULES:
1. Remove Costco-specific pack sizes (5-pack, 8-count, etc.)
2. Remove detailed specifications after commas
3. Keep brand + core product name + key differentiator
4. Max 60 characters
5. Focus on what makes it findable on Amazon

Examples:
❌ "Crest Pro Health Advanced Toothpaste, 5.9 oz, 5-pack"
✅ "Crest Pro Health Advanced Toothpaste"

❌ "Dyson V15 Detect Total Clean Extra Cordless Stick Vacuum"
✅ "Dyson V15 Detect Cordless Vacuum"

❌ "MacBook Air Laptop (13-inch) - Apple M4 chip, Built for Apple Intelligence, 10-core CPU, 8-core GPU, 16GB Memory, 256GB SSD Storage"
✅ "MacBook Air 13 inch M4 16GB 256GB"

Products to clean:
{product_list}

Return ONLY a JSON array with cleaned names in the same order:
["cleaned name 1", "cleaned name 2", ...]
"""
        
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are an expert at optimizing product names for e-commerce search. Return ONLY valid JSON."},
                {"role": "user", "content": prompt}
            ],
            temperature=0
        )
        
        # Parse response
        cleaned_names = json.loads(response.choices[0].message.content)
        
        # Write back to Airtable
        for j, cleaned_name in enumerate(cleaned_names):
            record = batch[j]
            utils.update_product(record['id'], {
                'Cleaned Product Name': cleaned_name
            })
            print(f"   ✅ {record['fields']['Product Name'][:50]}...")
            print(f"      → {cleaned_name}")
