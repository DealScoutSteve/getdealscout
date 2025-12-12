from pyairtable import Table
import config

def get_airtable_table(table_name):
    """Get Airtable table connection"""
    return Table(
        config.AIRTABLE_API_KEY,
        config.AIRTABLE_BASE_ID,
        table_name
    )

# NEW FUNCTIONS for costco_to_airtable.py

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

def get_all_products():
    """
    Get all products from Airtable
    
    Returns:
        List of all product records
    """
    products_table = get_airtable_table('Products')
    return products_table.all()

def create_price_history(costco_sku, product_name, old_price, new_price, product_record_id):
    """
    Create a Price History record when Amazon price changes
    
    Args:
        costco_sku: Costco SKU
        product_name: Product name
        old_price: Previous Amazon price
        new_price: Current Amazon price
        product_record_id: Record ID to link back to Products table
    """
    from datetime import datetime
    
    price_history_table = get_airtable_table('Price History')
    
    return price_history_table.create({
        'Costco SKU': costco_sku,
        'Product Name': product_name,
        'Old Price': old_price,
        'New Price': new_price,
        'Date': datetime.now().strftime('%Y-%m-%dT%H:%M:%S.000Z'),
        'Product': [product_record_id]  # Link to Products table
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
        'Amazon ASIN': opportunity.get('amazon_asin', ''),
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
    
    # Airtable formula to find old records
    formula = f"IS_BEFORE({{Date Found}}, '{cutoff_date.isoformat()}')"
    
    old_records = products_table.all(formula=formula)
    
    for record in old_records:
        products_table.delete(record['id'])
        print(f"🗑️ Deleted old record: {record['fields'].get('Product Name')}")
    
    print(f"Cleaned up {len(old_records)} old records")
