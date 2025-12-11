from pyairtable import Table
import config

def get_airtable_table(table_name):
    """Get Airtable table connection"""
    return Table(
        config.AIRTABLE_API_KEY,
        config.AIRTABLE_BASE_ID,
        table_name
    )

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
