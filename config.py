import os

# Airtable
AIRTABLE_API_KEY = os.getenv('AIRTABLE_API_KEY')
AIRTABLE_BASE_ID = os.getenv('AIRTABLE_BASE_ID')

# OpenAI (keep for potential future use)
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')

# Keepa
KEEPA_API_KEY = os.getenv('KEEPA_API_KEY')

# SendGrid (for future email sending)
SENDGRID_API_KEY = os.getenv('SENDGRID_API_KEY')
SENDGRID_FROM_EMAIL = os.getenv('SENDGRID_FROM_EMAIL', 'alerts@getdealscout.com')

# OLD SETTINGS (no longer needed with API approach)
# COSTCO_DEALS_URL = "https://www.costco.com/warehouse-savings.html"
# MAX_PRODUCTS_TO_SCRAPE = 5
