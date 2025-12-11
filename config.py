import os

# Airtable
AIRTABLE_API_KEY = os.getenv('AIRTABLE_API_KEY')
AIRTABLE_BASE_ID = os.getenv('AIRTABLE_BASE_ID')

# OpenAI
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')

# Keepa
KEEPA_API_KEY = os.getenv('KEEPA_API_KEY')

# Settings
COSTCO_DEALS_URL = "https://www.costco.com/warehouse-savings.html"
MAX_PRODUCTS_TO_SCRAPE = 5  # Start small for testing!
