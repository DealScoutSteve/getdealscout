#!/usr/bin/env python3
"""
Clean Costco product names for better Amazon matching using AI

This script:
1. Gets all products without cleaned names
2. Batch processes them through GPT-4o-mini
3. Writes cleaned names to Airtable
"""

import json
from openai import OpenAI
import config
import utils

client = OpenAI(api_key=config.OPENAI_API_KEY)

def clean_product_names_batch(products, batch_size=50):
    """
    Clean multiple product names in a single API call
    """
    
    total_cleaned = 0
    
    for i in range(0, len(products), batch_size):
        batch = products[i:i + batch_size]
        
        print(f"\n🧹 Cleaning batch {i//batch_size + 1} ({len(batch)} products)...")
        
        # Build product list
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

Return ONLY a JSON array with cleaned names in the same order. No markdown, no explanation.
["cleaned name 1", "cleaned name 2", ...]
"""
        
        try:
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "You are an expert at optimizing product names for e-commerce search. Return ONLY valid JSON."},
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
            
            cleaned_names = json.loads(ai_response.strip())
            
            # Write back to Airtable
            for j, cleaned_name in enumerate(cleaned_names):
                record = batch[j]
                utils.update_product(record['id'], {
                    'Cleaned Product Name': cleaned_name
                })
                print(f"   ✅ {record['fields']['Product Name'][:50]}...")
                print(f"      → {cleaned_name}")
                total_cleaned += 1
                
        except Exception as e:
            print(f"   ❌ Error cleaning batch: {e}")
            print(f"   Raw response: {ai_response[:200] if 'ai_response' in locals() else 'No response'}")
    
    return total_cleaned

def main():
    print("=" * 70)
    print("🧹 CLEAN PRODUCT NAMES FOR AMAZON MATCHING")
    print("=" * 70)
    print()
    
    # Get all products without cleaned names
    all_products = utils.get_all_products()
    
    products_to_clean = [
        p for p in all_products
        if not p['fields'].get('Cleaned Product Name')
    ]
    
    if not products_to_clean:
        print("✅ All products already have cleaned names!")
        return
    
    print(f"📦 Found {len(products_to_clean)} products to clean")
    print()
    
    total = clean_product_names_batch(products_to_clean, batch_size=50)
    
    print()
    print("=" * 70)
    print(f"✅ CLEANING COMPLETE! Processed {total} products")
    print(f"💰 Estimated cost: ${total * 0.002:.2f}")
    print("=" * 70)

if __name__ == "__main__":
    main()
