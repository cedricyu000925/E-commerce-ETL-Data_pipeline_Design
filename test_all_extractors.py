"""
Test all extractors to ensure they work correctly
"""

import sys
sys.path.append('.')

from src.extract.extract_orders import OrdersExtractor
from src.extract.extract_order_items import OrderItemsExtractor
from src.extract.extract_customers import CustomersExtractor
from src.extract.extract_products import ProductsExtractor
from src.extract.extract_payments import PaymentsExtractor
from src.extract.extract_reviews import ReviewsExtractor

print("="*80)
print("TESTING ALL EXTRACTORS")
print("="*80)

try:
    # Test 1: Orders
    print("\n[1/6] Testing Orders Extractor...")
    orders_ext = OrdersExtractor()
    df_orders = orders_ext.extract()
    print(f"✓ Orders: {len(df_orders):,} rows extracted\n")
    
    # Test 2: Order Items
    print("[2/6] Testing Order Items Extractor...")
    items_ext = OrderItemsExtractor()
    df_items = items_ext.extract()
    print(f"✓ Order Items: {len(df_items):,} rows extracted\n")
    
    # Test 3: Customers
    print("[3/6] Testing Customers Extractor...")
    customers_ext = CustomersExtractor()
    df_customers = customers_ext.extract()
    print(f"✓ Customers: {len(df_customers):,} rows extracted\n")
    
    # Test 4: Products
    print("[4/6] Testing Products Extractor...")
    products_ext = ProductsExtractor()
    df_products = products_ext.extract()
    print(f"✓ Products: {len(df_products):,} rows extracted\n")
    
    # Test 5: Payments
    print("[5/6] Testing Payments Extractor...")
    payments_ext = PaymentsExtractor()
    df_payments = payments_ext.extract()
    print(f"✓ Payments: {len(df_payments):,} rows extracted\n")
    
    # Test 6: Reviews
    print("[6/6] Testing Reviews Extractor...")
    reviews_ext = ReviewsExtractor()
    df_reviews = reviews_ext.extract()
    print(f"✓ Reviews: {len(df_reviews):,} rows extracted\n")
    
    print("="*80)
    print("✓ ALL EXTRACTORS PASSED!")
    print("="*80)
    
    # Summary
    print("\nEXTRACTION SUMMARY:")
    print(f"  Orders:       {len(df_orders):>8,} rows")
    print(f"  Order Items:  {len(df_items):>8,} rows")
    print(f"  Customers:    {len(df_customers):>8,} rows")
    print(f"  Products:     {len(df_products):>8,} rows")
    print(f"  Payments:     {len(df_payments):>8,} rows")
    print(f"  Reviews:      {len(df_reviews):>8,} rows")
    print(f"  {'─'*30}")
    print(f"  Total:        {len(df_orders)+len(df_items)+len(df_customers)+len(df_products)+len(df_payments)+len(df_reviews):>8,} rows")
    
except Exception as e:
    print(f"\n✗ EXTRACTOR TEST FAILED: {e}")
    import traceback
    traceback.print_exc()
