"""
Build FACT_ORDERS - Central Fact Table
Uses both Polars (for aggregations) and Pandas (for complex joins)
"""

import pandas as pd
import polars as pl
import sys
import os
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from src.utils.logger import setup_logger
from src.extract.extract_orders import OrdersExtractor
from src.extract.extract_order_items import OrderItemsExtractor
from src.extract.extract_payments import PaymentsExtractor
from src.extract.extract_reviews import ReviewsExtractor

logger = setup_logger('transform_fact_orders')


class FactOrdersBuilder:
    """Build FACT_ORDERS with all metrics and foreign keys"""
    
    def __init__(self):
        """Initialize fact orders builder"""
        logger.info("FactOrdersBuilder initialized")
    
    def build(self, dim_customers, dim_products, dim_payment_type, dim_date):
        """
        Build fact orders table by joining all sources
        
        Args:
            dim_customers: Customer dimension DataFrame
            dim_products: Product dimension DataFrame
            dim_payment_type: Payment type dimension DataFrame
            dim_date: Date dimension DataFrame
        
        Returns:
            DataFrame: Fact orders table
        """
        try:
            logger.info("Building FACT_ORDERS...")
            
            # Step 1: Extract source data
            logger.info("Step 1/8: Extracting source data...")
            orders_ext = OrdersExtractor()
            df_orders = orders_ext.extract()
            logger.info(f"  ✓ Orders: {len(df_orders):,} rows")
            
            items_ext = OrderItemsExtractor()
            df_items = items_ext.extract()
            logger.info(f"  ✓ Order items: {len(df_items):,} rows")
            
            payments_ext = PaymentsExtractor()
            df_payments = payments_ext.extract()
            logger.info(f"  ✓ Payments: {len(df_payments):,} rows")
            
            reviews_ext = ReviewsExtractor()
            df_reviews = reviews_ext.extract()
            logger.info(f"  ✓ Reviews: {len(df_reviews):,} rows")
            
            # Step 2: Aggregate order items to order level using Polars
            logger.info("Step 2/8: Aggregating order items to order level with Polars...")
            
            df_items_pl = pl.from_pandas(df_items)
            
            order_items_agg_pl = df_items_pl.group_by('order_id').agg([
                pl.col('order_item_id').count().alias('order_item_count'),
                pl.col('price').sum().alias('order_subtotal'),
                pl.col('freight_value').sum().alias('order_freight_total'),
                pl.col('item_total').sum().alias('order_total_value'),
                # Get first product_id (we're at order grain, not line item)
                pl.col('product_id').first().alias('primary_product_id'),
                # Get first seller_id
                pl.col('seller_id').first().alias('seller_id')
            ])
            
            order_items_agg = order_items_agg_pl.to_pandas()
            logger.info(f"  ✓ Aggregated {len(order_items_agg):,} orders")
            
            # Step 3: Aggregate payments to order level using Polars
            logger.info("Step 3/8: Aggregating payments to order level with Polars...")
            
            df_payments_pl = pl.from_pandas(df_payments)
            
            order_payments_agg_pl = df_payments_pl.group_by('order_id').agg([
                pl.col('payment_value').sum().alias('payment_value'),
                # Get primary payment type (most common for this order)
                pl.col('payment_type').first().alias('payment_type'),
                # Max installments
                pl.col('payment_installments').max().alias('payment_installments')
            ])
            
            order_payments_agg = order_payments_agg_pl.to_pandas()
            logger.info(f"  ✓ Aggregated {len(order_payments_agg):,} payment records")
            
            # Step 4: Join orders with aggregated items
            logger.info("Step 4/8: Joining orders with order items...")
            
            df_fact = df_orders.merge(
                order_items_agg,
                on='order_id',
                how='left'
            )
            
            # Fill missing values for orders without items (canceled, etc.)
            df_fact['order_item_count'] = df_fact['order_item_count'].fillna(0)
            df_fact['order_subtotal'] = df_fact['order_subtotal'].fillna(0)
            df_fact['order_freight_total'] = df_fact['order_freight_total'].fillna(0)
            df_fact['order_total_value'] = df_fact['order_total_value'].fillna(0)
            
            logger.info(f"  ✓ After items join: {len(df_fact):,} rows")
            
            # Step 5: Join with payments
            logger.info("Step 5/8: Joining with payments...")
            
            df_fact = df_fact.merge(
                order_payments_agg,
                on='order_id',
                how='left'
            )
            
            # Fill missing payment values
            df_fact['payment_value'] = df_fact['payment_value'].fillna(0)
            df_fact['payment_installments'] = df_fact['payment_installments'].fillna(1)
            df_fact['payment_type'] = df_fact['payment_type'].fillna('not_defined')
            
            logger.info(f"  ✓ After payments join: {len(df_fact):,} rows")
            
            # Step 6: Join with reviews (optional - not all orders have reviews)
            logger.info("Step 6/8: Joining with reviews...")
            
            df_reviews_subset = df_reviews[['order_id', 'review_score']].copy()
            
            df_fact = df_fact.merge(
                df_reviews_subset,
                on='order_id',
                how='left'
            )
            
            # Create has_review flag
            df_fact['has_review'] = df_fact['review_score'].notna()
            
            reviews_count = df_fact['has_review'].sum()
            logger.info(f"  ✓ {reviews_count:,} orders have reviews ({reviews_count/len(df_fact)*100:.1f}%)")
            
            # Step 7: Calculate delivery metrics
            logger.info("Step 7/8: Calculating delivery metrics...")
            
            # Delivery days (from purchase to actual delivery)
            df_fact['delivery_days'] = (
                df_fact['order_delivered_customer_date'] - 
                df_fact['order_purchase_timestamp']
            ).dt.days
            
            # Estimated delivery days (from purchase to estimated delivery)
            df_fact['estimated_delivery_days'] = (
                df_fact['order_estimated_delivery_date'] - 
                df_fact['order_purchase_timestamp']
            ).dt.days
            
            # Delivery delay (negative = early, positive = late)
            df_fact['delivery_delay_days'] = (
                df_fact['order_delivered_customer_date'] - 
                df_fact['order_estimated_delivery_date']
            ).dt.days
            
            # Boolean flags
            df_fact['is_late_delivery'] = df_fact['delivery_delay_days'] > 0
            df_fact['is_completed_order'] = df_fact['order_status'] == 'delivered'
            
            # Fill NaN for non-delivered orders
            df_fact['delivery_days'] = df_fact['delivery_days'].fillna(0)
            df_fact['delivery_delay_days'] = df_fact['delivery_delay_days'].fillna(0)
            df_fact['is_late_delivery'] = df_fact['is_late_delivery'].fillna(False)
            
            logger.info(f"  ✓ Calculated delivery metrics")
            logger.info(f"    - Completed orders: {df_fact['is_completed_order'].sum():,}")
            logger.info(f"    - Late deliveries: {df_fact['is_late_delivery'].sum():,}")
            logger.info(f"    - Average delivery time: {df_fact[df_fact['delivery_days'] > 0]['delivery_days'].mean():.1f} days")
            
            # Step 8: Add foreign keys to dimension tables
            logger.info("Step 8/8: Adding foreign keys to dimensions...")
            
            # Create lookup dictionaries for fast FK mapping
            customer_lookup = dict(zip(
                dim_customers['customer_id'],
                dim_customers['customer_key']
            ))
            
            product_lookup = dict(zip(
                dim_products['product_id'],
                dim_products['product_key']
            ))
            
            payment_type_lookup = dict(zip(
                dim_payment_type['payment_type'],
                dim_payment_type['payment_type_key']
            ))
            
            # Create date lookup (date_key is YYYYMMDD integer)
            dim_date['date_str'] = dim_date['full_date'].dt.strftime('%Y%m%d').astype(int)
            date_lookup = dict(zip(
                dim_date['full_date'].dt.date,
                dim_date['date_key']
            ))
            
            # Map customer_key
            df_fact['customer_key'] = df_fact['customer_id'].map(customer_lookup)
            missing_customers = df_fact['customer_key'].isnull().sum()
            if missing_customers > 0:
                logger.warning(f"  ⚠ {missing_customers} orders with unmapped customers")
            
            # Map product_key (for primary product)
            df_fact['product_key'] = df_fact['primary_product_id'].map(product_lookup)
            missing_products = df_fact['product_key'].isnull().sum()
            if missing_products > 0:
                logger.warning(f"  ⚠ {missing_products} orders with unmapped products")
            
            # Map payment_type_key
            df_fact['payment_type_key'] = df_fact['payment_type'].map(payment_type_lookup)
            missing_payment_types = df_fact['payment_type_key'].isnull().sum()
            if missing_payment_types > 0:
                logger.warning(f"  ⚠ {missing_payment_types} orders with unmapped payment types")
            
            # Map order_date_key
            df_fact['order_date'] = df_fact['order_purchase_timestamp'].dt.date
            df_fact['order_date_key'] = df_fact['order_date'].map(date_lookup)
            
            # Map delivery_date_key (might be null for non-delivered orders)
            df_fact['delivery_date'] = df_fact['order_delivered_customer_date'].dt.date
            df_fact['delivery_date_key'] = df_fact['delivery_date'].map(date_lookup)
            
            logger.info(f"  ✓ Mapped all foreign keys")
            
            # Create surrogate key (order_key)
            df_fact = df_fact.reset_index(drop=True)
            df_fact.insert(0, 'order_key', df_fact.index + 1)
            
            # Add created_at timestamp
            df_fact['created_at'] = pd.Timestamp.now()
            
            # Step 9: Select and order final columns
            logger.info("Finalizing fact table structure...")
            
            final_columns = [
                # Surrogate key
                'order_key',
                
                # Foreign keys to dimensions
                'customer_key',
                'product_key',
                'order_date_key',
                'delivery_date_key',
                'payment_type_key',
                
                # Business key
                'order_id',
                
                # Dimensions (non-FK attributes)
                'order_status',
                'seller_id',
                
                # Numeric measures
                'order_item_count',
                'order_subtotal',
                'order_freight_total',
                'order_total_value',
                'payment_value',
                'payment_installments',
                
                # Delivery metrics
                'delivery_days',
                'estimated_delivery_days',
                'delivery_delay_days',
                'is_late_delivery',
                'is_completed_order',
                
                # Review data
                'review_score',
                'has_review',
                
                # Timestamps
                'order_purchase_timestamp',
                'order_delivered_customer_date',
                'created_at'
            ]
            
            # Keep only columns that exist
            final_columns = [col for col in final_columns if col in df_fact.columns]
            df_fact = df_fact[final_columns]
            
            logger.info(f"✓ FACT_ORDERS built successfully: {len(df_fact):,} rows, {len(df_fact.columns)} columns")
            
            # Log some business metrics
            logger.info("\n" + "="*60)
            logger.info("FACT_ORDERS BUSINESS METRICS:")
            logger.info(f"  - Total orders: {len(df_fact):,}")
            logger.info(f"  - Total revenue: R$ {df_fact['order_total_value'].sum():,.2f}")
            logger.info(f"  - Average order value: R$ {df_fact['order_total_value'].mean():.2f}")
            logger.info(f"  - Completed orders: {df_fact['is_completed_order'].sum():,} ({df_fact['is_completed_order'].sum()/len(df_fact)*100:.1f}%)")
            logger.info(f"  - Late deliveries: {df_fact['is_late_delivery'].sum():,} ({df_fact['is_late_delivery'].sum()/len(df_fact)*100:.1f}%)")
            logger.info(f"  - Orders with reviews: {df_fact['has_review'].sum():,} ({df_fact['has_review'].sum()/len(df_fact)*100:.1f}%)")
            logger.info(f"  - Average review score: {df_fact['review_score'].mean():.2f}/5.0")
            logger.info("="*60 + "\n")
            
            return df_fact
            
        except Exception as e:
            logger.error(f"✗ FACT_ORDERS build failed: {e}")
            import traceback
            traceback.print_exc()
            raise


# Test the builder
if __name__ == "__main__":
    from src.transform.dim_customers import CustomerDimensionBuilder
    from src.transform.dim_products import ProductDimensionBuilder
    from src.transform.dim_payment_type import PaymentTypeDimensionBuilder
    from src.transform.dim_date import DateDimensionBuilder
    
    print("\n" + "="*80)
    print("FACT_ORDERS BUILD TEST")
    print("="*80)
    
    # Build dimensions first
    print("\nBuilding required dimensions...")
    
    dim_customers_builder = CustomerDimensionBuilder()
    dim_customers = dim_customers_builder.build()
    print(f"✓ Customers dimension: {len(dim_customers):,} rows")
    
    dim_products_builder = ProductDimensionBuilder()
    dim_products = dim_products_builder.build()
    print(f"✓ Products dimension: {len(dim_products):,} rows")
    
    dim_payment_type_builder = PaymentTypeDimensionBuilder()
    dim_payment_type = dim_payment_type_builder.build()
    print(f"✓ Payment type dimension: {len(dim_payment_type)} rows")
    
    dim_date_builder = DateDimensionBuilder()
    dim_date = dim_date_builder.build()
    print(f"✓ Date dimension: {len(dim_date):,} rows")
    
    # Build fact table
    print("\nBuilding FACT_ORDERS...")
    fact_builder = FactOrdersBuilder()
    df_fact = fact_builder.build(dim_customers, dim_products, dim_payment_type, dim_date)
    
    print("\n" + "="*80)
    print("FACT_ORDERS TEST RESULTS")
    print("="*80)
    
    print(f"\nTotal rows: {len(df_fact):,}")
    print(f"Columns: {len(df_fact.columns)}")
    
    print("\nColumn list:")
    print(df_fact.columns.tolist())
    
    print("\nFirst 5 rows:")
    print(df_fact.head())
    
    print("\nData types:")
    print(df_fact.dtypes)
    
    print("\nOrder status distribution:")
    print(df_fact['order_status'].value_counts())
    
    print("\nRevenue statistics:")
    print(df_fact['order_total_value'].describe())
    
    print("\nForeign key coverage:")
    print(f"  - customer_key nulls: {df_fact['customer_key'].isnull().sum()}")
    print(f"  - product_key nulls: {df_fact['product_key'].isnull().sum()}")
    print(f"  - payment_type_key nulls: {df_fact['payment_type_key'].isnull().sum()}")
    print(f"  - order_date_key nulls: {df_fact['order_date_key'].isnull().sum()}")
