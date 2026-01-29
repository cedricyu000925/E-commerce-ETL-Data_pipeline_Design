"""
Transform Customers into Dimension with CLV and Segmentation
Uses Polars for performance, Pandas for complex logic
"""

import pandas as pd
import polars as pl
import sys
import os
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from src.utils.logger import setup_logger
from src.extract.extract_customers import CustomersExtractor
from src.extract.extract_orders import OrdersExtractor
from src.extract.extract_order_items import OrderItemsExtractor
import yaml

logger = setup_logger('transform_dim_customers')


class CustomerDimensionBuilder:
    """Build Customer Dimension with CLV and segmentation"""
    
    def __init__(self, config_path='config/business_rules.yaml'):
        """Initialize with business rules from config"""
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        self.segmentation_rules = config['customer_segmentation']
        self.regions = config['regions']
        self.clv_params = config['clv_calculation']
        
        logger.info("CustomerDimensionBuilder initialized")
        logger.info(f"  - Segmentation rules: {self.segmentation_rules}")
    
    def build(self):
        """
        Build customer dimension with CLV and segmentation
        
        Returns:
            DataFrame: Customer dimension (Pandas)
        """
        try:
            logger.info("Building customer dimension...")
            
            # Step 1: Extract base customer data
            customers_ext = CustomersExtractor()
            df_customers = customers_ext.extract()
            logger.info(f"✓ Loaded {len(df_customers):,} customer records")
            
            # Step 2: Extract orders for metrics
            orders_ext = OrdersExtractor()
            df_orders = orders_ext.extract()
            logger.info(f"✓ Loaded {len(df_orders):,} orders")
            
            # Step 3: Extract order items for revenue
            items_ext = OrderItemsExtractor()
            df_items = items_ext.extract()
            logger.info(f"✓ Loaded {len(df_items):,} order items")
            
            # Step 4: Calculate order-level revenue using Polars (FAST!)
            logger.info("Calculating order-level revenue with Polars...")
            
            # Convert to Polars for aggregation performance
            df_items_pl = pl.from_pandas(df_items)
            
            # Aggregate to order level
            order_revenue_pl = df_items_pl.group_by('order_id').agg([
                pl.col('item_total').sum().alias('order_total')
            ])
            
            # Convert back to Pandas for join
            order_revenue = order_revenue_pl.to_pandas()
            logger.info(f"✓ Calculated revenue for {len(order_revenue):,} orders")
            
            # Step 5: Join orders with revenue
            df_orders = df_orders.merge(
                order_revenue,
                on='order_id',
                how='left'
            )
            
            # Fill missing revenue with 0 (canceled orders, etc.)
            df_orders['order_total'] = df_orders['order_total'].fillna(0)
            
            # Step 6: Calculate customer metrics using Polars (PERFORMANCE!)
            logger.info("Calculating customer metrics...")
            
            # Convert to Polars
            df_orders_pl = pl.from_pandas(df_orders[[
                'customer_id', 'order_id', 'order_purchase_timestamp',
                'order_delivered_customer_date', 'order_total', 'order_status'
            ]])
            
            # Calculate customer aggregations
            customer_metrics_pl = df_orders_pl.group_by('customer_id').agg([
                pl.col('order_id').count().alias('total_orders'),
                pl.col('order_total').sum().alias('total_spent'),
                pl.col('order_total').mean().alias('avg_order_value'),
                pl.col('order_purchase_timestamp').min().alias('first_order_date'),
                pl.col('order_purchase_timestamp').max().alias('last_order_date'),
                (pl.col('order_status') == 'delivered').sum().alias('delivered_orders')
            ])
            
            # Convert back to Pandas
            customer_metrics = customer_metrics_pl.to_pandas()
            
            logger.info(f"✓ Calculated metrics for {len(customer_metrics):,} customers")
            
            # Step 7: Calculate days between first and last order
            customer_metrics['days_as_customer'] = (
                customer_metrics['last_order_date'] - customer_metrics['first_order_date']
            ).dt.days
            
            # Avoid division by zero
            customer_metrics['days_as_customer'] = customer_metrics['days_as_customer'].apply(
                lambda x: max(x, 1)  # Minimum 1 day
            )
            
            # Step 8: Calculate CLV (Customer Lifetime Value)
            logger.info("Calculating Customer Lifetime Value (CLV)...")
            
            estimated_lifespan = self.clv_params['estimated_lifespan_days']
            
            # Purchase frequency (annualized)
            customer_metrics['purchase_frequency_annual'] = (
                customer_metrics['total_orders'] / customer_metrics['days_as_customer'] * 365
            )
            
            # CLV = Average Order Value × Purchase Frequency × Customer Lifespan
            customer_metrics['lifetime_value'] = (
                customer_metrics['avg_order_value'] * 
                customer_metrics['purchase_frequency_annual'] * 
                (estimated_lifespan / 365)
            ).round(2)
            
            logger.info(f"✓ Calculated CLV for all customers")
            logger.info(f"  - Average CLV: R$ {customer_metrics['lifetime_value'].mean():.2f}")
            logger.info(f"  - Median CLV: R$ {customer_metrics['lifetime_value'].median():.2f}")
            logger.info(f"  - Max CLV: R$ {customer_metrics['lifetime_value'].max():.2f}")
            
            # Step 9: Join metrics with base customer data
            df = df_customers.merge(
                customer_metrics,
                on='customer_id',
                how='left'
            )
            
            # Handle customers with no orders (should be rare)
            no_orders = df['total_orders'].isnull()
            if no_orders.any():
                logger.warning(f"⚠ {no_orders.sum()} customers with no orders")
                df.loc[no_orders, 'total_orders'] = 0
                df.loc[no_orders, 'total_spent'] = 0
                df.loc[no_orders, 'lifetime_value'] = 0
            
            # Step 10: Add customer region
            df['customer_region'] = df['customer_state'].apply(self._get_region)
            
            logger.info("✓ Mapped customers to regions")
            region_dist = df['customer_region'].value_counts()
            for region, count in region_dist.items():
                logger.info(f"  - {region}: {count:,} customers")
            
            # Step 11: Add customer segmentation
            df['customer_segment'] = df.apply(self._segment_customer, axis=1)
            
            logger.info("✓ Applied customer segmentation")
            segment_dist = df['customer_segment'].value_counts()
            for segment, count in segment_dist.items():
                logger.info(f"  - {segment}: {count:,} customers")
            
            # Step 12: Create surrogate key
            df = df.reset_index(drop=True)
            df.insert(0, 'customer_key', df.index + 1)
            
            # Step 13: Add timestamps
            df['created_at'] = pd.Timestamp.now()
            df['updated_at'] = pd.Timestamp.now()
            
            # Step 14: Select and order final columns
            final_columns = [
                'customer_key',
                'customer_id',
                'customer_unique_id',
                'customer_city',
                'customer_state',
                'customer_region',
                'customer_segment',
                'first_order_date',
                'last_order_date',
                'total_orders',
                'delivered_orders',
                'total_spent',
                'avg_order_value',
                'lifetime_value',
                'days_as_customer',
                'purchase_frequency_annual',
                'created_at',
                'updated_at'
            ]
            
            # Keep only columns that exist
            final_columns = [col for col in final_columns if col in df.columns]
            df = df[final_columns]
            
            logger.info(f"✓ Customer dimension built successfully: {len(df):,} rows")
            logger.info(f"  - Columns: {len(df.columns)}")
            
            return df
            
        except Exception as e:
            logger.error(f"✗ Customer dimension build failed: {e}")
            raise
    
    def _get_region(self, state):
        """Map state to region"""
        if pd.isna(state):
            return 'Unknown'
        
        for region, states in self.regions.items():
            if state in states:
                return region
        return 'Other'
    
    def _segment_customer(self, row):
        """
        Segment customer based on business rules
        
        Segmentation:
        - New: 1 order
        - Returning: 2-5 orders
        - VIP: >5 orders OR lifetime_value > threshold
        """
        total_orders = row['total_orders']
        lifetime_value = row['lifetime_value'] if pd.notna(row['lifetime_value']) else 0
        
        vip_threshold = self.segmentation_rules['vip_customer_min_value']
        vip_orders = self.segmentation_rules['vip_customer_min_orders']
        returning_max = self.segmentation_rules['returning_customer_max_orders']
        
        if total_orders == 0:
            return 'Inactive'
        elif total_orders >= vip_orders or lifetime_value >= vip_threshold:
            return 'VIP'
        elif total_orders <= 1:
            return 'New'
        elif total_orders <= returning_max:
            return 'Returning'
        else:
            return 'Loyal'


# Test the builder
if __name__ == "__main__":
    builder = CustomerDimensionBuilder()
    df_customers = builder.build()
    
    print("\n" + "="*80)
    print("CUSTOMER DIMENSION TEST")
    print("="*80)
    
    print(f"\nTotal customers: {len(df_customers):,}")
    print(f"\nColumns: {list(df_customers.columns)}")
    
    print("\nFirst 10 rows:")
    print(df_customers.head(10))
    
    print("\nData types:")
    print(df_customers.dtypes)
    
    print("\nCustomer segment distribution:")
    print(df_customers['customer_segment'].value_counts())
    
    print("\nRegion distribution:")
    print(df_customers['customer_region'].value_counts())
    
    print("\nCLV statistics:")
    print(df_customers['lifetime_value'].describe())
    
    print("\nTop 10 customers by CLV:")
    top_clv = df_customers.nlargest(10, 'lifetime_value')
    print(top_clv[['customer_key', 'customer_segment', 'total_orders', 'total_spent', 'lifetime_value']])
