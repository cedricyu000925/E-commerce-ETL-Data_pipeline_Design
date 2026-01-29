"""
Build FACT_COHORT_RETENTION - Pre-calculated cohort analysis
Shows retention rates by customer acquisition cohort
"""

import pandas as pd
import polars as pl
import sys
import os
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from src.utils.logger import setup_logger
from src.extract.extract_orders import OrdersExtractor

logger = setup_logger('transform_fact_cohort')


class CohortRetentionBuilder:
    """Build cohort retention fact table"""
    
    def __init__(self):
        """Initialize cohort retention builder"""
        logger.info("CohortRetentionBuilder initialized")
    
    def build(self, dim_customers):
        """
        Build cohort retention table
        
        Args:
            dim_customers: Customer dimension with first_order_date
        
        Returns:
            DataFrame: Cohort retention fact table
        """
        try:
            logger.info("Building FACT_COHORT_RETENTION...")
            
            # Step 1: Extract orders
            logger.info("Step 1/5: Extracting orders...")
            orders_ext = OrdersExtractor()
            df_orders = orders_ext.extract()
            logger.info(f"  ✓ Loaded {len(df_orders):,} orders")
            
            # Step 2: Get customer first order dates from dimension
            logger.info("Step 2/5: Getting customer cohorts...")
            
            df_customers_cohort = dim_customers[['customer_id', 'first_order_date']].copy()
            
            # Create cohort month (YYYY-MM format)
            df_customers_cohort['cohort_month'] = pd.to_datetime(
                df_customers_cohort['first_order_date']
            ).dt.to_period('M')
            
            logger.info(f"  ✓ Identified {df_customers_cohort['cohort_month'].nunique()} cohorts")
            
            # Step 3: Prepare orders data
            logger.info("Step 3/5: Preparing order data for cohort analysis...")
            
            df_orders_cohort = df_orders[['customer_id', 'order_purchase_timestamp']].copy()
            df_orders_cohort['order_month'] = pd.to_datetime(
                df_orders_cohort['order_purchase_timestamp']
            ).dt.to_period('M')
            
            # Join orders with customer cohorts
            df_analysis = df_orders_cohort.merge(
                df_customers_cohort,
                on='customer_id',
                how='inner'
            )
            
            logger.info(f"  ✓ Prepared {len(df_analysis):,} order records for analysis")
            
            # Step 4: Calculate months since first purchase
            logger.info("Step 4/5: Calculating retention by cohort...")
            
            # Calculate months difference
            df_analysis['months_since_first_purchase'] = (
                (df_analysis['order_month'] - df_analysis['cohort_month']).apply(lambda x: x.n)
            )
            
            # Convert periods back to dates for grouping
            df_analysis['cohort_month_date'] = df_analysis['cohort_month'].dt.to_timestamp()
            
            # Use Polars for fast aggregation
            df_analysis_pl = pl.from_pandas(df_analysis[[
                'cohort_month_date', 'months_since_first_purchase', 'customer_id'
            ]])
            
            # Group by cohort and months since first purchase
            cohort_data_pl = df_analysis_pl.group_by([
                'cohort_month_date', 'months_since_first_purchase'
            ]).agg([
                pl.col('customer_id').n_unique().alias('retained_customers')
            ]).sort(['cohort_month_date', 'months_since_first_purchase'])
            
            cohort_data = cohort_data_pl.to_pandas()
            
            logger.info(f"  ✓ Calculated retention for {len(cohort_data):,} cohort-month combinations")
            
            # Step 5: Calculate cohort sizes and retention rates
            logger.info("Step 5/5: Calculating cohort sizes and retention rates...")
            
            # Get cohort sizes (month 0 = acquisition month)
            cohort_sizes = cohort_data[
                cohort_data['months_since_first_purchase'] == 0
            ][['cohort_month_date', 'retained_customers']].copy()
            cohort_sizes.columns = ['cohort_month_date', 'cohort_size']
            
            # Merge cohort sizes
            cohort_data = cohort_data.merge(
                cohort_sizes,
                on='cohort_month_date',
                how='left'
            )
            
            # Calculate retention rate
            cohort_data['retention_rate'] = (
                cohort_data['retained_customers'] / cohort_data['cohort_size'] * 100
            ).round(2)
            
            # Rename for final output
            cohort_data = cohort_data.rename(columns={
                'cohort_month_date': 'cohort_month'
            })
            
            # Create surrogate key
            cohort_data = cohort_data.reset_index(drop=True)
            cohort_data.insert(0, 'cohort_retention_key', cohort_data.index + 1)
            
            # Add created_at
            cohort_data['created_at'] = pd.Timestamp.now()
            
            # Reorder columns
            final_columns = [
                'cohort_retention_key',
                'cohort_month',
                'months_since_first_purchase',
                'cohort_size',
                'retained_customers',
                'retention_rate',
                'created_at'
            ]
            
            cohort_data = cohort_data[final_columns]
            
            logger.info(f"✓ FACT_COHORT_RETENTION built successfully: {len(cohort_data):,} rows")
            
            # Log insights
            logger.info("\n" + "="*60)
            logger.info("COHORT RETENTION INSIGHTS:")
            logger.info(f"  - Total cohorts: {cohort_data['cohort_month'].nunique()}")
            logger.info(f"  - Average cohort size: {cohort_data[cohort_data['months_since_first_purchase']==0]['cohort_size'].mean():.0f} customers")
            
            # Month 1 retention (repeat purchase rate)
            month_1_retention = cohort_data[
                cohort_data['months_since_first_purchase'] == 1
            ]['retention_rate'].mean()
            logger.info(f"  - Average Month 1 retention: {month_1_retention:.1f}%")
            
            # Month 3 retention
            month_3_retention = cohort_data[
                cohort_data['months_since_first_purchase'] == 3
            ]['retention_rate'].mean()
            if pd.notna(month_3_retention):
                logger.info(f"  - Average Month 3 retention: {month_3_retention:.1f}%")
            
            logger.info("="*60 + "\n")
            
            return cohort_data
            
        except Exception as e:
            logger.error(f"✗ FACT_COHORT_RETENTION build failed: {e}")
            import traceback
            traceback.print_exc()
            raise


# Test the builder
if __name__ == "__main__":
    from src.transform.dim_customers import CustomerDimensionBuilder
    
    print("\n" + "="*80)
    print("FACT_COHORT_RETENTION BUILD TEST")
    print("="*80)
    
    # Build customer dimension first
    print("\nBuilding customer dimension...")
    dim_customers_builder = CustomerDimensionBuilder()
    dim_customers = dim_customers_builder.build()
    print(f"✓ Customer dimension: {len(dim_customers):,} rows")
    
    # Build cohort retention
    print("\nBuilding FACT_COHORT_RETENTION...")
    cohort_builder = CohortRetentionBuilder()
    df_cohort = cohort_builder.build(dim_customers)
    
    print("\n" + "="*80)
    print("FACT_COHORT_RETENTION TEST RESULTS")
    print("="*80)
    
    print(f"\nTotal rows: {len(df_cohort):,}")
    print(f"Columns: {list(df_cohort.columns)}")
    
    print("\nFirst 10 rows:")
    print(df_cohort.head(10))
    
    print("\nSample cohort (first cohort, all months):")
    first_cohort = df_cohort['cohort_month'].min()
    sample = df_cohort[df_cohort['cohort_month'] == first_cohort]
    print(sample)
    
    print("\nRetention rate statistics:")
    print(df_cohort['retention_rate'].describe())
    
    print("\nMonth-over-month retention:")
    avg_retention = df_cohort.groupby('months_since_first_purchase')['retention_rate'].mean()
    print(avg_retention.head(12))
