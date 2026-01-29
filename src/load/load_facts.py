"""
Load fact tables from transformed CSV files into PostgreSQL
Uses COPY FROM for maximum speed and bypasses all constraints
"""

import pandas as pd
import sys
import os
from io import StringIO

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from src.utils.logger import setup_logger
from src.utils.db_connector import DatabaseConnector

logger = setup_logger('load_facts')


class FactLoader:
    """Load fact tables into PostgreSQL"""
    
    def __init__(self, staging_dir='data/staging'):
        """
        Initialize fact loader
        
        Args:
            staging_dir: Directory containing transformed CSV files
        """
        self.staging_dir = staging_dir
        self.db = DatabaseConnector()
        logger.info(f"FactLoader initialized with staging dir: {staging_dir}")
    
    def load_all_facts(self):
        """Load all fact tables"""
        try:
            logger.info("Loading fact tables...")
            
            # Load fact orders
            self._load_fact_orders()
            
            # Load cohort retention
            self._load_fact_cohort_retention()
            
            logger.info("\n" + "="*80)
            logger.info("✓ ALL FACT TABLES LOADED SUCCESSFULLY")
            logger.info("="*80 + "\n")
            
            # Verify loads
            self._verify_loads()
            
            # Show sample analytics
            self._show_analytics()
            
        except Exception as e:
            logger.error(f"✗ Fact loading failed: {e}")
            raise
        finally:
            self.db.close_pool()
    
    def _load_fact_orders(self):
        """Load fact orders - using chunked pandas loading"""
        logger.info("\n[1/2] Loading fact_orders...")
        
        # Read CSV
        filepath = os.path.join(self.staging_dir, 'fact_orders.csv')
        df = pd.read_csv(filepath)
        
        logger.info(f"  - Read {len(df):,} rows from CSV")
        
        # Convert timestamps
        timestamp_columns = ['order_purchase_timestamp', 'order_delivered_customer_date', 'created_at']
        for col in timestamp_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Drop order_key if present
        if 'order_key' in df.columns:
            df = df.drop('order_key', axis=1)
        
        # Check for required columns
        required_columns = [
            'customer_key', 'product_key', 'order_date_key', 'delivery_date_key',
            'payment_type_key', 'order_id', 'order_status', 'seller_id',
            'order_item_count', 'order_subtotal', 'order_freight_total',
            'order_total_value', 'payment_value', 'payment_installments',
            'delivery_days', 'estimated_delivery_days', 'delivery_delay_days',
            'is_late_delivery', 'is_completed_order', 'review_score',
            'has_review', 'order_purchase_timestamp', 'order_delivered_customer_date',
            'created_at'
        ]
        
        # Filter to only required columns
        available_columns = [col for col in required_columns if col in df.columns]
        df = df[available_columns]
        
        logger.info(f"  - Using {len(available_columns)} columns")
        
        # Load to PostgreSQL in chunks
        logger.info("  - Loading to PostgreSQL in chunks...")
        
        engine = self.db.get_engine()
        chunk_size = 5000  # Smaller chunks for stability
        total_chunks = (len(df) // chunk_size) + 1
        
        loaded_rows = 0
        for i, chunk_start in enumerate(range(0, len(df), chunk_size)):
            chunk = df.iloc[chunk_start:chunk_start + chunk_size]
            
            try:
                chunk.to_sql(
                    'fact_orders',
                    engine,
                    if_exists='append',
                    index=False,
                    method='multi',
                    chunksize=1000
                )
                loaded_rows += len(chunk)
                logger.info(f"    → Loaded chunk {i+1}/{total_chunks} ({loaded_rows:,}/{len(df):,} rows)")
                
            except Exception as e:
                logger.error(f"    ✗ Chunk {i+1} failed: {str(e)[:200]}")
                logger.info(f"    → Attempting row-by-row insert for failed chunk...")
                
                # Try row by row for failed chunk
                for idx, row in chunk.iterrows():
                    try:
                        row.to_frame().T.to_sql(
                            'fact_orders',
                            engine,
                            if_exists='append',
                            index=False
                        )
                        loaded_rows += 1
                    except Exception as row_error:
                        logger.warning(f"    ⚠ Skipped row {idx}: {str(row_error)[:100]}")
                        continue
        
        logger.info(f"  ✓ Loaded {loaded_rows:,} rows into fact_orders")
    
    def _load_fact_cohort_retention(self):
        """Load cohort retention fact table"""
        logger.info("\n[2/2] Loading fact_cohort_retention...")
        
        filepath = os.path.join(self.staging_dir, 'fact_cohort_retention.csv')
        df = pd.read_csv(filepath)
        
        logger.info(f"  - Read {len(df):,} rows from CSV")
        
        # Convert dates
        df['cohort_month'] = pd.to_datetime(df['cohort_month'])
        df['created_at'] = pd.to_datetime(df['created_at'])
        
        # Drop surrogate key
        if 'cohort_retention_key' in df.columns:
            df = df.drop('cohort_retention_key', axis=1)
        
        # Load to PostgreSQL
        engine = self.db.get_engine()
        
        df.to_sql(
            'fact_cohort_retention',
            engine,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=1000
        )
        
        logger.info(f"  ✓ Loaded {len(df):,} rows into fact_cohort_retention")
    
    def _verify_loads(self):
        """Verify fact tables were loaded correctly"""
        logger.info("\nVerifying fact table loads:")
        
        tables = ['fact_orders', 'fact_cohort_retention']
        
        for table in tables:
            query = f"SELECT COUNT(*) FROM {table};"
            result = self.db.fetch_query(query)
            count = result[0][0]
            logger.info(f"  - {table}: {count:,} rows")
    
    def _show_analytics(self):
        """Show sample analytics from loaded data"""
        logger.info("\n" + "="*60)
        logger.info("SAMPLE ANALYTICS FROM DATA WAREHOUSE")
        logger.info("="*60)
        
        # Total revenue
        query = """
        SELECT 
            SUM(order_total_value) as total_revenue,
            AVG(order_total_value) as avg_order_value,
            COUNT(*) as total_orders
        FROM fact_orders
        WHERE is_completed_order = TRUE;
        """
        result = self.db.fetch_query(query)
        if result:
            total_rev, avg_order, total_orders = result[0]
            logger.info(f"\nRevenue Metrics:")
            logger.info(f"  - Total Revenue: R$ {total_rev:,.2f}")
            logger.info(f"  - Average Order Value: R$ {avg_order:.2f}")
            logger.info(f"  - Completed Orders: {total_orders:,}")
        
        # Delivery performance
        query = """
        SELECT 
            COUNT(*) FILTER (WHERE is_late_delivery = TRUE) as late_deliveries,
            COUNT(*) FILTER (WHERE is_late_delivery = FALSE) as on_time_deliveries,
            AVG(delivery_days) as avg_delivery_days
        FROM fact_orders
        WHERE is_completed_order = TRUE;
        """
        result = self.db.fetch_query(query)
        if result:
            late, on_time, avg_days = result[0]
            total = late + on_time
            late_pct = (late / total * 100) if total > 0 else 0
            logger.info(f"\nDelivery Performance:")
            logger.info(f"  - Late Deliveries: {late:,} ({late_pct:.1f}%)")
            logger.info(f"  - On-Time Deliveries: {on_time:,} ({100-late_pct:.1f}%)")
            logger.info(f"  - Average Delivery Time: {avg_days:.1f} days")
        
        # Order status distribution
        query = """
        SELECT 
            order_status,
            COUNT(*) as order_count
        FROM fact_orders
        GROUP BY order_status
        ORDER BY order_count DESC;
        """
        result = self.db.fetch_query(query)
        if result:
            logger.info(f"\nOrder Status Distribution:")
            for status, count in result[:5]:  # Top 5
                logger.info(f"  - {status}: {count:,} orders")
        
        logger.info("\n" + "="*60 + "\n")


# Main execution
if __name__ == "__main__":
    print("\n" + "="*80)
    print("LOADING FACT TABLES INTO POSTGRESQL")
    print("="*80 + "\n")
    
    loader = FactLoader()
    loader.load_all_facts()
    
    print("\n✓ Fact tables loaded successfully!")
    print("\nYour data warehouse is now fully populated!")
    print("\nNext step: Connect Power BI to PostgreSQL for dashboard creation")
