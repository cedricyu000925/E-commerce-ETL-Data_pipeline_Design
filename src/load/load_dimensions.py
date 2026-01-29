"""
Load dimension tables from transformed CSV files into PostgreSQL
Uses pandas to_sql for efficient bulk loading
"""

import pandas as pd
import sys
import os
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from src.utils.logger import setup_logger
from src.utils.db_connector import DatabaseConnector

logger = setup_logger('load_dimensions')


class DimensionLoader:
    """Load dimension tables into PostgreSQL"""
    
    def __init__(self, staging_dir='data/staging'):
        """
        Initialize dimension loader
        
        Args:
            staging_dir: Directory containing transformed CSV files
        """
        self.staging_dir = staging_dir
        self.db = DatabaseConnector()
        logger.info(f"DimensionLoader initialized with staging dir: {staging_dir}")
    
    def load_all_dimensions(self):
        """Load all dimension tables"""
        try:
            logger.info("Loading dimension tables...")
            
            # Load in dependency order
            self._load_dim_date()
            self._load_dim_products()
            self._load_dim_payment_type()
            self._load_dim_customers()
            
            logger.info("\n" + "="*80)
            logger.info("✓ ALL DIMENSION TABLES LOADED SUCCESSFULLY")
            logger.info("="*80 + "\n")
            
            # Verify row counts
            self._verify_loads()
            
        except Exception as e:
            logger.error(f"✗ Dimension loading failed: {e}")
            raise
        finally:
            self.db.close_pool()
    
    def _load_dim_date(self):
        """Load date dimension"""
        logger.info("\n[1/4] Loading dim_date...")
        
        # Read CSV
        filepath = os.path.join(self.staging_dir, 'dim_date.csv')
        df = pd.read_csv(filepath)
        
        logger.info(f"  - Read {len(df):,} rows from CSV")
        
        # Convert date column
        df['full_date'] = pd.to_datetime(df['full_date'])
        df['created_at'] = pd.to_datetime(df['created_at'])
        
        # Define expected columns (15 columns total - NO date_str)
        expected_columns = [
            'date_key', 'full_date', 'year', 'quarter', 'month', 
            'month_name', 'week', 'day_of_month', 'day_of_week', 
            'day_name', 'is_weekend', 'is_holiday', 'fiscal_year', 
            'fiscal_quarter', 'created_at'
        ]
        
        # Remove any extra columns (like date_str)
        df = df[expected_columns]
        
        # Load to PostgreSQL using pandas to_sql
        engine = self.db.get_engine()
        
        df.to_sql(
            'dim_date',
            engine,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=1000
        )
        
        logger.info(f"  ✓ Loaded {len(df):,} rows into dim_date")
    
    def _load_dim_products(self):
        """Load product dimension"""
        logger.info("\n[2/4] Loading dim_products...")
        
        filepath = os.path.join(self.staging_dir, 'dim_products.csv')
        df = pd.read_csv(filepath)
        
        logger.info(f"  - Read {len(df):,} rows from CSV")
        
        # Convert timestamps
        df['created_at'] = pd.to_datetime(df['created_at'])
        
        # KEEP product_key from CSV
        
        # Load to PostgreSQL
        engine = self.db.get_engine()
        
        df.to_sql(
            'dim_products',
            engine,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=1000
        )
        
        logger.info(f"  ✓ Loaded {len(df):,} rows into dim_products")
    
    def _load_dim_payment_type(self):
        """Load payment type dimension"""
        logger.info("\n[3/4] Loading dim_payment_type...")
        
        filepath = os.path.join(self.staging_dir, 'dim_payment_type.csv')
        df = pd.read_csv(filepath)
        
        logger.info(f"  - Read {len(df)} rows from CSV")
        
        # Convert timestamps
        df['created_at'] = pd.to_datetime(df['created_at'])
        
        # KEEP payment_type_key from CSV
        
        # Load to PostgreSQL
        engine = self.db.get_engine()
        
        df.to_sql(
            'dim_payment_type',
            engine,
            if_exists='append',
            index=False,
            method='multi'
        )
        
        logger.info(f"  ✓ Loaded {len(df)} rows into dim_payment_type")
    
    def _load_dim_customers(self):
        """Load customer dimension"""
        logger.info("\n[4/4] Loading dim_customers...")
        
        filepath = os.path.join(self.staging_dir, 'dim_customers.csv')
        df = pd.read_csv(filepath)
        
        logger.info(f"  - Read {len(df):,} rows from CSV")
        
        # Convert timestamps
        date_columns = ['first_order_date', 'last_order_date', 'created_at', 'updated_at']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col])
        
        # KEEP customer_key from CSV
        
        # Load to PostgreSQL
        engine = self.db.get_engine()
        
        df.to_sql(
            'dim_customers',
            engine,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=5000
        )
        
        logger.info(f"  ✓ Loaded {len(df):,} rows into dim_customers")
    
    def _verify_loads(self):
        """Verify dimension tables were loaded correctly"""
        logger.info("\nVerifying dimension table loads:")
        
        tables = ['dim_date', 'dim_products', 'dim_payment_type', 'dim_customers']
        
        for table in tables:
            query = f"SELECT COUNT(*) FROM {table};"
            result = self.db.fetch_query(query)
            count = result[0][0]
            logger.info(f"  - {table}: {count:,} rows")


# Main execution
if __name__ == "__main__":
    print("\n" + "="*80)
    print("LOADING DIMENSION TABLES INTO POSTGRESQL")
    print("="*80 + "\n")
    
    loader = DimensionLoader()
    loader.load_all_dimensions()
    
    print("\n✓ Dimension tables loaded successfully!")
    print("\nNext step: Load fact tables")
