"""
Extract orders data from CSV
Includes data quality validation
"""

import pandas as pd
import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from src.utils.logger import setup_logger
import yaml

logger = setup_logger('extract_orders')


class OrdersExtractor:
    """Extract and validate orders data"""
    
    def __init__(self, config_path='config/file_paths.yaml'):
        """Initialize with file paths from config"""
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        self.file_path = config['data_paths']['orders']
        logger.info(f"OrdersExtractor initialized with path: {self.file_path}")
    
    def extract(self):
        """
        Extract orders data from CSV
        
        Returns:
            DataFrame: Orders data with parsed dates
        """
        try:
            logger.info("Starting orders extraction...")
            
            # Read CSV
            df = pd.read_csv(self.file_path)
            logger.info(f"✓ Loaded {len(df):,} rows from {self.file_path}")
            
            # Parse date columns
            date_columns = [
                'order_purchase_timestamp',
                'order_approved_at',
                'order_delivered_carrier_date',
                'order_delivered_customer_date',
                'order_estimated_delivery_date'
            ]
            
            for col in date_columns:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                    logger.info(f"✓ Parsed date column: {col}")
            
            # Validate data quality
            self._validate_data(df)
            
            logger.info(f"✓ Orders extraction complete: {len(df):,} rows")
            return df
            
        except Exception as e:
            logger.error(f"✗ Orders extraction failed: {e}")
            raise
    
    def _validate_data(self, df):
        """Validate data quality"""
        # Check for duplicates
        duplicates = df['order_id'].duplicated().sum()
        if duplicates > 0:
            logger.warning(f"⚠ Found {duplicates} duplicate order_ids")
        else:
            logger.info("✓ No duplicate order_ids")
        
        # Check for missing critical fields
        missing_order_id = df['order_id'].isnull().sum()
        missing_customer_id = df['customer_id'].isnull().sum()
        
        if missing_order_id > 0 or missing_customer_id > 0:
            logger.error(f"✗ Missing critical fields: order_id={missing_order_id}, customer_id={missing_customer_id}")
            raise ValueError("Critical fields cannot be null")
        
        logger.info("✓ Data quality validation passed")


# Test the extractor
if __name__ == "__main__":
    extractor = OrdersExtractor()
    df_orders = extractor.extract()
    print("\nFirst 5 rows:")
    print(df_orders.head())
    print(f"\nShape: {df_orders.shape}")
    print(f"\nData types:\n{df_orders.dtypes}")
