"""
Extract order items data from CSV
Includes revenue calculations
"""

import pandas as pd
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from src.utils.logger import setup_logger
import yaml

logger = setup_logger('extract_order_items')


class OrderItemsExtractor:
    """Extract and validate order items data"""
    
    def __init__(self, config_path='config/file_paths.yaml'):
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        self.file_path = config['data_paths']['order_items']
    
    def extract(self):
        """Extract order items with calculated metrics"""
        try:
            logger.info("Starting order items extraction...")
            
            # Read CSV
            df = pd.read_csv(self.file_path)
            logger.info(f"✓ Loaded {len(df):,} rows")
            
            # Calculate item total
            df['item_total'] = df['price'] + df['freight_value']
            logger.info("✓ Calculated item_total column")
            
            # Validate
            self._validate_data(df)
            
            logger.info(f"✓ Order items extraction complete: {len(df):,} rows")
            return df
            
        except Exception as e:
            logger.error(f"✗ Order items extraction failed: {e}")
            raise
    
    def _validate_data(self, df):
        """Validate order items data"""
        # Check for negative prices
        negative_prices = (df['price'] < 0).sum()
        if negative_prices > 0:
            logger.warning(f"⚠ Found {negative_prices} negative prices")
        
        # Check for zero prices
        zero_prices = (df['price'] == 0).sum()
        if zero_prices > 0:
            logger.warning(f"⚠ Found {zero_prices} zero-price items")
        
        # Check missing values
        missing_order_id = df['order_id'].isnull().sum()
        missing_product_id = df['product_id'].isnull().sum()
        
        if missing_order_id > 0 or missing_product_id > 0:
            logger.error(f"✗ Missing critical IDs")
            raise ValueError("order_id and product_id cannot be null")
        
        logger.info("✓ Data quality validation passed")


if __name__ == "__main__":
    extractor = OrderItemsExtractor()
    df = extractor.extract()
    print(f"\nExtracted {len(df):,} order items")
    print(f"Total revenue: R$ {df['item_total'].sum():,.2f}")
    print(f"Average item price: R$ {df['price'].mean():.2f}")
