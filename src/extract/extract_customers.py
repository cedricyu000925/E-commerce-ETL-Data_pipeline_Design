"""
Extract customers data from CSV
Includes geographic validation and deduplication checks
"""

import pandas as pd
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from src.utils.logger import setup_logger
import yaml

logger = setup_logger('extract_customers')


class CustomersExtractor:
    """Extract and validate customers data"""
    
    def __init__(self, config_path='config/file_paths.yaml'):
        """Initialize with file paths from config"""
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        self.file_path = config['data_paths']['customers']
        logger.info(f"CustomersExtractor initialized with path: {self.file_path}")
    
    def extract(self):
        """
        Extract customers data from CSV
        
        Returns:
            DataFrame: Customers data with quality checks
        """
        try:
            logger.info("Starting customers extraction...")
            
            # Read CSV
            df = pd.read_csv(self.file_path)
            logger.info(f"✓ Loaded {len(df):,} rows from {self.file_path}")
            
            # Basic info logging
            logger.info(f"  - Unique customer_ids: {df['customer_id'].nunique():,}")
            if 'customer_unique_id' in df.columns:
                logger.info(f"  - Unique customer_unique_ids: {df['customer_unique_id'].nunique():,}")
            if 'customer_state' in df.columns:
                logger.info(f"  - Unique states: {df['customer_state'].nunique()}")
            if 'customer_city' in df.columns:
                logger.info(f"  - Unique cities: {df['customer_city'].nunique():,}")
            
            # Validate data quality
            self._validate_data(df)
            
            logger.info(f"✓ Customers extraction complete: {len(df):,} rows")
            return df
            
        except Exception as e:
            logger.error(f"✗ Customers extraction failed: {e}")
            raise
    
    def _validate_data(self, df):
        """Validate customers data quality"""
        
        # Check for duplicate customer_id (should be unique per order)
        duplicate_customer_ids = df['customer_id'].duplicated().sum()
        if duplicate_customer_ids > 0:
            logger.warning(f"⚠ Found {duplicate_customer_ids} duplicate customer_ids (this is OK if same person ordered multiple times)")
        else:
            logger.info("✓ No duplicate customer_ids")
        
        # Check if customer_unique_id exists and has duplicates (expected)
        if 'customer_unique_id' in df.columns:
            unique_customers = df['customer_unique_id'].nunique()
            total_records = len(df)
            repeat_customers = total_records - unique_customers
            logger.info(f"  - Repeat customer records: {repeat_customers:,} ({repeat_customers/total_records*100:.1f}%)")
        
        # Check for missing critical fields
        missing_checks = {
            'customer_id': df['customer_id'].isnull().sum(),
            'customer_unique_id': df['customer_unique_id'].isnull().sum() if 'customer_unique_id' in df.columns else 0,
            'customer_state': df['customer_state'].isnull().sum() if 'customer_state' in df.columns else 0,
            'customer_city': df['customer_city'].isnull().sum() if 'customer_city' in df.columns else 0
        }
        
        critical_missing = False
        for field, missing_count in missing_checks.items():
            if missing_count > 0:
                if field in ['customer_id', 'customer_unique_id']:
                    logger.error(f"✗ Missing critical field {field}: {missing_count} nulls")
                    critical_missing = True
                else:
                    logger.warning(f"⚠ Missing {field}: {missing_count} nulls ({missing_count/len(df)*100:.2f}%)")
        
        if critical_missing:
            raise ValueError("Critical customer fields cannot be null")
        
        # Validate state codes (Brazilian states are 2 characters)
        if 'customer_state' in df.columns:
            invalid_states = df[df['customer_state'].str.len() != 2]
            if len(invalid_states) > 0:
                logger.warning(f"⚠ Found {len(invalid_states)} records with invalid state codes")
            else:
                logger.info("✓ All state codes are valid (2 characters)")
        
        # Log top states
        if 'customer_state' in df.columns:
            top_states = df['customer_state'].value_counts().head(5)
            logger.info(f"  - Top 5 states: {', '.join([f'{state}({count:,})' for state, count in top_states.items()])}")
        
        logger.info("✓ Customer data quality validation passed")


# Test the extractor
if __name__ == "__main__":
    extractor = CustomersExtractor()
    df_customers = extractor.extract()
    
    print("\n" + "="*80)
    print("CUSTOMERS EXTRACTION TEST")
    print("="*80)
    print(f"\nTotal rows: {len(df_customers):,}")
    print(f"Columns: {list(df_customers.columns)}")
    
    print("\nFirst 5 rows:")
    print(df_customers.head())
    
    print("\nData types:")
    print(df_customers.dtypes)
    
    print("\nMissing values:")
    print(df_customers.isnull().sum())
    
    if 'customer_state' in df_customers.columns:
        print("\nState distribution:")
        print(df_customers['customer_state'].value_counts().head(10))
