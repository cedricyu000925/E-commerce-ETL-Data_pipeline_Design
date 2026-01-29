"""
Extract products data from CSV
Includes product dimension calculations and category validation
"""

import pandas as pd
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from src.utils.logger import setup_logger
import yaml

logger = setup_logger('extract_products')


class ProductsExtractor:
    """Extract and validate products data"""
    
    def __init__(self, config_path='config/file_paths.yaml'):
        """Initialize with file paths from config"""
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        self.file_path = config['data_paths']['products']
        logger.info(f"ProductsExtractor initialized with path: {self.file_path}")
    
    def extract(self):
        """
        Extract products data from CSV with calculated fields
        
        Returns:
            DataFrame: Products data with volume calculations
        """
        try:
            logger.info("Starting products extraction...")
            
            # Read CSV
            df = pd.read_csv(self.file_path)
            logger.info(f"✓ Loaded {len(df):,} rows from {self.file_path}")
            
            # Calculate product volume (length × width × height)
            dimension_cols = ['product_length_cm', 'product_height_cm', 'product_width_cm']
            if all(col in df.columns for col in dimension_cols):
                df['product_volume_cm3'] = (
                    df['product_length_cm'] * 
                    df['product_height_cm'] * 
                    df['product_width_cm']
                ).round(2)
                logger.info("✓ Calculated product_volume_cm3 column")
            
            # Create has_photos flag
            if 'product_photos_qty' in df.columns:
                df['has_photos'] = df['product_photos_qty'] > 0
                logger.info("✓ Created has_photos flag column")
            
            # Basic info logging
            logger.info(f"  - Unique products: {df['product_id'].nunique():,}")
            if 'product_category_name' in df.columns:
                logger.info(f"  - Unique categories: {df['product_category_name'].nunique()}")
            
            # Validate data quality
            self._validate_data(df)
            
            logger.info(f"✓ Products extraction complete: {len(df):,} rows")
            return df
            
        except Exception as e:
            logger.error(f"✗ Products extraction failed: {e}")
            raise
    
    def _validate_data(self, df):
        """Validate products data quality"""
        
        # Check for duplicate product_id (should be unique)
        duplicate_products = df['product_id'].duplicated().sum()
        if duplicate_products > 0:
            logger.error(f"✗ Found {duplicate_products} duplicate product_ids - products should be unique!")
            # Don't raise error, but flag for investigation
        else:
            logger.info("✓ No duplicate product_ids")
        
        # Check for missing product_id (critical)
        missing_product_id = df['product_id'].isnull().sum()
        if missing_product_id > 0:
            logger.error(f"✗ Missing product_id: {missing_product_id} nulls")
            raise ValueError("product_id cannot be null")
        
        # Check for missing category names
        if 'product_category_name' in df.columns:
            missing_categories = df['product_category_name'].isnull().sum()
            if missing_categories > 0:
                logger.warning(f"⚠ Missing product_category_name: {missing_categories} nulls ({missing_categories/len(df)*100:.2f}%)")
                logger.info("  → These will be labeled as 'Uncategorized' in Transform phase")
            else:
                logger.info("✓ All products have categories")
        
        # Check for invalid dimensions (negative or zero)
        dimension_cols = ['product_weight_g', 'product_length_cm', 'product_height_cm', 'product_width_cm']
        for col in dimension_cols:
            if col in df.columns:
                invalid_dims = ((df[col] <= 0) | (df[col].isnull())).sum()
                if invalid_dims > 0:
                    logger.warning(f"⚠ Invalid/missing {col}: {invalid_dims} records ({invalid_dims/len(df)*100:.2f}%)")
        
        # Log product dimension statistics
        if 'product_weight_g' in df.columns:
            logger.info(f"  - Weight stats: min={df['product_weight_g'].min():.0f}g, "
                       f"max={df['product_weight_g'].max():.0f}g, "
                       f"avg={df['product_weight_g'].mean():.0f}g")
        
        if 'product_volume_cm3' in df.columns:
            valid_volumes = df[df['product_volume_cm3'] > 0]
            logger.info(f"  - Volume stats: min={valid_volumes['product_volume_cm3'].min():.0f}cm³, "
                       f"max={valid_volumes['product_volume_cm3'].max():.0f}cm³, "
                       f"avg={valid_volumes['product_volume_cm3'].mean():.0f}cm³")
        
        # Log top categories
        if 'product_category_name' in df.columns:
            top_categories = df['product_category_name'].value_counts().head(5)
            logger.info(f"  - Top 5 categories: {', '.join([f'{cat}({count})' for cat, count in top_categories.items()])}")
        
        # Check photo statistics
        if 'product_photos_qty' in df.columns:
            products_with_photos = (df['product_photos_qty'] > 0).sum()
            logger.info(f"  - Products with photos: {products_with_photos:,} ({products_with_photos/len(df)*100:.1f}%)")
        
        logger.info("✓ Product data quality validation passed")


# Test the extractor
if __name__ == "__main__":
    extractor = ProductsExtractor()
    df_products = extractor.extract()
    
    print("\n" + "="*80)
    print("PRODUCTS EXTRACTION TEST")
    print("="*80)
    print(f"\nTotal rows: {len(df_products):,}")
    print(f"Columns: {list(df_products.columns)}")
    
    print("\nFirst 5 rows:")
    print(df_products.head())
    
    print("\nData types:")
    print(df_products.dtypes)
    
    print("\nMissing values:")
    print(df_products.isnull().sum())
    
    if 'product_category_name' in df_products.columns:
        print("\nTop 10 categories:")
        print(df_products['product_category_name'].value_counts().head(10))
    
    if 'product_volume_cm3' in df_products.columns:
        print(f"\nVolume statistics:")
        print(df_products['product_volume_cm3'].describe())
