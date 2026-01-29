"""
Extract payments data from CSV
Includes payment method validation and aggregation checks
"""

import pandas as pd
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from src.utils.logger import setup_logger
import yaml

logger = setup_logger('extract_payments')


class PaymentsExtractor:
    """Extract and validate payments data"""
    
    def __init__(self, config_path='config/file_paths.yaml'):
        """Initialize with file paths from config"""
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        self.file_path = config['data_paths']['payments']
        logger.info(f"PaymentsExtractor initialized with path: {self.file_path}")
    
    def extract(self):
        """
        Extract payments data from CSV
        
        Returns:
            DataFrame: Payments data with validation
        """
        try:
            logger.info("Starting payments extraction...")
            
            # Read CSV
            df = pd.read_csv(self.file_path)
            logger.info(f"✓ Loaded {len(df):,} rows from {self.file_path}")
            
            # Basic info logging
            if 'order_id' in df.columns:
                unique_orders = df['order_id'].nunique()
                logger.info(f"  - Orders with payments: {unique_orders:,}")
                
                # Check for multiple payments per order
                payments_per_order = df.groupby('order_id').size()
                multi_payment_orders = (payments_per_order > 1).sum()
                logger.info(f"  - Orders with multiple payments: {multi_payment_orders:,} ({multi_payment_orders/unique_orders*100:.1f}%)")
            
            if 'payment_type' in df.columns:
                logger.info(f"  - Unique payment types: {df['payment_type'].nunique()}")
                logger.info(f"  - Payment types: {', '.join(df['payment_type'].unique())}")
            
            if 'payment_value' in df.columns:
                logger.info(f"  - Total payment value: R$ {df['payment_value'].sum():,.2f}")
                logger.info(f"  - Average payment: R$ {df['payment_value'].mean():.2f}")
            
            # Validate data quality
            self._validate_data(df)
            
            logger.info(f"✓ Payments extraction complete: {len(df):,} rows")
            return df
            
        except Exception as e:
            logger.error(f"✗ Payments extraction failed: {e}")
            raise
    
    def _validate_data(self, df):
        """Validate payments data quality"""
        
        # Check for missing critical fields
        missing_order_id = df['order_id'].isnull().sum()
        if missing_order_id > 0:
            logger.error(f"✗ Missing order_id: {missing_order_id} nulls")
            raise ValueError("order_id cannot be null in payments")
        else:
            logger.info("✓ No missing order_ids")
        
        # Check payment type
        if 'payment_type' in df.columns:
            missing_payment_type = df['payment_type'].isnull().sum()
            if missing_payment_type > 0:
                logger.warning(f"⚠ Missing payment_type: {missing_payment_type} nulls")
            
            # Log payment type distribution
            payment_dist = df['payment_type'].value_counts()
            logger.info("  - Payment type distribution:")
            for ptype, count in payment_dist.items():
                percentage = (count / len(df)) * 100
                logger.info(f"    → {ptype}: {count:,} ({percentage:.1f}%)")
        
        # Check payment values
        if 'payment_value' in df.columns:
            negative_payments = (df['payment_value'] < 0).sum()
            zero_payments = (df['payment_value'] == 0).sum()
            missing_payments = df['payment_value'].isnull().sum()
            
            if negative_payments > 0:
                logger.error(f"✗ Negative payment values: {negative_payments}")
                # Don't raise - might be refunds
            
            if zero_payments > 0:
                logger.warning(f"⚠ Zero payment values: {zero_payments} ({zero_payments/len(df)*100:.2f}%)")
            
            if missing_payments > 0:
                logger.warning(f"⚠ Missing payment values: {missing_payments}")
        
        # Check installments
        if 'payment_installments' in df.columns:
            logger.info(f"  - Installments stats: min={df['payment_installments'].min():.0f}, "
                       f"max={df['payment_installments'].max():.0f}, "
                       f"avg={df['payment_installments'].mean():.1f}")
            
            # Check for invalid installments
            invalid_installments = (df['payment_installments'] < 1).sum()
            if invalid_installments > 0:
                logger.warning(f"⚠ Invalid installments (< 1): {invalid_installments}")
        
        # Check payment sequential (multiple payments per order)
        if 'payment_sequential' in df.columns:
            max_sequential = df['payment_sequential'].max()
            logger.info(f"  - Max payment sequential: {max_sequential} (indicates up to {max_sequential} payments per order)")
        
        logger.info("✓ Payment data quality validation passed")


# Test the extractor
if __name__ == "__main__":
    extractor = PaymentsExtractor()
    df_payments = extractor.extract()
    
    print("\n" + "="*80)
    print("PAYMENTS EXTRACTION TEST")
    print("="*80)
    print(f"\nTotal rows: {len(df_payments):,}")
    print(f"Columns: {list(df_payments.columns)}")
    
    print("\nFirst 5 rows:")
    print(df_payments.head())
    
    print("\nData types:")
    print(df_payments.dtypes)
    
    print("\nMissing values:")
    print(df_payments.isnull().sum())
    
    if 'payment_type' in df_payments.columns:
        print("\nPayment type distribution:")
        print(df_payments['payment_type'].value_counts())
    
    if 'payment_value' in df_payments.columns:
        print(f"\nPayment value statistics:")
        print(df_payments['payment_value'].describe())
    
    # Check for multi-payment orders
    if 'order_id' in df_payments.columns:
        payments_per_order = df_payments.groupby('order_id').size()
        multi_payments = payments_per_order[payments_per_order > 1]
        print(f"\nOrders with multiple payments: {len(multi_payments):,}")
        if len(multi_payments) > 0:
            print(f"Example multi-payment order:")
            example_order = multi_payments.index[0]
            print(df_payments[df_payments['order_id'] == example_order])
