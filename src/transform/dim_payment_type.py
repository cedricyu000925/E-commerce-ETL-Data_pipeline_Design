"""
Create Payment Type Dimension (Lookup Table)
Simple dimension with payment method categorization
"""

import pandas as pd
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from src.utils.logger import setup_logger
from src.extract.extract_payments import PaymentsExtractor

logger = setup_logger('transform_dim_payment_type')


class PaymentTypeDimensionBuilder:
    """Build Payment Type Dimension (simple lookup)"""
    
    def __init__(self):
        """Initialize payment type dimension builder"""
        # Payment type categorization
        self.payment_categories = {
            'credit_card': 'Credit',
            'boleto': 'Cash/Banking',
            'debit_card': 'Debit',
            'voucher': 'Voucher',
            'not_defined': 'Unknown'
        }
        logger.info("PaymentTypeDimensionBuilder initialized")
    
    def build(self):
        """
        Build payment type dimension
        
        Returns:
            DataFrame: Payment type dimension
        """
        try:
            logger.info("Building payment type dimension...")
            
            # Extract payments to get distinct payment types
            extractor = PaymentsExtractor()
            df_payments = extractor.extract()
            
            # Get unique payment types
            unique_payment_types = df_payments['payment_type'].dropna().unique()
            
            logger.info(f"✓ Found {len(unique_payment_types)} unique payment types")
            
            # Create dimension table
            df = pd.DataFrame({
                'payment_type': unique_payment_types
            })
            
            # Add payment category
            df['payment_category'] = df['payment_type'].map(self.payment_categories)
            
            # Handle any unmapped payment types
            unmapped = df['payment_category'].isnull()
            if unmapped.any():
                logger.warning(f"⚠ {unmapped.sum()} unmapped payment types, setting to 'Other'")
                df.loc[unmapped, 'payment_category'] = 'Other'
            
            # Create surrogate key
            df = df.reset_index(drop=True)
            df.insert(0, 'payment_type_key', df.index + 1)
            
            # Add timestamp
            df['created_at'] = pd.Timestamp.now()
            
            # Reorder columns
            df = df[['payment_type_key', 'payment_type', 'payment_category', 'created_at']]
            
            logger.info(f"✓ Payment type dimension built successfully: {len(df)} rows")
            logger.info(f"  - Payment types: {', '.join(df['payment_type'].tolist())}")
            
            return df
            
        except Exception as e:
            logger.error(f"✗ Payment type dimension build failed: {e}")
            raise


# Test the builder
if __name__ == "__main__":
    builder = PaymentTypeDimensionBuilder()
    df_payment_type = builder.build()
    
    print("\n" + "="*80)
    print("PAYMENT TYPE DIMENSION TEST")
    print("="*80)
    
    print(f"\nTotal payment types: {len(df_payment_type)}")
    print(f"\nColumns: {list(df_payment_type.columns)}")
    
    print("\nAll rows:")
    print(df_payment_type)
    
    print("\nData types:")
    print(df_payment_type.dtypes)
