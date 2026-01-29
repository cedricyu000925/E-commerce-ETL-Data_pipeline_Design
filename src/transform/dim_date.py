"""
Generate Date Dimension table
Creates a complete date dimension from 2016-2018 with all date attributes
Uses Polars for fast date generation
"""

import polars as pl
import pandas as pd
from datetime import datetime, timedelta
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from src.utils.logger import setup_logger
import yaml

logger = setup_logger('transform_dim_date')


class DateDimensionBuilder:
    """Build Date Dimension using Polars for performance"""
    
    def __init__(self, config_path='config/business_rules.yaml'):
        """Initialize with date range from config"""
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        self.start_date = config['date_dimension']['start_date']
        self.end_date = config['date_dimension']['end_date']
        
        # Brazilian holidays (2016-2018)
        self.holidays = [
            # 2016
            '2016-01-01', '2016-02-08', '2016-02-09', '2016-03-25',  # New Year, Carnival, Good Friday
            '2016-04-21', '2016-05-01', '2016-09-07', '2016-10-12',  # Tiradentes, Labor, Independence, Our Lady
            '2016-11-02', '2016-11-15', '2016-12-25',                # All Souls, Republic, Christmas
            # 2017
            '2017-01-01', '2017-02-27', '2017-02-28', '2017-04-14',
            '2017-04-21', '2017-05-01', '2017-09-07', '2017-10-12',
            '2017-11-02', '2017-11-15', '2017-12-25',
            # 2018
            '2018-01-01', '2018-02-12', '2018-02-13', '2018-03-30',
            '2018-04-21', '2018-05-01', '2018-09-07', '2018-10-12',
            '2018-11-02', '2018-11-15', '2018-12-25'
        ]
        
        logger.info(f"DateDimensionBuilder initialized: {self.start_date} to {self.end_date}")
    
    def build(self):
        """
        Build complete date dimension
        
        Returns:
            DataFrame: Date dimension with all attributes (Pandas format)
        """
        try:
            logger.info("Building date dimension...")
            
            # Use Polars for fast date generation
            start = datetime.strptime(self.start_date, '%Y-%m-%d')
            end = datetime.strptime(self.end_date, '%Y-%m-%d')
            
            # Generate date range
            date_range = pl.date_range(
                start=start,
                end=end,
                interval='1d',
                eager=True
            )
            
            # Create Polars DataFrame
            df = pl.DataFrame({
                'full_date': date_range
            })
            
            logger.info(f"✓ Generated {len(df):,} dates from {self.start_date} to {self.end_date}")
            
            # Add date attributes using Polars (much faster than pandas)
            df = df.with_columns([
                # Date key (YYYYMMDD format as integer)
                (pl.col('full_date').dt.strftime('%Y%m%d').cast(pl.Int32)).alias('date_key'),
                
                # Year, Quarter, Month
                pl.col('full_date').dt.year().alias('year'),
                pl.col('full_date').dt.quarter().alias('quarter'),
                pl.col('full_date').dt.month().alias('month'),
                
                # Week
                pl.col('full_date').dt.week().alias('week'),
                
                # Day attributes
                pl.col('full_date').dt.day().alias('day_of_month'),
                pl.col('full_date').dt.weekday().alias('day_of_week'),  # 1=Monday, 7=Sunday
                
                # Boolean flags
                (pl.col('full_date').dt.weekday() >= 6).alias('is_weekend'),
            ])
            
            # Add month name
            df = df.with_columns([
                pl.col('full_date').dt.strftime('%B').alias('month_name')
            ])
            
            # Add day name
            df = df.with_columns([
                pl.col('full_date').dt.strftime('%A').alias('day_name')
            ])
            
            # Add fiscal year and quarter (Brazilian fiscal year = calendar year)
            df = df.with_columns([
                pl.col('year').alias('fiscal_year'),
                pl.col('quarter').alias('fiscal_quarter')
            ])
            
            logger.info("✓ Added date attributes (year, quarter, month, week, day)")
            
            # Convert to Pandas for holiday processing (easier string matching)
            df_pandas = df.to_pandas()
            
            # Add holiday flag
            df_pandas['full_date_str'] = df_pandas['full_date'].dt.strftime('%Y-%m-%d')
            df_pandas['is_holiday'] = df_pandas['full_date_str'].isin(self.holidays)
            df_pandas = df_pandas.drop('full_date_str', axis=1)
            
            logger.info(f"✓ Marked {df_pandas['is_holiday'].sum()} holidays")
            
            # Add created_at timestamp
            df_pandas['created_at'] = datetime.now()
            
            # Reorder columns
            column_order = [
                'date_key', 'full_date',
                'year', 'quarter', 'month', 'month_name', 'week',
                'day_of_month', 'day_of_week', 'day_name',
                'is_weekend', 'is_holiday',
                'fiscal_year', 'fiscal_quarter',
                'created_at'
            ]
            df_pandas = df_pandas[column_order]
            
            logger.info(f"✓ Date dimension built successfully: {len(df_pandas):,} rows")
            
            # Log some statistics
            logger.info(f"  - Date range: {df_pandas['full_date'].min()} to {df_pandas['full_date'].max()}")
            logger.info(f"  - Weekend days: {df_pandas['is_weekend'].sum():,}")
            logger.info(f"  - Holidays: {df_pandas['is_holiday'].sum()}")
            logger.info(f"  - Total days: {len(df_pandas):,}")
            
            return df_pandas
            
        except Exception as e:
            logger.error(f"✗ Date dimension build failed: {e}")
            raise


# Test the builder
if __name__ == "__main__":
    builder = DateDimensionBuilder()
    df_date = builder.build()
    
    print("\n" + "="*80)
    print("DATE DIMENSION TEST")
    print("="*80)
    
    print(f"\nTotal dates: {len(df_date):,}")
    print(f"\nColumns: {list(df_date.columns)}")
    
    print("\nFirst 10 rows:")
    print(df_date.head(10))
    
    print("\nLast 10 rows:")
    print(df_date.tail(10))
    
    print("\nData types:")
    print(df_date.dtypes)
    
    print("\nSample holidays:")
    holidays = df_date[df_date['is_holiday'] == True]
    print(holidays[['date_key', 'full_date', 'day_name']].head(10))
    
    print("\nWeekend count:")
    print(f"Weekends: {df_date['is_weekend'].sum():,} days")
    
    print("\nYear distribution:")
    print(df_date['year'].value_counts().sort_index())
