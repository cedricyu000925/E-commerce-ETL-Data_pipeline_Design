"""
Master Transform Orchestrator
Runs all transformations in correct dependency order
Demonstrates production-level ETL orchestration
"""

import pandas as pd
import sys
import os
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from src.utils.logger import setup_logger

# Import all dimension builders
from src.transform.dim_date import DateDimensionBuilder
from src.transform.dim_products import ProductDimensionBuilder
from src.transform.dim_payment_type import PaymentTypeDimensionBuilder
from src.transform.dim_customers import CustomerDimensionBuilder

# Import fact builders
from src.transform.fact_orders import FactOrdersBuilder
from src.transform.fact_cohort_retention import CohortRetentionBuilder

logger = setup_logger('transform_orchestrator')


class TransformOrchestrator:
    """
    Master orchestrator for all transformations
    Executes transforms in dependency order
    """
    
    def __init__(self):
        """Initialize orchestrator"""
        self.start_time = None
        self.dimensions = {}
        self.facts = {}
        logger.info("TransformOrchestrator initialized")
    
    def run_all(self):
        """
        Execute all transformations in correct order
        
        Returns:
            dict: Dictionary containing all dimension and fact tables
        """
        try:
            self.start_time = datetime.now()
            
            logger.info("\n" + "="*80)
            logger.info("STARTING COMPLETE TRANSFORMATION PIPELINE")
            logger.info("="*80 + "\n")
            
            # Phase 1: Build Dimensions (no dependencies)
            logger.info("PHASE 1: Building Dimension Tables")
            logger.info("-" * 80)
            
            self._build_dim_date()
            self._build_dim_products()
            self._build_dim_payment_type()
            
            logger.info("\n")
            
            # Phase 2: Build Customer Dimension (depends on orders for CLV)
            logger.info("PHASE 2: Building Customer Dimension (with CLV)")
            logger.info("-" * 80)
            
            self._build_dim_customers()
            
            logger.info("\n")
            
            # Phase 3: Build Fact Tables (depend on all dimensions)
            logger.info("PHASE 3: Building Fact Tables")
            logger.info("-" * 80)
            
            self._build_fact_orders()
            self._build_fact_cohort_retention()
            
            logger.info("\n")
            
            # Summary
            self._print_summary()
            
            # Return all tables
            result = {
                'dimensions': self.dimensions,
                'facts': self.facts
            }
            
            return result
            
        except Exception as e:
            logger.error(f"✗ Transformation pipeline failed: {e}")
            import traceback
            traceback.print_exc()
            raise
    
    def _build_dim_date(self):
        """Build date dimension"""
        logger.info("[1/6] Building DIM_DATE...")
        builder = DateDimensionBuilder()
        self.dimensions['dim_date'] = builder.build()
        logger.info(f"  ✓ DIM_DATE: {len(self.dimensions['dim_date']):,} rows\n")
    
    def _build_dim_products(self):
        """Build product dimension"""
        logger.info("[2/6] Building DIM_PRODUCTS...")
        builder = ProductDimensionBuilder()
        self.dimensions['dim_products'] = builder.build()
        logger.info(f"  ✓ DIM_PRODUCTS: {len(self.dimensions['dim_products']):,} rows\n")
    
    def _build_dim_payment_type(self):
        """Build payment type dimension"""
        logger.info("[3/6] Building DIM_PAYMENT_TYPE...")
        builder = PaymentTypeDimensionBuilder()
        self.dimensions['dim_payment_type'] = builder.build()
        logger.info(f"  ✓ DIM_PAYMENT_TYPE: {len(self.dimensions['dim_payment_type'])} rows\n")
    
    def _build_dim_customers(self):
        """Build customer dimension (with CLV)"""
        logger.info("[4/6] Building DIM_CUSTOMERS (with CLV calculation)...")
        builder = CustomerDimensionBuilder()
        self.dimensions['dim_customers'] = builder.build()
        logger.info(f"  ✓ DIM_CUSTOMERS: {len(self.dimensions['dim_customers']):,} rows\n")
    
    def _build_fact_orders(self):
        """Build fact orders"""
        logger.info("[5/6] Building FACT_ORDERS...")
        builder = FactOrdersBuilder()
        self.facts['fact_orders'] = builder.build(
            dim_customers=self.dimensions['dim_customers'],
            dim_products=self.dimensions['dim_products'],
            dim_payment_type=self.dimensions['dim_payment_type'],
            dim_date=self.dimensions['dim_date']
        )
        logger.info(f"  ✓ FACT_ORDERS: {len(self.facts['fact_orders']):,} rows\n")
    
    def _build_fact_cohort_retention(self):
        """Build cohort retention fact"""
        logger.info("[6/6] Building FACT_COHORT_RETENTION...")
        builder = CohortRetentionBuilder()
        self.facts['fact_cohort_retention'] = builder.build(
            dim_customers=self.dimensions['dim_customers']
        )
        logger.info(f"  ✓ FACT_COHORT_RETENTION: {len(self.facts['fact_cohort_retention']):,} rows\n")
    
    def _print_summary(self):
        """Print pipeline execution summary"""
        end_time = datetime.now()
        duration = (end_time - self.start_time).total_seconds()
        
        logger.info("\n" + "="*80)
        logger.info("TRANSFORMATION PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("="*80)
        
        logger.info(f"\nExecution time: {duration:.1f} seconds ({duration/60:.1f} minutes)")
        
        logger.info("\nDIMENSION TABLES:")
        for name, df in self.dimensions.items():
            logger.info(f"  - {name.upper()}: {len(df):,} rows × {len(df.columns)} columns")
        
        logger.info("\nFACT TABLES:")
        for name, df in self.facts.items():
            logger.info(f"  - {name.upper()}: {len(df):,} rows × {len(df.columns)} columns")
        
        # Calculate total row count
        total_dim_rows = sum(len(df) for df in self.dimensions.values())
        total_fact_rows = sum(len(df) for df in self.facts.values())
        total_rows = total_dim_rows + total_fact_rows
        
        logger.info(f"\nTOTAL DATA WAREHOUSE SIZE:")
        logger.info(f"  - Dimension rows: {total_dim_rows:,}")
        logger.info(f"  - Fact rows: {total_fact_rows:,}")
        logger.info(f"  - Total rows: {total_rows:,}")
        
        logger.info("\n" + "="*80 + "\n")
    
    def save_to_csv(self, output_dir='data/staging'):
        """
        Save all transformed tables to CSV (for inspection before loading to DB)
        
        Args:
            output_dir: Directory to save CSV files
        """
        import os
        
        os.makedirs(output_dir, exist_ok=True)
        
        logger.info(f"Saving transformed tables to {output_dir}...")
        
        # Save dimensions
        for name, df in self.dimensions.items():
            filepath = os.path.join(output_dir, f"{name}.csv")
            df.to_csv(filepath, index=False)
            logger.info(f"  ✓ Saved {name}.csv ({len(df):,} rows)")
        
        # Save facts
        for name, df in self.facts.items():
            filepath = os.path.join(output_dir, f"{name}.csv")
            df.to_csv(filepath, index=False)
            logger.info(f"  ✓ Saved {name}.csv ({len(df):,} rows)")
        
        logger.info(f"\n✓ All tables saved to {output_dir}/")


# Main execution
if __name__ == "__main__":
    print("\n" + "="*80)
    print("MASTER TRANSFORM ORCHESTRATOR")
    print("="*80 + "\n")
    
    # Create orchestrator
    orchestrator = TransformOrchestrator()
    
    # Run all transformations
    result = orchestrator.run_all()
    
    # Save to CSV for inspection
    orchestrator.save_to_csv()
    
    print("\n" + "="*80)
    print("✓ TRANSFORMATION PIPELINE COMPLETE")
    print("="*80)
    print("\nTransformed data saved to: data/staging/")
    print("\nYou can now inspect the CSV files before loading to PostgreSQL")
    print("\nNext step: Load transformed data into PostgreSQL (Phase 4: Load)")
