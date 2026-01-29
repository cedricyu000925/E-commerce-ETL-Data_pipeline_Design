"""
Master Load Orchestrator
Runs complete load pipeline: Schema → Dimensions → Facts
"""

import sys
import os
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from src.utils.logger import setup_logger
from src.load.create_schema import SchemaCreator
from src.load.load_dimensions import DimensionLoader
from src.load.load_facts import FactLoader

logger = setup_logger('load_orchestrator')


class LoadOrchestrator:
    """Master orchestrator for complete load pipeline"""
    
    def __init__(self):
        """Initialize load orchestrator"""
        self.start_time = None
        logger.info("LoadOrchestrator initialized")
    
    def run_complete_load(self):
        """Execute complete load pipeline"""
        try:
            self.start_time = datetime.now()
            
            logger.info("\n" + "="*80)
            logger.info("STARTING COMPLETE LOAD PIPELINE")
            logger.info("="*80 + "\n")
            
            # Phase 1: Create schema
            logger.info("PHASE 1: Creating Database Schema")
            logger.info("-" * 80)
            schema_creator = SchemaCreator()
            schema_creator.create_all_tables()
            
            # Phase 2: Load dimensions
            logger.info("\nPHASE 2: Loading Dimension Tables")
            logger.info("-" * 80)
            dim_loader = DimensionLoader()
            dim_loader.load_all_dimensions()
            
            # Phase 3: Load facts
            logger.info("\nPHASE 3: Loading Fact Tables")
            logger.info("-" * 80)
            fact_loader = FactLoader()
            fact_loader.load_all_facts()
            
            # Summary
            self._print_summary()
            
        except Exception as e:
            logger.error(f"✗ Load pipeline failed: {e}")
            import traceback
            traceback.print_exc()
            raise
    
    def _print_summary(self):
        """Print pipeline execution summary"""
        end_time = datetime.now()
        duration = (end_time - self.start_time).total_seconds()
        
        logger.info("\n" + "="*80)
        logger.info("LOAD PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("="*80)
        
        logger.info(f"\nExecution time: {duration:.1f} seconds ({duration/60:.1f} minutes)")
        
        logger.info("\n✓ Data warehouse is now fully populated!")
        logger.info("\nWhat you've accomplished:")
        logger.info("  1. ✓ Created PostgreSQL schema with 6 tables")
        logger.info("  2. ✓ Loaded 4 dimension tables")
        logger.info("  3. ✓ Loaded 2 fact tables")
        logger.info("  4. ✓ Created 20+ indexes for query performance")
        logger.info("  5. ✓ Established referential integrity with foreign keys")
        
        logger.info("\n" + "="*80 + "\n")


# Main execution
if __name__ == "__main__":
    print("\n" + "="*80)
    print("MASTER LOAD ORCHESTRATOR")
    print("Running complete ETL Load pipeline...")
    print("="*80 + "\n")
    
    orchestrator = LoadOrchestrator()
    orchestrator.run_complete_load()
    
    print("\n" + "="*80)
    print("✓ LOAD PIPELINE COMPLETE")
    print("="*80)
    print("\nYour PostgreSQL data warehouse is ready!")
    print("\nNext steps:")
    print("  1. Verify data in PostgreSQL (use pgAdmin or psql)")
    print("  2. Run SQL queries to validate data quality")
    print("  3. Connect Power BI to PostgreSQL")
    print("  4. Build your executive dashboard")
