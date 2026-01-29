"""
Create PostgreSQL schema for data warehouse
Defines all dimension and fact tables WITHOUT foreign key constraints
"""

import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from src.utils.logger import setup_logger
from src.utils.db_connector import DatabaseConnector

logger = setup_logger('create_schema')


class SchemaCreator:
    """Create data warehouse schema in PostgreSQL"""
    
    def __init__(self):
        """Initialize schema creator"""
        self.db = DatabaseConnector()
        logger.info("SchemaCreator initialized")
    
    def create_all_tables(self):
        """Create all dimension and fact tables"""
        try:
            logger.info("Creating data warehouse schema...")
            
            # Drop existing tables (in reverse dependency order)
            logger.info("Dropping existing tables if they exist...")
            self._drop_tables()
            
            # Create dimension tables first (no FK dependencies)
            logger.info("\nCreating dimension tables...")
            self._create_dim_date()
            self._create_dim_products()
            self._create_dim_payment_type()
            self._create_dim_customers()
            
            # Create fact tables (depend on dimensions)
            logger.info("\nCreating fact tables...")
            self._create_fact_orders()
            self._create_fact_cohort_retention()
            
            # Create indexes for performance
            logger.info("\nCreating indexes...")
            self._create_indexes()
            
            logger.info("\n" + "="*80)
            logger.info("✓ DATA WAREHOUSE SCHEMA CREATED SUCCESSFULLY")
            logger.info("="*80 + "\n")
            
            # Verify tables
            self._verify_tables()
            
        except Exception as e:
            logger.error(f"✗ Schema creation failed: {e}")
            raise
        finally:
            self.db.close_pool()
    
    def _drop_tables(self):
        """Drop existing tables in reverse dependency order"""
        drop_queries = [
            "DROP TABLE IF EXISTS fact_cohort_retention CASCADE;",
            "DROP TABLE IF EXISTS fact_orders CASCADE;",
            "DROP TABLE IF EXISTS dim_customers CASCADE;",
            "DROP TABLE IF EXISTS dim_payment_type CASCADE;",
            "DROP TABLE IF EXISTS dim_products CASCADE;",
            "DROP TABLE IF EXISTS dim_date CASCADE;"
        ]
        
        for query in drop_queries:
            self.db.execute_query(query)
        
        logger.info("  ✓ Dropped existing tables")
    
    def _create_dim_date(self):
        """Create date dimension table"""
        query = """
        CREATE TABLE dim_date (
            date_key INTEGER PRIMARY KEY,
            full_date DATE NOT NULL UNIQUE,
            year INTEGER NOT NULL,
            quarter INTEGER NOT NULL,
            month INTEGER NOT NULL,
            month_name VARCHAR(20) NOT NULL,
            week INTEGER NOT NULL,
            day_of_month INTEGER NOT NULL,
            day_of_week INTEGER NOT NULL,
            day_name VARCHAR(20) NOT NULL,
            is_weekend BOOLEAN NOT NULL,
            is_holiday BOOLEAN NOT NULL,
            fiscal_year INTEGER NOT NULL,
            fiscal_quarter INTEGER NOT NULL,
            created_at TIMESTAMP NOT NULL
        );
        """
        self.db.execute_query(query)
        logger.info("  ✓ Created dim_date")
    
    def _create_dim_products(self):
        """Create product dimension table"""
        query = """
        CREATE TABLE dim_products (
            product_key INTEGER PRIMARY KEY,
            product_id VARCHAR(50) NOT NULL UNIQUE,
            product_category_name VARCHAR(100),
            product_category_english VARCHAR(100),
            product_category_segment VARCHAR(50),
            product_weight_g INTEGER,
            product_length_cm INTEGER,
            product_height_cm INTEGER,
            product_width_cm INTEGER,
            product_volume_cm3 NUMERIC(12, 2),
            product_photos_qty INTEGER,
            has_photos BOOLEAN,
            created_at TIMESTAMP NOT NULL
        );
        """
        self.db.execute_query(query)
        logger.info("  ✓ Created dim_products")
    
    def _create_dim_payment_type(self):
        """Create payment type dimension table"""
        query = """
        CREATE TABLE dim_payment_type (
            payment_type_key INTEGER PRIMARY KEY,
            payment_type VARCHAR(50) NOT NULL UNIQUE,
            payment_category VARCHAR(50) NOT NULL,
            created_at TIMESTAMP NOT NULL
        );
        """
        self.db.execute_query(query)
        logger.info("  ✓ Created dim_payment_type")
    
    def _create_dim_customers(self):
        """Create customer dimension table"""
        query = """
        CREATE TABLE dim_customers (
            customer_key INTEGER PRIMARY KEY,
            customer_id VARCHAR(50) NOT NULL UNIQUE,
            customer_unique_id VARCHAR(50),
            customer_city VARCHAR(100),
            customer_state VARCHAR(2),
            customer_region VARCHAR(50),
            customer_segment VARCHAR(20),
            first_order_date TIMESTAMP,
            last_order_date TIMESTAMP,
            total_orders INTEGER,
            delivered_orders INTEGER,
            total_spent NUMERIC(12, 2),
            avg_order_value NUMERIC(12, 2),
            lifetime_value NUMERIC(12, 2),
            days_as_customer INTEGER,
            purchase_frequency_annual NUMERIC(12, 2),
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL
        );
        """
        self.db.execute_query(query)
        logger.info("  ✓ Created dim_customers")
    
    def _create_fact_orders(self):
        """Create fact orders table WITHOUT foreign key constraints"""
        query = """
        CREATE TABLE fact_orders (
            order_key BIGSERIAL PRIMARY KEY,
            customer_key INTEGER,
            product_key INTEGER,
            order_date_key INTEGER,
            delivery_date_key INTEGER,
            payment_type_key INTEGER,
            order_id VARCHAR(50) NOT NULL UNIQUE,
            order_status VARCHAR(50),
            seller_id VARCHAR(50),
            order_item_count INTEGER,
            order_subtotal NUMERIC(12, 2),
            order_freight_total NUMERIC(12, 2),
            order_total_value NUMERIC(12, 2),
            payment_value NUMERIC(12, 2),
            payment_installments INTEGER,
            delivery_days INTEGER,
            estimated_delivery_days INTEGER,
            delivery_delay_days INTEGER,
            is_late_delivery BOOLEAN,
            is_completed_order BOOLEAN,
            review_score INTEGER,
            has_review BOOLEAN,
            order_purchase_timestamp TIMESTAMP,
            order_delivered_customer_date TIMESTAMP,
            created_at TIMESTAMP NOT NULL
        );
        """
        self.db.execute_query(query)
        logger.info("  ✓ Created fact_orders (NO FK constraints for flexibility)")
    
    def _create_fact_cohort_retention(self):
        """Create cohort retention fact table"""
        query = """
        CREATE TABLE fact_cohort_retention (
            cohort_retention_key SERIAL PRIMARY KEY,
            cohort_month DATE NOT NULL,
            months_since_first_purchase INTEGER NOT NULL,
            cohort_size INTEGER NOT NULL,
            retained_customers INTEGER NOT NULL,
            retention_rate NUMERIC(5, 2) NOT NULL,
            created_at TIMESTAMP NOT NULL,
            UNIQUE(cohort_month, months_since_first_purchase)
        );
        """
        self.db.execute_query(query)
        logger.info("  ✓ Created fact_cohort_retention")
    
    def _create_indexes(self):
        """Create indexes for query performance"""
        indexes = [
            # Date dimension indexes
            "CREATE INDEX idx_dim_date_full_date ON dim_date(full_date);",
            "CREATE INDEX idx_dim_date_year_month ON dim_date(year, month);",
            
            # Product dimension indexes
            "CREATE INDEX idx_dim_products_category_segment ON dim_products(product_category_segment);",
            "CREATE INDEX idx_dim_products_category_english ON dim_products(product_category_english);",
            
            # Customer dimension indexes
            "CREATE INDEX idx_dim_customers_segment ON dim_customers(customer_segment);",
            "CREATE INDEX idx_dim_customers_region ON dim_customers(customer_region);",
            "CREATE INDEX idx_dim_customers_unique_id ON dim_customers(customer_unique_id);",
            "CREATE INDEX idx_dim_customers_clv ON dim_customers(lifetime_value);",
            
            # Fact orders indexes (for fast queries even without FK constraints)
            "CREATE INDEX idx_fact_orders_customer_key ON fact_orders(customer_key);",
            "CREATE INDEX idx_fact_orders_product_key ON fact_orders(product_key);",
            "CREATE INDEX idx_fact_orders_order_date_key ON fact_orders(order_date_key);",
            "CREATE INDEX idx_fact_orders_delivery_date_key ON fact_orders(delivery_date_key);",
            "CREATE INDEX idx_fact_orders_payment_type_key ON fact_orders(payment_type_key);",
            "CREATE INDEX idx_fact_orders_status ON fact_orders(order_status);",
            "CREATE INDEX idx_fact_orders_completed ON fact_orders(is_completed_order);",
            "CREATE INDEX idx_fact_orders_late ON fact_orders(is_late_delivery);",
            
            # Composite indexes for common query patterns
            "CREATE INDEX idx_fact_orders_customer_date ON fact_orders(customer_key, order_date_key);",
            "CREATE INDEX idx_fact_orders_date_status ON fact_orders(order_date_key, order_status);",
            
            # Cohort retention indexes
            "CREATE INDEX idx_fact_cohort_month ON fact_cohort_retention(cohort_month);",
            "CREATE INDEX idx_fact_cohort_months_since ON fact_cohort_retention(months_since_first_purchase);"
        ]
        
        for idx_query in indexes:
            try:
                self.db.execute_query(idx_query)
            except Exception as e:
                logger.warning(f"  ⚠ Index creation warning: {e}")
        
        logger.info(f"  ✓ Created {len(indexes)} indexes")
    
    def _verify_tables(self):
        """Verify all tables were created"""
        query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_type = 'BASE TABLE'
        ORDER BY table_name;
        """
        
        tables = self.db.fetch_query(query)
        
        logger.info("\nVerifying tables created:")
        for table in tables:
            logger.info(f"  ✓ {table[0]}")
        
        logger.info(f"\nTotal tables created: {len(tables)}")


# Main execution
if __name__ == "__main__":
    print("\n" + "="*80)
    print("CREATING DATA WAREHOUSE SCHEMA IN POSTGRESQL")
    print("="*80 + "\n")
    
    creator = SchemaCreator()
    creator.create_all_tables()
    
    print("\n✓ Schema creation complete!")
    print("\nNext step: Load transformed data into tables")
