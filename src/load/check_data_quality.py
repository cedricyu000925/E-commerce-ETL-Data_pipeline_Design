"""
Data Quality Check Script
Validates data integrity and referential relationships
"""

import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from src.utils.logger import setup_logger
from src.utils.db_connector import DatabaseConnector

logger = setup_logger('data_quality_check')


class DataQualityChecker:
    """Check data quality in the data warehouse"""
    
    def __init__(self):
        """Initialize data quality checker"""
        self.db = DatabaseConnector()
        logger.info("DataQualityChecker initialized")
    
    def run_all_checks(self):
        """Run all data quality checks"""
        try:
            print("\n" + "="*80)
            print("DATA QUALITY REPORT")
            print("="*80 + "\n")
            
            # 1. Row counts
            self._check_row_counts()
            
            # 2. Referential integrity
            self._check_referential_integrity()
            
            # 3. NULL values
            self._check_null_values()
            
            # 4. Data consistency
            self._check_data_consistency()
            
            # 5. Business logic validation
            self._check_business_logic()
            
            print("\n" + "="*80)
            print("✓ DATA QUALITY CHECK COMPLETE")
            print("="*80 + "\n")
            
        except Exception as e:
            logger.error(f"✗ Data quality check failed: {e}")
            raise
        finally:
            self.db.close_pool()
    
    def _check_row_counts(self):
        """Check row counts for all tables"""
        print("[1/5] ROW COUNTS")
        print("-" * 80)
        
        tables = [
            'dim_date',
            'dim_products', 
            'dim_payment_type',
            'dim_customers',
            'fact_orders',
            'fact_cohort_retention'
        ]
        
        for table in tables:
            query = f"SELECT COUNT(*) FROM {table};"
            result = self.db.fetch_query(query)
            count = result[0][0]
            print(f"  {table:25} : {count:>10,} rows")
        
        print()
    
    def _check_referential_integrity(self):
        """Check referential integrity between fact and dimension tables"""
        print("[2/5] REFERENTIAL INTEGRITY")
        print("-" * 80)
        
        # Check customer_key
        query = """
        SELECT COUNT(*) as total_rows,
               COUNT(DISTINCT f.customer_key) as unique_customer_keys,
               COUNT(DISTINCT c.customer_key) as matched_customer_keys,
               COUNT(DISTINCT f.customer_key) - COUNT(DISTINCT c.customer_key) as orphaned_keys
        FROM fact_orders f
        LEFT JOIN dim_customers c ON f.customer_key = c.customer_key;
        """
        result = self.db.fetch_query(query)
        total, unique, matched, orphaned = result[0]
        
        print(f"  Customer Keys:")
        print(f"    - Total orders              : {total:>10,}")
        print(f"    - Unique customer keys      : {unique:>10,}")
        print(f"    - Matched in dim_customers  : {matched:>10,}")
        print(f"    - Orphaned (no match)       : {orphaned:>10,}")
        
        if orphaned > 0:
            print(f"    ⚠ WARNING: {orphaned} customer keys have no matching dimension record")
        else:
            print(f"    ✓ All customer keys are valid")
        
        # Check product_key
        query = """
        SELECT COUNT(*) as total_rows,
               COUNT(DISTINCT f.product_key) as unique_product_keys,
               COUNT(DISTINCT p.product_key) as matched_product_keys,
               COUNT(DISTINCT f.product_key) - COUNT(DISTINCT p.product_key) as orphaned_keys
        FROM fact_orders f
        LEFT JOIN dim_products p ON f.product_key = p.product_key;
        """
        result = self.db.fetch_query(query)
        total, unique, matched, orphaned = result[0]
        
        print(f"\n  Product Keys:")
        print(f"    - Total orders              : {total:>10,}")
        print(f"    - Unique product keys       : {unique:>10,}")
        print(f"    - Matched in dim_products   : {matched:>10,}")
        print(f"    - Orphaned (no match)       : {orphaned:>10,}")
        
        if orphaned > 0:
            print(f"    ⚠ WARNING: {orphaned} product keys have no matching dimension record")
        else:
            print(f"    ✓ All product keys are valid")
        
        # Check date_key
        query = """
        SELECT COUNT(*) as total_rows,
               COUNT(DISTINCT f.order_date_key) as unique_date_keys,
               COUNT(DISTINCT d.date_key) as matched_date_keys,
               COUNT(DISTINCT f.order_date_key) - COUNT(DISTINCT d.date_key) as orphaned_keys
        FROM fact_orders f
        LEFT JOIN dim_date d ON f.order_date_key = d.date_key;
        """
        result = self.db.fetch_query(query)
        total, unique, matched, orphaned = result[0]
        
        print(f"\n  Date Keys:")
        print(f"    - Total orders              : {total:>10,}")
        print(f"    - Unique date keys          : {unique:>10,}")
        print(f"    - Matched in dim_date       : {matched:>10,}")
        print(f"    - Orphaned (no match)       : {orphaned:>10,}")
        
        if orphaned > 0:
            print(f"    ⚠ WARNING: {orphaned} date keys have no matching dimension record")
        else:
            print(f"    ✓ All date keys are valid")
        
        # Check payment_type_key
        query = """
        SELECT COUNT(*) as total_rows,
               COUNT(DISTINCT f.payment_type_key) as unique_payment_keys,
               COUNT(DISTINCT pt.payment_type_key) as matched_payment_keys,
               COUNT(DISTINCT f.payment_type_key) - COUNT(DISTINCT pt.payment_type_key) as orphaned_keys
        FROM fact_orders f
        LEFT JOIN dim_payment_type pt ON f.payment_type_key = pt.payment_type_key;
        """
        result = self.db.fetch_query(query)
        total, unique, matched, orphaned = result[0]
        
        print(f"\n  Payment Type Keys:")
        print(f"    - Total orders              : {total:>10,}")
        print(f"    - Unique payment keys       : {unique:>10,}")
        print(f"    - Matched in dim_payment    : {matched:>10,}")
        print(f"    - Orphaned (no match)       : {orphaned:>10,}")
        
        if orphaned > 0:
            print(f"    ⚠ WARNING: {orphaned} payment keys have no matching dimension record")
        else:
            print(f"    ✓ All payment keys are valid")
        
        print()
    
    def _check_null_values(self):
        """Check for NULL values in critical fields"""
        print("[3/5] NULL VALUE CHECKS")
        print("-" * 80)
        
        # Check fact_orders
        query = """
        SELECT 
            COUNT(*) FILTER (WHERE customer_key IS NULL) as null_customer_keys,
            COUNT(*) FILTER (WHERE product_key IS NULL) as null_product_keys,
            COUNT(*) FILTER (WHERE order_date_key IS NULL) as null_order_date_keys,
            COUNT(*) FILTER (WHERE payment_type_key IS NULL) as null_payment_keys,
            COUNT(*) FILTER (WHERE order_id IS NULL) as null_order_ids,
            COUNT(*) FILTER (WHERE order_total_value IS NULL) as null_order_values,
            COUNT(*) as total_rows
        FROM fact_orders;
        """
        result = self.db.fetch_query(query)
        null_cust, null_prod, null_date, null_pay, null_id, null_val, total = result[0]
        
        print(f"  fact_orders (Total: {total:,} rows):")
        
        checks = [
            ("customer_key", null_cust),
            ("product_key", null_prod),
            ("order_date_key", null_date),
            ("payment_type_key", null_pay),
            ("order_id", null_id),
            ("order_total_value", null_val)
        ]
        
        has_nulls = False
        for field, null_count in checks:
            if null_count > 0:
                pct = (null_count / total * 100) if total > 0 else 0
                print(f"    ⚠ {field:25} : {null_count:>7,} NULLs ({pct:.2f}%)")
                has_nulls = True
        
        if not has_nulls:
            print(f"    ✓ No NULL values in critical fields")
        
        # Check dim_customers
        query = """
        SELECT 
            COUNT(*) FILTER (WHERE customer_id IS NULL) as null_customer_ids,
            COUNT(*) FILTER (WHERE customer_segment IS NULL) as null_segments,
            COUNT(*) as total_rows
        FROM dim_customers;
        """
        result = self.db.fetch_query(query)
        null_id, null_seg, total = result[0]
        
        print(f"\n  dim_customers (Total: {total:,} rows):")
        if null_id > 0 or null_seg > 0:
            if null_id > 0:
                print(f"    ⚠ customer_id                : {null_id:>7,} NULLs")
            if null_seg > 0:
                print(f"    ⚠ customer_segment           : {null_seg:>7,} NULLs")
        else:
            print(f"    ✓ No NULL values in critical fields")
        
        print()
    
    def _check_data_consistency(self):
        """Check data consistency and logical errors"""
        print("[4/5] DATA CONSISTENCY")
        print("-" * 80)
        
        # Check negative values
        query = """
        SELECT 
            COUNT(*) FILTER (WHERE order_total_value < 0) as negative_values,
            COUNT(*) FILTER (WHERE order_total_value = 0) as zero_values,
            COUNT(*) FILTER (WHERE delivery_days < 0) as negative_delivery_days
        FROM fact_orders;
        """
        result = self.db.fetch_query(query)
        neg_val, zero_val, neg_days = result[0]
        
        print(f"  Numeric Validity:")
        if neg_val > 0:
            print(f"    ⚠ Negative order values      : {neg_val:>7,} rows")
        else:
            print(f"    ✓ No negative order values")
        
        if zero_val > 0:
            print(f"    ⚠ Zero order values          : {zero_val:>7,} rows")
        else:
            print(f"    ✓ No zero order values")
        
        if neg_days > 0:
            print(f"    ⚠ Negative delivery days     : {neg_days:>7,} rows")
        else:
            print(f"    ✓ No negative delivery days")
        
        # Check duplicates
        query = """
        SELECT COUNT(*) as duplicate_count
        FROM (
            SELECT order_id, COUNT(*) as cnt
            FROM fact_orders
            GROUP BY order_id
            HAVING COUNT(*) > 1
        ) duplicates;
        """
        result = self.db.fetch_query(query)
        dup_count = result[0][0]
        
        print(f"\n  Uniqueness:")
        if dup_count > 0:
            print(f"    ⚠ Duplicate order_ids        : {dup_count:>7,} duplicates")
        else:
            print(f"    ✓ All order_ids are unique")
        
        print()
    
    def _check_business_logic(self):
        """Check business logic validation"""
        print("[5/5] BUSINESS LOGIC VALIDATION")
        print("-" * 80)
        
        # Check order status distribution
        query = """
        SELECT 
            order_status,
            COUNT(*) as count,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
        FROM fact_orders
        GROUP BY order_status
        ORDER BY count DESC;
        """
        result = self.db.fetch_query(query)
        
        print(f"  Order Status Distribution:")
        for status, count, pct in result:
            print(f"    - {status:20} : {count:>10,} ({pct:>5.2f}%)")
        
        # Check completed orders metrics
        query = """
        SELECT 
            COUNT(*) as total_orders,
            COUNT(*) FILTER (WHERE is_completed_order = TRUE) as completed_orders,
            COUNT(*) FILTER (WHERE is_late_delivery = TRUE) as late_deliveries,
            AVG(delivery_days) as avg_delivery_days,
            AVG(order_total_value) as avg_order_value
        FROM fact_orders;
        """
        result = self.db.fetch_query(query)
        total, completed, late, avg_days, avg_value = result[0]
        
        completion_rate = (completed / total * 100) if total > 0 else 0
        late_rate = (late / total * 100) if total > 0 else 0
        
        print(f"\n  Order Metrics:")
        print(f"    - Total orders               : {total:>10,}")
        print(f"    - Completed orders           : {completed:>10,} ({completion_rate:.2f}%)")
        print(f"    - Late deliveries            : {late:>10,} ({late_rate:.2f}%)")
        print(f"    - Average delivery days      : {avg_days:>10.1f} days")
        print(f"    - Average order value        : R$ {avg_value:>10,.2f}")
        
        # Check customer segments
        query = """
        SELECT 
            customer_segment,
            COUNT(*) as customer_count,
            AVG(lifetime_value) as avg_clv
        FROM dim_customers
        GROUP BY customer_segment
        ORDER BY customer_count DESC;
        """
        result = self.db.fetch_query(query)
        
        print(f"\n  Customer Segments:")
        for segment, count, avg_clv in result:
            print(f"    - {segment:20} : {count:>7,} customers (Avg CLV: R$ {avg_clv:,.2f})")
        
        print()


# Main execution
if __name__ == "__main__":
    checker = DataQualityChecker()
    checker.run_all_checks()
