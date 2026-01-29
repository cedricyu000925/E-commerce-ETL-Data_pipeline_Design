import pandas as pd
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from src.utils.db_connector import DatabaseConnector

db = DatabaseConnector()
engine = db.get_engine()

export_dir = 'data/powerbi_export'
os.makedirs(export_dir, exist_ok=True)

tables = ['dim_date', 'dim_products', 'dim_payment_type', 'dim_customers', 'fact_orders', 'fact_cohort_retention']

for table in tables:
    print(f"Exporting {table}...")
    df = pd.read_sql(f"SELECT * FROM {table}", engine)
    df.to_csv(f'{export_dir}/{table}.csv', index=False, encoding='utf-8-sig')
    print(f"  ✓ {len(df):,} rows")

print(f"\n✓ Files saved to: {os.path.abspath(export_dir)}")
db.close_pool()
