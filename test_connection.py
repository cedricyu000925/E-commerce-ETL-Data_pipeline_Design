import psycopg2
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Connection parameters
conn_params = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'olist_dw'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', '000925Lilas')
}

try:
    # Attempt connection
    print("Attempting to connect to PostgreSQL...")
    conn = psycopg2.connect(**conn_params)
    
    # Create cursor
    cursor = conn.cursor()
    
    # Test query
    cursor.execute("SELECT version();")
    db_version = cursor.fetchone()
    print(f"✅ Connection successful!")
    print(f"PostgreSQL version: {db_version[0]}")
    
    # Check database
    cursor.execute("SELECT current_database();")
    current_db = cursor.fetchone()[0]
    print(f"Connected to database: {current_db}")
    
    # Close connection
    cursor.close()
    conn.close()
    print("Connection closed successfully.")
    
except psycopg2.Error as e:
    print(f"❌ Connection failed!")
    print(f"Error: {e}")
    print("\nTroubleshooting:")
    print("1. Check if PostgreSQL service is running")
    print("2. Verify your password is correct")
    print("3. Ensure database 'olist_dw' exists")
    print("4. Check if port 5432 is correct")

except Exception as e:
    print(f"❌ Unexpected error: {e}")
