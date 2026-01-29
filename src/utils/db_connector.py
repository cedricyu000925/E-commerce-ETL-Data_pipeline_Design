"""
Database connection utility for PostgreSQL
Handles connection pooling and transaction management
"""

import psycopg2
from psycopg2 import pool
from sqlalchemy import create_engine
import yaml
import logging
from contextlib import contextmanager

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DatabaseConnector:
    """Manages PostgreSQL database connections"""
    
    def __init__(self, config_path='config/database.yaml'):
        """Initialize database connector with configuration"""
        # Load database configuration
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        self.db_config = config['database']
        self.connection_pool = None
        self.engine = None
        
        # Create connection pool
        self._create_pool()
        
        # Create SQLAlchemy engine (for pandas to_sql)
        self._create_engine()
    
    def _create_pool(self):
        """Create PostgreSQL connection pool"""
        try:
            self.connection_pool = psycopg2.pool.SimpleConnectionPool(
                minconn=1,
                maxconn=10,
                host=self.db_config['host'],
                port=self.db_config['port'],
                database=self.db_config['database'],
                user=self.db_config['user'],
                password=self.db_config['password']
            )
            logger.info("✓ Database connection pool created successfully")
        except Exception as e:
            logger.error(f"✗ Failed to create connection pool: {e}")
            raise
    
    def _create_engine(self):
        """Create SQLAlchemy engine for pandas integration"""
        connection_string = (
            f"postgresql://{self.db_config['user']}:"
            f"{self.db_config['password']}@"
            f"{self.db_config['host']}:"
            f"{self.db_config['port']}/"
            f"{self.db_config['database']}"
        )
        self.engine = create_engine(connection_string)
        logger.info("✓ SQLAlchemy engine created successfully")
    
    @contextmanager
    def get_connection(self):
        """Context manager for getting database connection"""
        conn = self.connection_pool.getconn()
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Transaction rolled back: {e}")
            raise
        finally:
            self.connection_pool.putconn(conn)
    
    def get_engine(self):
        """Return SQLAlchemy engine (for pandas)"""
        return self.engine
    
    def execute_query(self, query, params=None):
        """Execute a SQL query"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                logger.info(f"✓ Query executed: {query[:50]}...")
    
    def fetch_query(self, query, params=None):
        """Execute query and fetch results"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                results = cur.fetchall()
                return results
    
    def close_pool(self):
        """Close all connections in pool"""
        if self.connection_pool:
            self.connection_pool.closeall()
            logger.info("✓ Connection pool closed")


# Test the connection
if __name__ == "__main__":
    try:
        db = DatabaseConnector()
        result = db.fetch_query("SELECT version();")
        print(f"PostgreSQL version: {result[0][0]}")
        db.close_pool()
    except Exception as e:
        print(f"Connection test failed: {e}")
