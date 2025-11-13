"""
Database utility functions for data pipeline
"""

import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
from sqlalchemy import create_engine
import logging

logger = logging.getLogger(__name__)


def get_postgres_connection(config):
    """Create and return PostgreSQL connection"""
    try:
        conn = psycopg2.connect(
            host=config['host'],
            port=config['port'],
            database=config['database'],
            user=config['user'],
            password=config['password']
        )
        logger.info("PostgreSQL connection established")
        return conn
    except Exception as e:
        logger.error(f"Error connecting to PostgreSQL: {e}")
        raise


def get_sqlalchemy_engine(config):
    """Create and return SQLAlchemy engine"""
    try:
        connection_string = (
            f"postgresql://{config['user']}:{config['password']}@"
            f"{config['host']}:{config['port']}/{config['database']}"
        )
        engine = create_engine(connection_string)
        logger.info("SQLAlchemy engine created")
        return engine
    except Exception as e:
        logger.error(f"Error creating SQLAlchemy engine: {e}")
        raise


def execute_query(conn, query, params=None):
    """
    Execute SQL query
    
    Args:
        conn: PostgreSQL connection
        query: SQL query string
        params: Query parameters (optional)
    
    Returns:
        Query results
    """
    try:
        cursor = conn.cursor()
        cursor.execute(query, params)
        
        if query.strip().upper().startswith('SELECT'):
            results = cursor.fetchall()
            cursor.close()
            return results
        else:
            conn.commit()
            cursor.close()
            return None
    except Exception as e:
        logger.error(f"Error executing query: {e}")
        conn.rollback()
        raise


def load_dataframe_to_postgres(df, table_name, engine, if_exists='replace'):
    """
    Load pandas DataFrame to PostgreSQL
    
    Args:
        df: pandas DataFrame
        table_name: Target table name
        engine: SQLAlchemy engine
        if_exists: 'replace', 'append', or 'fail'
    
    Returns:
        Number of rows loaded
    """
    try:
        df.to_sql(
            table_name,
            engine,
            if_exists=if_exists,
            index=False,
            method='multi',
            chunksize=1000
        )
        logger.info(f"Loaded {len(df)} records to {table_name}")
        return len(df)
    except Exception as e:
        logger.error(f"Error loading to PostgreSQL: {e}")
        raise


def read_query_to_dataframe(query, engine):
    """
    Execute query and return results as DataFrame
    
    Args:
        query: SQL query string
        engine: SQLAlchemy engine
    
    Returns:
        pandas DataFrame
    """
    try:
        df = pd.read_sql(query, engine)
        logger.info(f"Query returned {len(df)} records")
        return df
    except Exception as e:
        logger.error(f"Error reading query: {e}")
        raise


def table_exists(conn, table_name):
    """
    Check if table exists in PostgreSQL
    
    Args:
        conn: PostgreSQL connection
        table_name: Table name to check
    
    Returns:
        Boolean
    """
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = %s
            );
        """, (table_name,))
        exists = cursor.fetchone()[0]
        cursor.close()
        return exists
    except Exception as e:
        logger.error(f"Error checking table existence: {e}")
        raise


def get_table_row_count(conn, table_name):
    """
    Get row count for a table
    
    Args:
        conn: PostgreSQL connection
        table_name: Table name
    
    Returns:
        Integer row count
    """
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
        count = cursor.fetchone()[0]
        cursor.close()
        return count
    except Exception as e:
        logger.error(f"Error getting row count: {e}")
        raise
