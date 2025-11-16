"""
Database utilities for the Weather + Air Quality Analytics Pipeline.

This module provides database connection management using SQLAlchemy
and helper functions for common database operations.
"""

import logging
from typing import Optional
from contextlib import contextmanager
from sqlalchemy import create_engine, text, Engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError
import psycopg2
from psycopg2.extras import RealDictCursor

from src.config import config

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global SQLAlchemy engine
_engine: Optional[Engine] = None
_SessionLocal: Optional[sessionmaker] = None


def get_engine() -> Engine:
    """Get or create the SQLAlchemy engine."""
    global _engine
    if _engine is None:
        database_url = config.database_url
        _engine = create_engine(
            database_url,
            echo=False,  # Set to True for SQL query logging
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
            pool_recycle=3600,
        )
        logger.info(f"Created database engine for {config.DB_HOST}:{config.DB_PORT}/{config.DB_NAME}")
    return _engine


def get_session_maker() -> sessionmaker:
    """Get or create the SQLAlchemy session maker."""
    global _SessionLocal
    if _SessionLocal is None:
        engine = get_engine()
        _SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)
    return _SessionLocal


@contextmanager
def get_db_session():
    """Context manager for database sessions."""
    SessionLocal = get_session_maker()
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        logger.error(f"Database session error: {e}")
        raise
    finally:
        session.close()


def get_raw_connection():
    """Get a raw psycopg2 connection for direct SQL operations."""
    try:
        connection = psycopg2.connect(
            host=config.DB_HOST,
            port=config.DB_PORT,
            database=config.DB_NAME,
            user=config.DB_USER,
            password=config.DB_PASSWORD
        )
        return connection
    except psycopg2.Error as e:
        logger.error(f"Failed to connect to database: {e}")
        raise


@contextmanager
def get_raw_db_connection():
    """Context manager for raw database connections."""
    connection = None
    try:
        connection = get_raw_connection()
        yield connection
        connection.commit()
    except Exception as e:
        if connection:
            connection.rollback()
        logger.error(f"Database connection error: {e}")
        raise
    finally:
        if connection:
            connection.close()


def test_connection() -> bool:
    """Test the database connection."""
    try:
        with get_db_session() as session:
            result = session.execute(text("SELECT 1 as test"))
            test_value = result.scalar()
            if test_value == 1:
                logger.info("✅ Database connection successful")
                return True
            else:
                logger.error("❌ Database connection test failed")
                return False
    except Exception as e:
        logger.error(f"❌ Database connection failed: {e}")
        return False


def init_db() -> None:
    """Initialize the database with required schemas and tables."""
    logger.info("Initializing database...")
    
    try:
        with get_raw_db_connection() as conn:
            with conn.cursor() as cursor:
                # Create schemas
                schemas = ['raw', 'staging', 'marts']
                for schema in schemas:
                    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
                    logger.info(f"Created schema: {schema}")
                
                # Create the main raw table if it doesn't exist
                create_table_sql = """
                CREATE TABLE IF NOT EXISTS raw.weather_observations (
                    id SERIAL PRIMARY KEY,
                    city VARCHAR(100) NOT NULL,
                    country VARCHAR(10),
                    latitude DECIMAL(10, 8),
                    longitude DECIMAL(11, 8),
                    observation_time TIMESTAMP WITH TIME ZONE NOT NULL,
                    weather_data JSONB NOT NULL,
                    air_quality_data JSONB,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                );
                """
                cursor.execute(create_table_sql)
                
                # Create indexes
                indexes = [
                    "CREATE INDEX IF NOT EXISTS idx_weather_city_time ON raw.weather_observations(city, observation_time)",
                    "CREATE INDEX IF NOT EXISTS idx_weather_observation_time ON raw.weather_observations(observation_time)",
                    "CREATE INDEX IF NOT EXISTS idx_weather_created_at ON raw.weather_observations(created_at)"
                ]
                
                for index_sql in indexes:
                    cursor.execute(index_sql)
                
                # Create update trigger function
                trigger_function_sql = """
                CREATE OR REPLACE FUNCTION update_updated_at_column()
                RETURNS TRIGGER AS $$
                BEGIN
                    NEW.updated_at = CURRENT_TIMESTAMP;
                    RETURN NEW;
                END;
                $$ language 'plpgsql';
                """
                cursor.execute(trigger_function_sql)
                
                # Create trigger
                trigger_sql = """
                DROP TRIGGER IF EXISTS update_weather_observations_updated_at ON raw.weather_observations;
                CREATE TRIGGER update_weather_observations_updated_at 
                    BEFORE UPDATE ON raw.weather_observations 
                    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
                """
                cursor.execute(trigger_sql)
                
                logger.info("✅ Database initialized successfully")
                
    except Exception as e:
        logger.error(f"❌ Failed to initialize database: {e}")
        raise


def execute_sql(sql: str, params: Optional[dict] = None) -> list:
    """Execute a SQL query and return results."""
    try:
        with get_raw_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(sql, params or {})
                if cursor.description:  # SELECT query
                    return cursor.fetchall()
                else:  # INSERT/UPDATE/DELETE
                    return []
    except Exception as e:
        logger.error(f"SQL execution error: {e}")
        raise


def get_table_info(schema: str, table: str) -> dict:
    """Get information about a table."""
    sql = """
    SELECT column_name, data_type, is_nullable, column_default
    FROM information_schema.columns
    WHERE table_schema = %(schema)s AND table_name = %(table)s
    ORDER BY ordinal_position;
    """
    
    try:
        columns = execute_sql(sql, {"schema": schema, "table": table})
        return {
            "schema": schema,
            "table": table,
            "columns": [dict(col) for col in columns]
        }
    except Exception as e:
        logger.error(f"Failed to get table info for {schema}.{table}: {e}")
        return {}


if __name__ == "__main__":
    # Test the database connection when run directly
    logger.info("Testing database connection...")
    
    if test_connection():
        logger.info("Testing database initialization...")
        init_db()
        
        # Show table info
        table_info = get_table_info("raw", "weather_observations")
        if table_info:
            logger.info(f"Table info: {table_info['schema']}.{table_info['table']}")
            for col in table_info['columns']:
                logger.info(f"  - {col['column_name']}: {col['data_type']}")
    else:
        logger.error("Database connection failed")
        exit(1)