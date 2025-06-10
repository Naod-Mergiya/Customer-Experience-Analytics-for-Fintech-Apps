# db_util.py
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
import pandas as pd
from typing import Optional

def get_engine(
    username: str = "postgres",
    password: str = "postgres",
    host: str = "localhost",
    port: int = 5432,
    database: str = "bank_reviews"
) -> Engine:
    """Create and return a SQLAlchemy engine for PostgreSQL.
    
    Args:
        username: Database username
        password: Database password
        host: Database host
        port: Database port
        database: Database name
    
    Returns:
        SQLAlchemy Engine instance
    """
    connection_string = f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}"
    return create_engine(connection_string)