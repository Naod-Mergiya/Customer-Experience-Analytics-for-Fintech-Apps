import pandas as pd
import pyodbc
import psycopg2
from sqlalchemy import create_engine



def write_unique_banks_to_postgres(df: pd.DataFrame, engine, table_name: str = 'banks'):
    """
    Extracts the unique 'bank' values from the DataFrame, renames the column to 'name',
    and writes the result to a PostgreSQL table.

    Parameters:
    - df: pd.DataFrame - The source DataFrame containing a 'bank' column.
    - engine: SQLAlchemy engine - The SQLAlchemy engine connected to PostgreSQL.
    - table_name: str - The name of the destination table in PostgreSQL. Defaults to 'banks'.
    """
    if 'bank' not in df.columns:
        raise ValueError("The DataFrame does not contain a 'bank' column.")

    banks_df = df[['bank']].rename(columns={'bank': 'name'}).drop_duplicates(subset=['name'])
    banks_df.to_sql(table_name, engine, if_exists='replace', index=False)

def insert_reviews(df: pd.DataFrame, engine, table_name: str = 'reviews'):
    """
    Inserts review data from a DataFrame into a PostgreSQL table.

    Extracts the 'bank', 'review', 'rating', 'date', 'source', 
    'sentiment_label', and 'sentiment_score' columns from the DataFrame,
    renames 'bank' to 'name' and 'date' to 'review_date', and writes the result
    to the specified PostgreSQL table.

    Parameters:
    - df (pd.DataFrame): The source DataFrame containing the review data.
    - engine (sqlalchemy.engine.Engine): SQLAlchemy engine connected to PostgreSQL.
    - table_name (str): Name of the destination table. Defaults to 'reviews'.

    Raises:
    - ValueError: If the required 'bank' column is not present in the DataFrame.
    """
    required_columns = ['bank', 'review', 'rating', 'date', 'source', 'sentiment_label', 'sentiment_score']
    missing_cols = [col for col in required_columns if col not in df.columns]
    
    if missing_cols:
        raise ValueError(f"The DataFrame is missing the following required column(s): {missing_cols}")

    reviews_df = df[required_columns].rename(columns={
        'bank': 'name',
        'date': 'review_date'
    })

    reviews_df.to_sql(table_name, engine, if_exists='replace', index=False)
