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
