# data_ingestion.py

import pandas as pd
from sqlalchemy import create_engine, text

def create_db_engine(db_path):
    """Create a SQLAlchemy engine given the SQLite database path."""
    engine = create_engine(f"sqlite:///{db_path}")
    return engine

def query_data(engine, sql_query):
    """Run a SQL query and return a Pandas DataFrame."""
    with engine.connect() as connection:
        df = pd.read_sql_query(text(sql_query), connection)
    return df

def read_from_web_CSV(url):
    """Read a CSV file from a URL and return a Pandas DataFrame."""
    df = pd.read_csv(url)
    return df
