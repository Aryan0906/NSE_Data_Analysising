import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

from backend.pipeline.settings import settings
from backend.ml.sql_guard import guard_sql

@st.cache_resource
def get_readonly_engine():
    """
    Creates a SQLAlchemy engine using the READONLY user credentials.
    Enforces the rule that the frontend UI cannot mutate data.
    """
    # Build readonly DSN
    readonly_dsn = (
        f"postgresql://{settings.postgres_readonly_user}:{settings.postgres_readonly_password}"
        f"@{settings.postgres_host}:{settings.postgres_port}/{settings.postgres_db}"
    )
    return create_engine(readonly_dsn, pool_pre_ping=True)

def fetch_data(query: str, params: dict = None) -> pd.DataFrame:
    """
    Fetches data safely into a pandas DataFrame.
    Enforces Rule 8 by passing the query through `guard_sql`.
    """
    engine = get_readonly_engine()
    
    # Rule 8: SQL Injection Guard
    safe_query = guard_sql(query)
    
    with engine.connect() as conn:
        result = conn.execute(text(safe_query), params or {})
        rows = result.fetchall()
        columns = list(result.keys())
    
    return pd.DataFrame(rows, columns=columns)

def get_tickers():
    """Get active tickers from dim_companies."""
    query = """
        SELECT symbol 
        FROM gold.dim_companies 
        WHERE is_current = TRUE
        ORDER BY symbol
    """
    df = fetch_data(query)
    return df['symbol'].tolist() if not df.empty else []
