import pytest
from backend.ml.sql_guard import guard_sql, SQLInjectionError

def test_sql_guard_valid_select():
    """Valid SELECT queries pass through and are capped sequentially."""
    query = "SELECT ticker, date, close_px FROM gold.fact_prices"
    safe_query = guard_sql(query)
    
    assert safe_query.startswith("SELECT")
    assert "LIMIT 1000;" in safe_query


def test_sql_guard_blocks_destructive_keywords():
    """Rule 8: DROP, DELETE, TRUNCATE shouldn't bypass the check."""
    malicious_queries = [
        "SELECT * FROM prices; DROP TABLE users;",
        "DELETE FROM hf_api_budget;",
        "TRUNCATE TABLE silver.news;"
    ]
    
    for bad_query in malicious_queries:
        with pytest.raises(SQLInjectionError):
            guard_sql(bad_query)


def test_sql_guard_enforces_select_prefix():
    """Rule 8: Enforces that strings must explicitly start with SELECT."""
    bad_query = "UPDATE backend SET value = 1"
    
    with pytest.raises(SQLInjectionError, match="Only SELECT queries are allowed."):
        guard_sql(bad_query)
