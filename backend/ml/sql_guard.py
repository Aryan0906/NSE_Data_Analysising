import logging
import re
from typing import Optional

logger = logging.getLogger(__name__)

class SQLInjectionError(ValueError):
    pass

def guard_sql(query: str) -> str:
    """
    Enforces Rule 8 (SQL Injection Guard).
    - Strips all non-SELECT statements.
    - Blocks drop, delete, truncate, insert, update, alter, create, --.
    - Injects LIMIT 1000 if missing.
    """
    query_upper = query.upper()
    
    # 1. Block destructive commands
    forbidden_keywords = [
        "DROP", "DELETE", "TRUNCATE", "INSERT", "UPDATE", "ALTER", "CREATE", "--"
    ]
    
    for word in forbidden_keywords:
        # Check for whole-word existence
        if re.search(rf"\b{word}\b", query_upper) or (word == "--" and "--" in query):
            logger.error(f"Rule 8 Violation: Blocked query containing '{word}'. Query: {query}")
            raise SQLInjectionError(f"Forbidden keyword '{word}' in query.")
            
    # 2. Enforce SELECT only
    if not query_upper.strip().startswith("SELECT"):
        logger.error(f"Rule 8 Violation: Query must start with SELECT. Query: {query}")
        raise SQLInjectionError("Only SELECT queries are allowed.")
        
    # 3. Inject LIMIT 1000 if missing
    if "LIMIT" not in query_upper:
        query = query.rstrip().rstrip(";") + " LIMIT 1000;"
    elif not query.rstrip().endswith(";"):
        query += ";"
        
    return query
