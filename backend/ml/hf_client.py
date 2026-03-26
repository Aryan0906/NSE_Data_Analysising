import json
import logging
import time
from datetime import date
from typing import Any, Dict, List, Optional

import requests
from sqlalchemy import create_engine, text

from backend.pipeline.settings import settings

logger = logging.getLogger(__name__)

class HFApiBudgetExhausted(Exception):
    """Raised when the daily HuggingFace API budget (Rule 1) is exhausted."""
    pass

class HFClient:
    """
    HuggingFace Inference API client strictly enforcing Rule 1:
      - Max calls/day tracked in PostgreSQL `hf_api_budget`
      - 1 second sleep between calls
      - Exponential backoff on 503
      - Raises HFApiBudgetExhausted on budget limit
    """
    
    def __init__(self, db_engine=None):
        self.api_token = settings.hf_api_token
        self.daily_limit = settings.hf_daily_call_limit
        self.db_engine = db_engine or create_engine(settings.postgres_dsn, pool_pre_ping=True)
        self._ensure_budget_table()
        
    def _ensure_budget_table(self):
        """Creates the hf_api_budget table if it doesn't exist."""
        with self.db_engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS hf_api_budget (
                    budget_date DATE PRIMARY KEY,
                    calls_made INT NOT NULL DEFAULT 0
                );
            """))
            
    def _check_and_increment_budget(self) -> None:
        """
        Atomically checks and increments the daily budget.
        Raises HFApiBudgetExhausted if the limit is reached.
        """
        today = date.today()
        
        with self.db_engine.begin() as conn:
            # Ensure row exists
            conn.execute(text("""
                INSERT INTO hf_api_budget (budget_date, calls_made)
                VALUES (:today, 0)
                ON CONFLICT (budget_date) DO NOTHING;
            """), {"today": today})
            
            # Fetch current calls (locking the row)
            result = conn.execute(text("""
                SELECT calls_made FROM hf_api_budget
                WHERE budget_date = :today FOR UPDATE;
            """), {"today": today}).fetchone()
            
            if not result:
                raise RuntimeError("Failed to lock hf_api_budget row.")
                
            calls_made = result[0]
            
            if calls_made >= self.daily_limit:
                logger.error(f"HF API budget exhausted. {calls_made}/{self.daily_limit} calls made today.")
                raise HFApiBudgetExhausted(f"Daily budget of {self.daily_limit} calls exhausted.")
                
            # Increment
            conn.execute(text("""
                UPDATE hf_api_budget 
                SET calls_made = calls_made + 1 
                WHERE budget_date = :today;
            """), {"today": today})

    def query(self, model: str, payload: Dict[str, Any], max_retries=3) -> Any:
        """
        Queries HF Inference API, enforcing budget, rate limits, and retries.
        """
        if not self.api_token:
            logger.warning("HF_API_TOKEN not set! Skipping real API call.")
            return None
            
        self._check_and_increment_budget()
        
        # Rule 1: 1 second sleep between calls
        time.sleep(1.0)
        
        headers = {"Authorization": f"Bearer {self.api_token}"}
        api_url = f"https://api-inference.huggingface.co/models/{model}"
        
        backoff = 2
        for attempt in range(1, max_retries + 1):
            try:
                response = requests.post(api_url, headers=headers, json=payload, timeout=30)
                
                # Rule 1: Exponential backoff on 503
                if response.status_code == 503:
                    logger.warning(f"HF API 503 (Model loading). Attempt {attempt}/{max_retries}. Sleeping {backoff}s...")
                    time.sleep(backoff)
                    backoff *= 2
                    continue
                    
                response.raise_for_status()
                return response.json()
                
            except requests.exceptions.HTTPError as e:
                logger.error(f"HF API HTTP Error: {str(e)} - {response.text}")
                if attempt == max_retries:
                    raise
            except Exception as e:
                logger.error(f"HF API Error: {str(e)}")
                if attempt == max_retries:
                    raise
                    
        raise RuntimeError(f"HF API model {model} failed after {max_retries} attempts.")
