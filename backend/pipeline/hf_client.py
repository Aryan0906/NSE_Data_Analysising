"""
backend/pipeline/hf_client.py
===============================
Thin wrapper around the HuggingFace Inference API that enforces the
≤80 calls/day rate limit (Rule 1) using a PostgreSQL-backed counter.

Every call to summarise() or get_sentiment() first checks the daily budget.
If the budget is exhausted, it throws a RuntimeError.

Rate-limit state is stored in public.hf_api_calls:
    date_key DATE PRIMARY KEY, call_count INT DEFAULT 0
The table is created here on first use (idempotent).
"""

from __future__ import annotations

import logging
import time
import json
from datetime import date
from typing import Optional

import psycopg2
import psycopg2.extras
from huggingface_hub import InferenceClient

from backend.pipeline.settings import settings

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=settings.log_level,
    format="%(asctime)s | %(levelname)s | hf_client | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)

# ─────────────────────────────────────────────────
# Rate-limit table (created once, idempotent)
# ─────────────────────────────────────────────────

_CREATE_RATE_TABLE = """
CREATE TABLE IF NOT EXISTS public.hf_api_calls (
    date_key    DATE  PRIMARY KEY,
    call_count  INT   NOT NULL DEFAULT 0
);
"""


def _ensure_rate_table() -> None:
    with psycopg2.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        dbname=settings.postgres_db,
        user=settings.postgres_user,
        password=settings.postgres_password,
    ) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(_CREATE_RATE_TABLE)


_ensure_rate_table()


# ─────────────────────────────────────────────────
# Rate-limit helpers
# ─────────────────────────────────────────────────

def _get_daily_count(today: date) -> int:
    with psycopg2.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        dbname=settings.postgres_db,
        user=settings.postgres_user,
        password=settings.postgres_password,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT call_count FROM public.hf_api_calls WHERE date_key = %s",
                (today,),
            )
            row = cur.fetchone()
    return row[0] if row else 0


def _increment_daily_count(today: date) -> None:
    with psycopg2.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        dbname=settings.postgres_db,
        user=settings.postgres_user,
        password=settings.postgres_password,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO public.hf_api_calls (date_key, call_count)
                VALUES (%s, 1)
                ON CONFLICT (date_key)
                DO UPDATE SET call_count = hf_api_calls.call_count + 1;
                """,
                (today,),
            )
        conn.commit()


def _budget_check() -> None:
    """Check daily budget and raise Exception if limit reached (Rule 1)."""
    if not settings.hf_api_token:
        logger.warning("HF_API_TOKEN not set — HF calls might fail.")
        
    today = date.today()
    count = _get_daily_count(today)
    if count >= settings.hf_daily_call_limit:
        logger.error(
            "HF daily call limit reached (%d/%d) — raising Exception.",
            count, settings.hf_daily_call_limit,
        )
        raise RuntimeError(f"Rule 1 Violation: HF daily API budget exhausted ({count}/{settings.hf_daily_call_limit})")


# ─────────────────────────────────────────────────
# Inference helpers
# ─────────────────────────────────────────────────

def _get_client() -> InferenceClient:
    return InferenceClient(
        provider="hf-inference",
        api_key=settings.hf_api_token,
    )

def _execute_with_retry(func, *args, **kwargs):
    """Executes a function obeying Rule 1 (1s sleep, exponential backoff on 503)."""
    max_retries = 3
    base_delay = 2
    
    for attempt in range(max_retries + 1):
        try:
            # Rule 1: 1 second sleep between calls
            time.sleep(1)
            result = func(*args, **kwargs)
            _increment_daily_count(date.today())
            return result
        except Exception as e:
            # Detect 503 errors (typically embedded in the error string or as HTTPError)
            error_str = str(e)
            is_503 = (
                "503 Service Unavailable" in error_str or 
                "503" in error_str or 
                "Model is currently loading" in error_str
            )
            
            if is_503 and attempt < max_retries:
                delay = base_delay ** attempt
                logger.warning(f"HF API 503 Service Unavailable. Retrying in {delay}s... (Attempt {attempt+1}/{max_retries})")
                time.sleep(delay)
            else:
                logger.error(f"HF inference error: {e}")
                raise


# ─────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────

class HFClient:
    """Class-based wrapper around HF API for compatibility with ML modules."""
    def __init__(self, db_engine=None):
        self.db_engine = db_engine

    def query(self, model: str, payload: dict) -> Optional[dict | list]:
        """Legacy payload poster for RAG"""
        _budget_check()
        client = _get_client()
        result_bytes = _execute_with_retry(client.post, json=payload, model=model)
        try:
            return json.loads(result_bytes.decode('utf-8'))
        except BaseException:
            return None
            
    def summarization(self, text: str, model: str) -> str:
        """Execute inference using native huggingface_hub client."""
        _budget_check()
        client = _get_client()
        
        # Native Hugging Face inference helper
        def _call():
            return client.summarization(text, model=model)
            
        result = _execute_with_retry(_call)
        
        if isinstance(result, str):
            return result
        elif isinstance(result, list) and len(result) > 0 and isinstance(result[0], dict) and "summary_text" in result[0]:
            return result[0]["summary_text"]
        elif hasattr(result, "summary_text"):
            return result.summary_text
        return str(result)


# ─────────────────────────────────────────────────
# Standalone convenience functions (used by transform.py)
# ─────────────────────────────────────────────────

def summarise(text: str, max_length: int = 130, min_length: int = 30) -> Optional[str]:
    """
    Summarise *text* using BART (facebook/bart-large-cnn).
    Returns the summary string, or None if the budget is exhausted / error.
    Maps to Rule 1: ≤80 HF calls/day.
    """
    try:
        _budget_check()
    except RuntimeError:
        return None

    client = _get_client()

    def _call():
        return client.summarization(
            text[:1024],
            model=settings.hf_summarisation_model,
        )

    try:
        result = _execute_with_retry(_call)
        if isinstance(result, str):
            return result
        elif hasattr(result, "summary_text"):
            return result.summary_text
        elif isinstance(result, list) and result:
            return result[0].get("summary_text")
        return str(result)
    except Exception as exc:
        logger.error("Summarise error: %s", exc)
        return None


def get_sentiment(text: str) -> Optional[float]:
    """
    Run FinBERT (ProsusAI/finbert) on *text*.
    Returns a float in [-1, 1]:  positive → +score, negative → -score.
    Returns None if the budget is exhausted / error.
    """
    try:
        _budget_check()
    except RuntimeError:
        return None

    client = _get_client()

    def _call():
        return client.post(json={"inputs": text[:512]}, model=settings.hf_sentiment_model)

    try:
        result_bytes = _execute_with_retry(_call)
        result = json.loads(result_bytes.decode("utf-8"))
    except Exception as exc:
        logger.error("Sentiment error: %s", exc)
        return None

    # FinBERT returns: [[{"label": "positive"|"negative"|"neutral", "score": float}]]
    if not isinstance(result, list) or not result:
        return None

    scores: list[dict] = result[0] if isinstance(result[0], list) else result
    label_map = {"positive": 1.0, "negative": -1.0, "neutral": 0.0}

    best = max(scores, key=lambda x: x.get("score", 0))
    label = best.get("label", "neutral").lower()
    score = best.get("score", 0.0)
    return round(label_map.get(label, 0.0) * score, 4)
