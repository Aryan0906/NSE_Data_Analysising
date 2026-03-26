"""
backend/pipeline/hf_client.py
===============================
Thin wrapper around the HuggingFace Inference API that enforces the
≤80 calls/day rate limit (Rule 1) using a PostgreSQL-backed counter.

Every call to summarise() or get_sentiment() first checks the daily budget.
If the budget is exhausted the caller receives None and the pipeline continues
gracefully without the enrichment.

Rate-limit state is stored in public.hf_api_calls:
    date_key DATE PRIMARY KEY, call_count INT DEFAULT 0
The table is created here on first use (idempotent).
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Optional

import psycopg2
import psycopg2.extras
import requests

from backend.pipeline.settings import settings

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=settings.log_level,
    format="%(asctime)s | %(levelname)s | hf_client | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)

_HF_INFERENCE_BASE = "https://api-inference.huggingface.co/models"

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


def _budget_check() -> bool:
    """Return True if a call may proceed, False if daily limit reached (Rule 1)."""
    if not settings.hf_api_token:
        logger.debug("HF_API_TOKEN not set — HF calls disabled.")
        return False

    today = date.today()
    count = _get_daily_count(today)
    if count >= settings.hf_daily_call_limit:
        logger.warning(
            "HF daily call limit reached (%d/%d) — skipping.",
            count, settings.hf_daily_call_limit,
        )
        return False
    return True


# ─────────────────────────────────────────────────
# Inference helpers
# ─────────────────────────────────────────────────

def _post_inference(model: str, payload: dict) -> Optional[dict | list]:
    """POST to HF Inference API; returns parsed JSON or None on error."""
    url = f"{_HF_INFERENCE_BASE}/{model}"
    headers = {"Authorization": f"Bearer {settings.hf_api_token}"}

    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=30)
        resp.raise_for_status()
        _increment_daily_count(date.today())
        return resp.json()
    except requests.RequestException as exc:
        logger.error("HF inference error [%s]: %s", model, exc)
        return None


# ─────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────

def summarise(text: str, max_length: int = 130, min_length: int = 30) -> Optional[str]:
    """
    Summarise *text* using BART (facebook/bart-large-cnn).
    Returns the summary string, or None if the budget is exhausted / error.
    Maps to Rule 1: ≤80 HF calls/day.
    """
    if not _budget_check():
        return None

    result = _post_inference(
        settings.hf_summarisation_model,
        {
            "inputs": text[:1024],   # BART max input tokens ~ 1024
            "parameters": {
                "max_length": max_length,
                "min_length": min_length,
                "do_sample": False,
            },
        },
    )
    if isinstance(result, list) and result:
        return result[0].get("summary_text")
    return None


def get_sentiment(text: str) -> Optional[float]:
    """
    Run FinBERT (ProsusAI/finbert) on *text*.
    Returns a float in [-1, 1]:  positive → +score, negative → -score.
    Returns None if the budget is exhausted / error.
    """
    if not _budget_check():
        return None

    result = _post_inference(
        settings.hf_sentiment_model,
        {"inputs": text[:512]},  # BERT max input
    )

    # FinBERT returns: [[{"label": "positive"|"negative"|"neutral", "score": float}]]
    if not isinstance(result, list) or not result:
        return None

    scores: list[dict] = result[0] if isinstance(result[0], list) else result
    label_map = {"positive": 1.0, "negative": -1.0, "neutral": 0.0}

    best = max(scores, key=lambda x: x.get("score", 0))
    label = best.get("label", "neutral").lower()
    score = best.get("score", 0.0)
    return round(label_map.get(label, 0.0) * score, 4)
