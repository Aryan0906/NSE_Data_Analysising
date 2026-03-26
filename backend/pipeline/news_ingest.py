"""
backend/pipeline/news_ingest.py
=================================
Task 3 of the Airflow DAG — fetches NSE stock news via NewsAPI and inserts
deduplicated records into bronze.raw_news.

Rules enforced:
  Rule 6 : SHA-256 content hash used as the primary deduplication key.
           Identical headlines are never re-processed regardless of source.
  Rule 5 : Runs AFTER extract and BEFORE transform.

Design:
  - UPSERT on content_hash (ON CONFLICT DO NOTHING) makes re-runs safe.
  - If NEWS_API_KEY is absent the task logs a warning and exits cleanly
    (pipeline must still succeed for price/fundamentals path).
  - raw_body limited to 5 000 chars to avoid bloating the table.
"""

from __future__ import annotations

import hashlib
import logging
from contextlib import contextmanager
from datetime import date, timedelta
from typing import Generator, Optional

import psycopg2
import psycopg2.extras

try:
    from newsapi import NewsApiClient
    _NEWSAPI_AVAILABLE = True
except ImportError:  # pragma: no cover
    _NEWSAPI_AVAILABLE = False

from backend.pipeline.settings import settings

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=settings.log_level,
    format="%(asctime)s | %(levelname)s | news_ingest | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)

_RAW_BODY_MAX_CHARS = 5_000


# ─────────────────────────────────────────────────
# Connection helper
# ─────────────────────────────────────────────────

@contextmanager
def _get_conn() -> Generator[psycopg2.extensions.connection, None, None]:
    conn = psycopg2.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        dbname=settings.postgres_db,
        user=settings.postgres_user,
        password=settings.postgres_password,
        connect_timeout=10,
    )
    try:
        yield conn
    finally:
        conn.close()


# ─────────────────────────────────────────────────
# Dedup hash (Rule 6)
# ─────────────────────────────────────────────────

def _sha256(text: str) -> str:
    """Return a 64-char hex SHA-256 of *text*."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


# ─────────────────────────────────────────────────
# Insert
# ─────────────────────────────────────────────────

_UPSERT_SQL = """
INSERT INTO bronze.raw_news
    (content_hash, symbol, headline, source, published_at, url, raw_body)
VALUES %s
ON CONFLICT (content_hash) DO NOTHING;
"""


def _insert_articles(
    conn: psycopg2.extensions.connection,
    rows: list[tuple],
) -> int:
    """Bulk-upsert rows into bronze.raw_news.  Returns rows attempted."""
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, _UPSERT_SQL, rows)
    conn.commit()
    return len(rows)


# ─────────────────────────────────────────────────
# NewsAPI fetch
# ─────────────────────────────────────────────────

def _fetch_via_newsapi(
    query: str,
    from_date: date,
    to_date: date,
) -> list[dict]:
    """Return raw article dicts from NewsAPI for a given query."""
    if not settings.news_api_key:
        return []

    client = NewsApiClient(api_key=settings.news_api_key)
    articles = []
    page = 1

    while True:
        response = client.get_everything(
            q=query,
            from_param=from_date.isoformat(),
            to=to_date.isoformat(),
            language="en",
            sort_by="publishedAt",
            page_size=settings.news_page_size,
            page=page,
        )
        batch = response.get("articles", [])
        articles.extend(batch)

        total_results = response.get("totalResults", 0)
        fetched = page * settings.news_page_size
        if not batch or fetched >= total_results or fetched >= 1000:
            break
        page += 1

    return articles


# ─────────────────────────────────────────────────
# Per-symbol ingestion
# ─────────────────────────────────────────────────

def ingest_news_for_symbol(
    symbol: str,
    from_date: date,
    to_date: date,
) -> int:
    """Fetch and persist news articles for a single *symbol*.  Returns row count."""
    # Build a human-readable query (NSE symbols look like "RELIANCE", "TCS")
    base = symbol.replace(".NS", "").upper()
    query = f'"{base}" AND (NSE OR "National Stock Exchange" OR India OR stock)'

    logger.info("Fetching news for %s [%s → %s]", base, from_date, to_date)

    try:
        articles = _fetch_via_newsapi(query, from_date, to_date)
    except Exception as exc:
        logger.warning("NewsAPI error for %s: %s", base, exc)
        return 0

    if not articles:
        logger.info("No news articles returned for %s", base)
        return 0

    rows: list[tuple] = []
    for art in articles:
        headline = (art.get("title") or "").strip()
        if not headline:
            continue

        content_hash = _sha256(headline)
        source_name = (art.get("source") or {}).get("name") or ""
        published_at = art.get("publishedAt")       # ISO 8601 string or None
        url = art.get("url") or ""
        raw_body = (art.get("content") or art.get("description") or "")
        raw_body = raw_body[:_RAW_BODY_MAX_CHARS]

        rows.append((
            content_hash,
            base,           # store clean symbol
            headline,
            source_name,
            published_at,
            url,
            raw_body,
        ))

    if not rows:
        return 0

    with _get_conn() as conn:
        written = _insert_articles(conn, rows)

    logger.info("Upserted %d news rows for %s", written, base)
    return written


# ─────────────────────────────────────────────────
# Airflow-callable entry-point
# ─────────────────────────────────────────────────

def run(
    symbols: Optional[list[str]] = None,
    lookback_days: int = 7,
    run_date: Optional[date] = None,
) -> int:
    """
    Called by the Airflow PythonOperator.
    Returns total news rows upserted (XCom-compatible int).
    """
    if not _NEWSAPI_AVAILABLE:
        logger.error("newsapi-python not installed — news ingestion skipped.")
        return 0

    if not settings.news_api_key:
        logger.warning("NEWS_API_KEY not set — news ingestion skipped.")
        return 0

    symbols = symbols or settings.nse_symbols
    run_date = run_date or date.today()
    to_date = run_date
    from_date = to_date - timedelta(days=lookback_days)

    total = 0
    for symbol in symbols:
        total += ingest_news_for_symbol(symbol, from_date, to_date)

    logger.info("News ingestion complete — %d rows total", total)
    return total


if __name__ == "__main__":
    result = run()
    print(f"News rows upserted: {result}")
