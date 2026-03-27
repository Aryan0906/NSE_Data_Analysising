"""
backend/pipeline/load.py
==========================
Task 5 of the Airflow DAG — materialises gold.* views / tables from
silver.* data, and (optionally) indexes news embeddings into ChromaDB.

Rules enforced:
  Rule 2 : ChromaDB is used for news headline embeddings (this module
           handles the upsert; sentence-transformers runs locally).
  Rule 5 : Load is the FINAL task; it reads silver.* and writes gold.*.
  Rule 8 : Gold tables are readable by nse_reader (grants applied in db_init).

Gold tables created / refreshed here:
  gold.stock_summary    — latest snapshot per symbol (price + fundamentals)
  gold.news_feed        — latest 100 enriched news per symbol

ChromaDB collection: "nse_news_headlines" (one doc per content_hash).
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from datetime import date
from typing import Generator, Optional

import psycopg2
import psycopg2.extras

from backend.pipeline.settings import settings

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=settings.log_level,
    format="%(asctime)s | %(levelname)s | load | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)

# ─────────────────────────────────────────────────
# Connection helpers
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
# Gold: stock_summary
# ─────────────────────────────────────────────────

_GOLD_SUMMARY_UPSERT = """
INSERT INTO gold.stock_summary (
    symbol, as_of_date,
    close_price, daily_return, normalised_close,
    sma_20, sma_50, rsi_14,
    market_cap, trailing_pe, price_to_book,
    debt_to_equity, return_on_equity,
    dividend_yield, fifty_two_week_high, fifty_two_week_low
)
SELECT
    p.symbol,
    p.trade_date                                AS as_of_date,
    p.close_price,
    p.daily_return,
    p.normalised_close,
    p.sma_20,
    p.sma_50,
    p.rsi_14,
    MAX(CASE WHEN f.metric_name = 'marketCap'       THEN f.metric_value END) AS market_cap,
    MAX(CASE WHEN f.metric_name = 'trailingPE'      THEN f.metric_value END) AS trailing_pe,
    MAX(CASE WHEN f.metric_name = 'priceToBook'     THEN f.metric_value END) AS price_to_book,
    MAX(CASE WHEN f.metric_name = 'debtToEquity'    THEN f.metric_value END) AS debt_to_equity,
    MAX(CASE WHEN f.metric_name = 'returnOnEquity'  THEN f.metric_value END) AS return_on_equity,
    MAX(CASE WHEN f.metric_name = 'dividendYield'   THEN f.metric_value END) AS dividend_yield,
    MAX(CASE WHEN f.metric_name = 'fiftyTwoWeekHigh' THEN f.metric_value END) AS fifty_two_week_high,
    MAX(CASE WHEN f.metric_name = 'fiftyTwoWeekLow'  THEN f.metric_value END) AS fifty_two_week_low
FROM (
    SELECT DISTINCT ON (symbol) *
    FROM silver.prices
    ORDER BY symbol, trade_date DESC
) p
LEFT JOIN silver.fundamentals f
    ON f.symbol = p.symbol
WHERE p.symbol = ANY(%s)
GROUP BY
    p.symbol, p.trade_date, p.close_price, p.daily_return,
    p.normalised_close, p.sma_20, p.sma_50, p.rsi_14
ON CONFLICT (symbol) DO UPDATE SET
    as_of_date         = EXCLUDED.as_of_date,
    close_price        = EXCLUDED.close_price,
    daily_return       = EXCLUDED.daily_return,
    normalised_close   = EXCLUDED.normalised_close,
    sma_20             = EXCLUDED.sma_20,
    sma_50             = EXCLUDED.sma_50,
    rsi_14             = EXCLUDED.rsi_14,
    market_cap         = EXCLUDED.market_cap,
    trailing_pe        = EXCLUDED.trailing_pe,
    price_to_book      = EXCLUDED.price_to_book,
    debt_to_equity     = EXCLUDED.debt_to_equity,
    return_on_equity   = EXCLUDED.return_on_equity,
    dividend_yield     = EXCLUDED.dividend_yield,
    fifty_two_week_high = EXCLUDED.fifty_two_week_high,
    fifty_two_week_low  = EXCLUDED.fifty_two_week_low,
    updated_at         = NOW();
"""


def load_stock_summary(symbols: list[str]) -> int:
    """
    Materialise the gold.stock_summary table for *symbols*.
    Returns rows written.
    """
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(_GOLD_SUMMARY_UPSERT, (symbols,))
            written = cur.rowcount
        conn.commit()
    logger.info("Gold stock_summary — %d rows upserted", written)
    return written


# ─────────────────────────────────────────────────
# Gold: news_feed
# ─────────────────────────────────────────────────

_GOLD_NEWS_UPSERT = """
INSERT INTO gold.news_feed
    (content_hash, symbol, headline, source, published_at,
     url, summary, sentiment_score)
SELECT
    content_hash, symbol, headline, source, published_at,
    url, summary, sentiment_score
FROM silver.news
WHERE symbol = ANY(%s)
  AND published_at IS NOT NULL
ORDER BY published_at DESC
LIMIT 10000
ON CONFLICT (content_hash) DO UPDATE SET
    summary         = EXCLUDED.summary,
    sentiment_score = EXCLUDED.sentiment_score,
    updated_at      = NOW();
"""


def load_news_feed(symbols: list[str]) -> int:
    """Materialise gold.news_feed for *symbols*.  Returns rows written."""
    # Strip .NS suffix to match how news is stored in silver
    base_symbols = [s.replace(".NS", "").upper() for s in symbols]
    
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(_GOLD_NEWS_UPSERT, (base_symbols,))
            written = cur.rowcount
        conn.commit()
    logger.info("Gold news_feed — %d rows upserted", written)
    return written


# ─────────────────────────────────────────────────
# ChromaDB embedding upsert (Rule 2)
# ─────────────────────────────────────────────────

def _chroma_client():
    """Lazy import — chromadb is optional; fail gracefully if absent."""
    try:
        import chromadb  # noqa: F401
        client = chromadb.HttpClient(
            host=settings.chromadb_host,
            port=settings.chromadb_port,
        )
        return client
    except Exception as exc:  # pragma: no cover
        logger.warning("ChromaDB not available: %s", exc)
        return None


def load_news_embeddings(symbols: list[str], batch_size: int = 50) -> int:
    """
    Rule 2: Upsert news headlines as embeddings in ChromaDB.
    Uses sentence-transformers (all-MiniLM-L6-v2) locally — no HF API call.
    Returns number of documents upserted.
    """
    client = _chroma_client()
    if client is None:
        logger.warning("ChromaDB unavailable — skipping embedding upsert.")
        return 0

    try:
        from sentence_transformers import SentenceTransformer  # local model
        model = SentenceTransformer("all-MiniLM-L6-v2")
    except ImportError:
        logger.warning("sentence-transformers not installed — skipping embeddings.")
        return 0

    collection = client.get_or_create_collection(
        name="nse_news_headlines",
        metadata={"hnsw:space": "cosine"},
    )

    # Fetch un-embedded silver news rows
    with _get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT content_hash, symbol, headline, published_at,
                       sentiment_score
                FROM silver.news
                WHERE symbol = ANY(%s)
                  AND headline IS NOT NULL
                  AND embedding_updated_at IS NULL
                ORDER BY published_at DESC
                LIMIT 500
                """,
                (symbols,),
            )
            rows = cur.fetchall()

    if not rows:
        logger.info("No un-embedded news rows.")
        return 0

    total_upserted = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i: i + batch_size]
        ids = [r["content_hash"] for r in batch]
        texts = [r["headline"] for r in batch]
        metadatas = [
            {
                "symbol": r["symbol"],
                "published_at": str(r["published_at"]) if r["published_at"] else "",
                "sentiment_score": float(r["sentiment_score"])
                if r["sentiment_score"] is not None else 0.0,
            }
            for r in batch
        ]

        embeddings = model.encode(texts, show_progress_bar=False).tolist()

        collection.upsert(
            ids=ids,
            documents=texts,
            metadatas=metadatas,
            embeddings=embeddings,
        )
        total_upserted += len(batch)

        # Mark as embedded in silver
        with _get_conn() as conn:
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur,
                    """
                    UPDATE silver.news
                       SET embedding_updated_at = NOW()
                     WHERE content_hash = %s
                    """,
                    [(r["content_hash"],) for r in batch],
                )
            conn.commit()

    logger.info("Upserted %d embeddings into ChromaDB", total_upserted)
    return total_upserted


# ─────────────────────────────────────────────────
# Airflow-callable entry-point
# ─────────────────────────────────────────────────

def run(
    symbols: Optional[list[str]] = None,
    run_date: Optional[date] = None,
) -> dict[str, int]:
    """Called by the Airflow PythonOperator."""
    symbols = symbols or settings.nse_symbols
    run_date = run_date or date.today()

    result = {
        "gold_stock_summary": load_stock_summary(symbols),
        "gold_news_feed": load_news_feed(symbols),
        "chroma_embeddings": load_news_embeddings(symbols),
    }
    logger.info("Load complete: %s", result)
    return result


if __name__ == "__main__":
    print(run())
