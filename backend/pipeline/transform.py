"""
backend/pipeline/transform.py
================================
Task 4 of the Airflow DAG — reads from bronze.*, applies cleaning /
enrichment, and writes to silver.* (prices, fundamentals, news).

Rules enforced:
  Rule 1 : HF API calls are rate-budgeted via hf_client.py.
  Rule 4 : Returns normalised to [0, 1] using 52-week rolling window.
  Rule 5 : Reads bronze; writes silver; called AFTER extract + news_ingest.
  Rule 7 : Z-score outlier filter on daily returns (|z| > 4 → flagged).

Schema targets:
  silver.prices       — cleaned OHLCV + technical indicators + returns
  silver.fundamentals — typed EAV rows (mirrors bronze but deduplicated)
  silver.news         — deduplicated news with HF summary + sentiment
"""

from __future__ import annotations

import logging
import math
from contextlib import contextmanager
from datetime import date, timedelta
from typing import Generator, Optional

import psycopg2
import psycopg2.extras

from backend.pipeline.hf_client import get_sentiment, summarise
from backend.pipeline.settings import settings

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=settings.log_level,
    format="%(asctime)s | %(levelname)s | transform | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)

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
# Price helpers
# ─────────────────────────────────────────────────

def _calc_sma(closes: list[float], window: int) -> Optional[float]:
    if len(closes) < window:
        return None
    return sum(closes[-window:]) / window


def _calc_ema(closes: list[float], window: int) -> Optional[float]:
    """Exponential moving average via multiplier."""
    if len(closes) < window:
        return None
    k = 2.0 / (window + 1)
    ema = sum(closes[:window]) / window
    for c in closes[window:]:
        ema = c * k + ema * (1 - k)
    return ema


def _calc_rsi(closes: list[float], period: int = 14) -> Optional[float]:
    if len(closes) < period + 1:
        return None
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i - 1]
        (gains if d >= 0 else losses).append(abs(d))
    avg_gain = sum(gains[-period:]) / period if gains else 0.0
    avg_loss = sum(losses[-period:]) / period if losses else 0.0
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100 - 100 / (1 + rs), 4)


def _normalise_52w(value: float, low52: float, high52: float) -> Optional[float]:
    """Rule 4: normalise to [0, 1] via 52-week range."""
    if high52 == low52:
        return None
    return round(max(0.0, min(1.0, (value - low52) / (high52 - low52))), 4)


# ─────────────────────────────────────────────────
# Transform prices
# ─────────────────────────────────────────────────

_SILVER_PRICE_UPSERT = """
INSERT INTO silver.prices (
    symbol, trade_date,
    open_price, high_price, low_price, close_price, adj_close_price, volume,
    daily_return, log_return, is_outlier,
    sma_20, sma_50, sma_200, ema_20,
    rsi_14, normalised_close,
    high_52w, low_52w, price_range_52w
)
VALUES %s
ON CONFLICT (symbol, trade_date) DO UPDATE SET
    daily_return      = EXCLUDED.daily_return,
    log_return        = EXCLUDED.log_return,
    is_outlier        = EXCLUDED.is_outlier,
    sma_20            = EXCLUDED.sma_20,
    sma_50            = EXCLUDED.sma_50,
    sma_200           = EXCLUDED.sma_200,
    ema_20            = EXCLUDED.ema_20,
    rsi_14            = EXCLUDED.rsi_14,
    normalised_close  = EXCLUDED.normalised_close,
    high_52w          = EXCLUDED.high_52w,
    low_52w           = EXCLUDED.low_52w,
    price_range_52w   = EXCLUDED.price_range_52w,
    updated_at        = NOW();
"""


def transform_prices(symbol: str, run_date: Optional[date] = None) -> int:
    """
    Read ALL bronze price rows for *symbol*, compute indicators, and upsert
    into silver.prices.  Returns row count written.
    """
    run_date = run_date or date.today()

    with _get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT trade_date, open_price, high_price, low_price,
                       close_price, adj_close_price, volume
                FROM bronze.raw_prices
                WHERE symbol = %s
                ORDER BY trade_date ASC
                """,
                (symbol,),
            )
            rows = cur.fetchall()

    if not rows:
        logger.info("No bronze price rows for %s", symbol)
        return 0

    closes = [float(r["close_price"]) for r in rows if r["close_price"]]

    # Rolling 52-week (252 trading days) min/max
    window_52w = 252

    # Compute daily returns for z-score (Rule 7)
    returns: list[Optional[float]] = [None]
    for i in range(1, len(closes)):
        if closes[i - 1] and closes[i]:
            ret = (closes[i] - closes[i - 1]) / closes[i - 1]
            returns.append(ret)
        else:
            returns.append(None)

    valid_returns = [r for r in returns if r is not None]
    mean_ret = sum(valid_returns) / len(valid_returns) if valid_returns else 0
    var_ret = (
        sum((r - mean_ret) ** 2 for r in valid_returns) / len(valid_returns)
        if valid_returns else 0
    )
    std_ret = math.sqrt(var_ret) if var_ret > 0 else 1e-9

    silver_rows: list[tuple] = []
    for i, row in enumerate(rows):
        close = float(row["close_price"]) if row["close_price"] else None
        adj_close = float(row["adj_close_price"]) if row["adj_close_price"] else None
        prev_close = float(rows[i - 1]["close_price"]) if i > 0 and rows[i - 1]["close_price"] else None

        daily_return: Optional[float] = None
        log_return: Optional[float] = None
        is_outlier = False

        if close and prev_close:
            daily_return = round((close - prev_close) / prev_close, 6)
            if close > 0 and prev_close > 0:
                log_return = round(math.log(close / prev_close), 6)
            z = abs((daily_return - mean_ret) / std_ret)
            is_outlier = z > 4.0   # Rule 7

        # 52-week rolling window
        start_idx = max(0, i - window_52w + 1)
        window_closes = closes[start_idx: i + 1]
        high_52w = max(window_closes) if window_closes else None
        low_52w = min(window_closes) if window_closes else None
        price_range_52w = (
            round(high_52w - low_52w, 4) if high_52w and low_52w else None
        )

        # Technical indicators (require history)
        close_hist = closes[: i + 1]
        sma_20 = _calc_sma(close_hist, 20)
        sma_50 = _calc_sma(close_hist, 50)
        sma_200 = _calc_sma(close_hist, 200)
        ema_20 = _calc_ema(close_hist, 20)
        rsi_14 = _calc_rsi(close_hist, 14)

        # Rule 4 normalisation
        normalised = (
            _normalise_52w(close, low_52w, high_52w)
            if close and high_52w and low_52w
            else None
        )

        silver_rows.append((
            symbol,
            row["trade_date"],
            float(row["open_price"]) if row["open_price"] else None,
            float(row["high_price"]) if row["high_price"] else None,
            float(row["low_price"]) if row["low_price"] else None,
            close,
            adj_close,
            int(row["volume"]) if row["volume"] else None,
            daily_return,
            log_return,
            is_outlier,
            sma_20,
            sma_50,
            sma_200,
            ema_20,
            rsi_14,
            normalised,
            high_52w,
            low_52w,
            price_range_52w,
        ))

    with _get_conn() as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, _SILVER_PRICE_UPSERT, silver_rows)
        conn.commit()

    logger.info("Transformed %d price rows for %s", len(silver_rows), symbol)
    return len(silver_rows)


# ─────────────────────────────────────────────────
# Transform fundamentals
# ─────────────────────────────────────────────────

_SILVER_FUND_UPSERT = """
INSERT INTO silver.fundamentals
    (symbol, report_period, metric_name, metric_value, unit)
VALUES %s
ON CONFLICT (symbol, report_period, metric_name) DO UPDATE SET
    metric_value = EXCLUDED.metric_value,
    validated_at = NOW();
"""


def transform_fundamentals(symbol: str) -> int:
    """Promote latest bronze fundamental rows to silver (deduplication pass)."""
    with _get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Take the most recent row per metric
            cur.execute(
                """
                SELECT DISTINCT ON (metric_name)
                    symbol, report_period, metric_name, metric_value, unit
                FROM bronze.raw_fundamentals
                WHERE symbol = %s
                ORDER BY metric_name, report_period DESC
                """,
                (symbol,),
            )
            rows = cur.fetchall()

    if not rows:
        logger.info("No bronze fundamental rows for %s", symbol)
        return 0

    silver_rows = [
        (r["symbol"], r["report_period"], r["metric_name"], r["metric_value"], r["unit"])
        for r in rows
    ]

    with _get_conn() as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, _SILVER_FUND_UPSERT, silver_rows)
        conn.commit()

    logger.info("Transformed %d fundamental rows for %s", len(silver_rows), symbol)
    return len(silver_rows)


# ─────────────────────────────────────────────────
# Transform news (HF enrichment)
# ─────────────────────────────────────────────────

_SILVER_NEWS_UPSERT = """
INSERT INTO silver.news
    (content_hash, symbol, headline, source, published_at, url,
     summary, sentiment_score, embedding_updated_at)
VALUES %s
ON CONFLICT (content_hash) DO UPDATE SET
    summary              = COALESCE(EXCLUDED.summary, silver.news.summary),
    sentiment_score      = COALESCE(EXCLUDED.sentiment_score, silver.news.sentiment_score),
    updated_at           = NOW();
"""


def transform_news(symbol: str, limit: int = 50) -> int:
    """
    Enrich bronze news with HF summary + sentiment and upsert to silver.news.
    Processes at most *limit* un-enriched rows to preserve the HF budget.
    """
    with _get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT b.content_hash, b.symbol, b.headline, b.source,
                       b.published_at, b.url, b.raw_body
                FROM bronze.raw_news b
                LEFT JOIN silver.news s ON s.content_hash = b.content_hash
                WHERE b.symbol = %s
                  AND (s.content_hash IS NULL
                       OR s.summary IS NULL
                       OR s.sentiment_score IS NULL)
                ORDER BY b.published_at DESC
                LIMIT %s
                """,
                (symbol, limit),
            )
            rows = cur.fetchall()

    if not rows:
        logger.info("No un-enriched news for %s", symbol)
        return 0

    silver_rows: list[tuple] = []
    for r in rows:
        text_for_hf = r["raw_body"] or r["headline"]

        summary = summarise(text_for_hf) if text_for_hf else None
        sentiment = get_sentiment(r["headline"])

        silver_rows.append((
            r["content_hash"],
            r["symbol"],
            r["headline"],
            r["source"],
            r["published_at"],
            r["url"],
            summary,
            sentiment,
            None,  # embedding_updated_at — handled by chroma_ingest task
        ))

    with _get_conn() as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, _SILVER_NEWS_UPSERT, silver_rows)
        conn.commit()

    logger.info("Enriched %d news rows for %s", len(silver_rows), symbol)
    return len(silver_rows)


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

    totals: dict[str, int] = {
        "prices": 0,
        "fundamentals": 0,
        "news": 0,
    }
    for symbol in symbols:
        totals["prices"] += transform_prices(symbol, run_date)
        totals["fundamentals"] += transform_fundamentals(symbol)
        totals["news"] += transform_news(symbol)

    logger.info("Transform complete: %s", totals)
    return totals


if __name__ == "__main__":
    result = run()
    print(result)
