"""
backend/pipeline/realtime_prices.py
====================================
Lightweight real-time price updater for streaming updates (every 1 second).
Features:
- Caching to avoid hammering NSE API
- Only updates database when prices actually change
- Handles rate limiting gracefully
- Minimal overhead
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, date
from typing import Optional, Dict
from contextlib import contextmanager

import psycopg2
import psycopg2.extras

from backend.pipeline.settings import settings
from backend.pipeline.nse_fetcher import _fetch_current_quote

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=settings.log_level,
    format="%(asctime)s | %(levelname)s | realtime | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)

# Cache to avoid redundant API calls
PRICE_CACHE: Dict[str, Dict] = {}
CACHE_TTL_SECONDS = 2  # Refresh every 2 seconds


@contextmanager
def _get_conn():
    """Get database connection."""
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


def update_realtime_prices(symbols: Optional[list[str]] = None) -> Dict[str, int]:
    """
    Fetch current prices from NSE API and update database if changed.

    Args:
        symbols: List of symbols to update (default: from settings)

    Returns:
        Dict with update counts: {symbol: rows_updated, ...}
    """
    symbols = symbols or settings.nse_symbols
    updates = {}

    for symbol in symbols:
        try:
            symbol_clean = symbol.replace(".NS", "")
            updated = _update_single_price(symbol_clean)
            updates[symbol_clean] = updated
        except Exception as e:
            logger.warning(f"Error updating {symbol}: {e}")
            updates[symbol.replace('.NS', '')] = 0

    return updates


def _update_single_price(symbol: str) -> int:
    """
    Fetch current price for single symbol and update database if changed.

    Returns: 1 if updated, 0 if no change
    """
    # Check cache first
    cached = PRICE_CACHE.get(symbol, {})
    cache_time = cached.get('timestamp', 0)

    if time.time() - cache_time < CACHE_TTL_SECONDS:
        return 0  # Use cached, no update needed

    # Fetch from NSE API
    try:
        df = _fetch_current_quote(symbol)
        if df is None or df.empty:
            return 0

        current_row = df.iloc[0]
        current_price = current_row['Close']

        # Check if price changed from last fetch
        last_price = cached.get('close_price')

        if last_price is not None and last_price == current_price:
            # Price didn't change, just update cache timestamp
            PRICE_CACHE[symbol] = {
                'close_price': current_price,
                'timestamp': time.time(),
                'open': current_row['Open'],
                'high': current_row['High'],
                'low': current_row['Low'],
                'volume': current_row['Volume'],
            }
            return 0

        # Price changed! Update database
        _insert_price_to_bronze(symbol, current_row)

        # Update cache
        PRICE_CACHE[symbol] = {
            'close_price': current_price,
            'timestamp': time.time(),
            'open': current_row['Open'],
            'high': current_row['High'],
            'low': current_row['Low'],
            'volume': current_row['Volume'],
        }

        logger.info(f"{symbol}: ₹{current_price:.2f} (updated)")
        return 1

    except Exception as e:
        logger.debug(f"Error fetching {symbol}: {e}")
        return 0


def _insert_price_to_bronze(symbol: str, price_row) -> None:
    """Insert current price to bronze.raw_prices."""
    trade_date = date.today()

    sql = """
        INSERT INTO bronze.raw_prices
            (symbol, trade_date, open_price, high_price, low_price,
             close_price, adj_close_price, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol, trade_date, source) DO UPDATE SET
            open_price = EXCLUDED.open_price,
            high_price = EXCLUDED.high_price,
            low_price = EXCLUDED.low_price,
            close_price = EXCLUDED.close_price,
            adj_close_price = EXCLUDED.adj_close_price,
            volume = EXCLUDED.volume
    """

    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (
                symbol,
                trade_date,
                float(price_row['Open']) if price_row['Open'] is not None else None,
                float(price_row['High']) if price_row['High'] is not None else None,
                float(price_row['Low']) if price_row['Low'] is not None else None,
                float(price_row['Close']),
                float(price_row['Adj Close']),
                int(price_row['Volume']) if price_row['Volume'] is not None else None,
            ))
        conn.commit()


if __name__ == "__main__":
    result = update_realtime_prices()
    print(f"Updates: {result}")
