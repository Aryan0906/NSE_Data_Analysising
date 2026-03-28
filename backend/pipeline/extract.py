"""
backend/pipeline/extract.py
============================
Task 2 of the Airflow DAG — pulls NSE price history and key fundamentals
from the official NSE India API and inserts them into the **bronze** layer.

Rules enforced:
  Rule 3 : NSE API exclusively for market data (official source).
  Rule 5 : This task runs AFTER db_init and BEFORE transform.
  Rule 8 : Writes as nse_admin; nse_reader gets SELECT via db_init grants.

Design:
  - All writes are UPSERT (ON CONFLICT DO NOTHING) → idempotent on re-run.
  - Fundamentals are stored as EAV rows (metric_name / metric_value) so new
    fields need no schema change.
  - A pipeline_runs audit row is written for lineage (Rule 5 design).
"""

from __future__ import annotations

import hashlib
import logging
import os
from contextlib import contextmanager
from datetime import date, timedelta
from typing import Generator, Optional

import psycopg2
import psycopg2.extras
import pandas as pd

from backend.pipeline.settings import settings
from backend.pipeline.nse_fetcher import get_nse_prices, get_nse_fundamentals

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=settings.log_level,
    format="%(asctime)s | %(levelname)s | extract | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)

# ── Fundamental metrics we persist from NSE API ────────────────────
FUNDAMENTAL_FIELDS: list[str] = [
    "marketCap",
    "pe",
    "sectorPE",
    "industryPE",
    "bookValue",
    "priceToBook",
    "faceValue",
    "eps",
    "deliveryToTradedQty",
    "securityWiseDP",
    "totalMarketCap",
    "ffmc",
    "high52",
    "low52",
    "pChange365d",
    "pChange30d",
]


# ─────────────────────────────────────────────────
# Connection helper (admin role)
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
# Audit helper
# ─────────────────────────────────────────────────

def _log_run(
    conn: psycopg2.extensions.connection,
    *,
    task_id: str,
    run_date: date,
    status: str,
    records_read: int = 0,
    records_written: int = 0,
    error_message: Optional[str] = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO public.pipeline_runs
                (dag_id, task_id, run_date, status,
                 records_read, records_written, error_message, finished_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
            """,
            (
                "nse_pipeline",
                task_id,
                run_date,
                status,
                records_read,
                records_written,
                error_message,
            ),
        )
    conn.commit()


# ─────────────────────────────────────────────────
# Price extraction
# ─────────────────────────────────────────────────

_PRICE_UPSERT = """
INSERT INTO bronze.raw_prices
    (symbol, trade_date, open_price, high_price, low_price,
     close_price, adj_close_price, volume)
VALUES %s
ON CONFLICT (symbol, trade_date, source) DO NOTHING;
"""


def extract_prices(
    symbols: list[str],
    start: date,
    end: date,
    run_date: Optional[date] = None,
) -> int:
    """
    Download OHLCV data for *symbols* between *start* and *end* and insert
    into bronze.raw_prices.  Returns total rows written.

    Uses NSE India API exclusively for accurate, real-time data.
    """
    run_date = run_date or date.today()
    total_written = 0

    for symbol in symbols:
        # Remove .NS suffix if present for NSE API
        symbol_clean = symbol.replace(".NS", "")
        logger.info("Extracting prices for %s [%s → %s]", symbol_clean, start, end)

        df = None

        # Fetch from NSE API (official source)
        try:
            # Fetch today's real-time price
            logger.info("Fetching TODAY's real-time price from NSE API for %s", symbol_clean)
            today_df = get_nse_prices(symbol_clean, date.today(), date.today())

            if today_df is not None and not today_df.empty:
                logger.info("✓ NSE API today's price for %s: ₹%.2f", symbol_clean, today_df.iloc[0]['Close'])

                # Then get historical data (if date range extends beyond today)
                if start < date.today():
                    logger.info("Fetching historical prices from NSE API for %s", symbol_clean)
                    hist_end = end - timedelta(days=1) if end >= date.today() else end
                    hist_df = get_nse_prices(symbol_clean, start, hist_end)

                    if hist_df is not None and not hist_df.empty:
                        df = pd.concat([hist_df, today_df], ignore_index=True)
                        logger.info("✓ NSE API success: %d records for %s", len(df), symbol_clean)
                    else:
                        df = today_df
                        logger.info("✓ NSE API (today only): %d records for %s", len(df), symbol_clean)
                else:
                    df = today_df
            else:
                logger.warning("NSE API returned no data for %s", symbol_clean)
                continue
        except Exception as exc:
            logger.error("NSE API error for %s: %s", symbol_clean, exc)
            continue

        if df is None or df.empty:
            logger.warning("No price data for %s", symbol_clean)
            continue

        df = df.reset_index(drop=True)
        rows = [
            (
                symbol_clean,                            # store original symbol (without .NS)
                row["Date"].date() if hasattr(row["Date"], "date") else row["Date"],
                float(row["Open"])   if pd.notna(row["Open"]) else None,
                float(row["High"])   if pd.notna(row["High"]) else None,
                float(row["Low"])    if pd.notna(row["Low"]) else None,
                float(row["Close"])  if pd.notna(row["Close"]) else None,
                float(row["Adj Close"]) if pd.notna(row["Adj Close"]) else None,
                int(row["Volume"])   if pd.notna(row["Volume"]) else None,
            )
            for _, row in df.iterrows()
        ]

        if not rows:
            continue

        with _get_conn() as conn:
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(cur, _PRICE_UPSERT, rows)
            conn.commit()
            total_written += len(rows)
            _log_run(
                conn,
                task_id="extract_prices",
                run_date=run_date,
                status="SUCCESS",
                records_read=len(rows),
                records_written=len(rows),
            )

        logger.info("Upserted %d price rows for %s", len(rows), symbol_clean)

    logger.info("Price extraction complete — %d rows total", total_written)
    return total_written


# ─────────────────────────────────────────────────
# Fundamentals extraction
# ─────────────────────────────────────────────────

_FUNDAMENTAL_UPSERT = """
INSERT INTO bronze.raw_fundamentals
    (symbol, report_period, metric_name, metric_value, unit)
VALUES %s
ON CONFLICT (symbol, report_period, metric_name, source) DO NOTHING;
"""


def extract_fundamentals(
    symbols: list[str],
    run_date: Optional[date] = None,
) -> int:
    """
    Fetch key fundamental metrics from NSE API for each symbol and
    insert into bronze.raw_fundamentals.  report_period = today's date.
    """
    run_date = run_date or date.today()
    total_written = 0

    for symbol in symbols:
        symbol_clean = symbol.replace(".NS", "")
        logger.info("Extracting fundamentals for %s", symbol_clean)

        try:
            info = get_nse_fundamentals(symbol_clean)
            if info is None:
                logger.warning("No fundamental data returned for %s", symbol_clean)
                continue
        except Exception as exc:
            logger.warning("NSE fundamentals error for %s: %s", symbol_clean, exc)
            continue

        rows = []
        for field in FUNDAMENTAL_FIELDS:
            raw_val = info.get(field)
            if raw_val is None:
                continue
            try:
                metric_value = float(raw_val)
            except (TypeError, ValueError):
                continue
            rows.append((symbol_clean, run_date, field, metric_value, None))

        if not rows:
            logger.warning("No fundamental data for %s", symbol_clean)
            continue

        with _get_conn() as conn:
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(cur, _FUNDAMENTAL_UPSERT, rows)
            conn.commit()
            total_written += len(rows)
            _log_run(
                conn,
                task_id="extract_fundamentals",
                run_date=run_date,
                status="SUCCESS",
                records_read=len(rows),
                records_written=len(rows),
            )

        logger.info("Upserted %d fundamental rows for %s", len(rows), symbol_clean)

    logger.info("Fundamentals extraction complete — %d rows total", total_written)
    return total_written


# ─────────────────────────────────────────────────
# Airflow-callable entry-point
# ─────────────────────────────────────────────────

def run(
    symbols: Optional[list[str]] = None,
    lookback_days: Optional[int] = None,
    run_date: Optional[date] = None,
) -> dict[str, int]:
    """
    Called by the Airflow PythonOperator.
    Returns a dict of {task_id: rows_written} for XCom.
    """
    symbols = symbols or settings.nse_symbols
    lookback_days = lookback_days or settings.etl_lookback_days
    run_date = run_date or date.today()
    end = run_date
    start = end - timedelta(days=lookback_days)

    prices_written = extract_prices(symbols, start, end, run_date)
    fundamentals_written = extract_fundamentals(symbols, run_date)

    return {
        "extract_prices": prices_written,
        "extract_fundamentals": fundamentals_written,
    }


if __name__ == "__main__":
    result = run()
    print(result)
