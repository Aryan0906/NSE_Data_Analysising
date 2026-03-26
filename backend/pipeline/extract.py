"""
backend/pipeline/extract.py
============================
Task 2 of the Airflow DAG — pulls NSE price history and key fundamentals
from Yahoo Finance (yfinance) and inserts them into the **bronze** layer.

Rules enforced:
  Rule 3 : yfinance exclusively for market data (no paid data sources).
  Rule 5 : This task runs AFTER db_init and BEFORE transform.
  Rule 8 : Writes as nse_admin; nse_reader gets SELECT via db_init grants.

Design:
  - All writes are UPSERT (ON CONFLICT DO NOTHING) → idempotent on re-run.
  - Fundamentals are stored as EAV rows (metric_name / metric_value) so new
    yfinance fields need no schema change.
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
import yfinance as yf

from backend.pipeline.settings import settings

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=settings.log_level,
    format="%(asctime)s | %(levelname)s | extract | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)

# ── Fundamental metrics we persist from yf.Ticker.info ────────────────────
FUNDAMENTAL_FIELDS: list[str] = [
    "marketCap",
    "trailingPE",
    "forwardPE",
    "priceToBook",
    "debtToEquity",
    "returnOnEquity",
    "returnOnAssets",
    "trailingEps",
    "forwardEps",
    "dividendYield",
    "payoutRatio",
    "revenueGrowth",
    "earningsGrowth",
    "currentRatio",
    "quickRatio",
    "freeCashflow",
    "operatingCashflow",
    "grossMargins",
    "operatingMargins",
    "profitMargins",
    "totalRevenue",
    "netIncomeToCommon",
    "totalDebt",
    "totalCash",
    "enterpriseValue",
    "beta",
    "52WeekChange",
    "fiftyTwoWeekHigh",
    "fiftyTwoWeekLow",
    "averageVolume",
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
    """
    run_date = run_date or date.today()
    total_written = 0

    for symbol in symbols:
        nse_symbol = symbol if symbol.endswith(".NS") else f"{symbol}.NS"
        logger.info("Extracting prices for %s [%s → %s]", nse_symbol, start, end)

        try:
            ticker = yf.Ticker(nse_symbol)
            df = ticker.history(
                start=start.isoformat(),
                end=end.isoformat(),
                auto_adjust=False,
                actions=False,
            )
        except Exception as exc:
            logger.warning("yfinance error for %s: %s", nse_symbol, exc)
            continue

        if df.empty:
            logger.warning("No price data returned for %s", nse_symbol)
            continue

        df = df.reset_index()
        rows = [
            (
                symbol,                            # store original symbol (without .NS)
                row["Date"].date(),
                float(row["Open"])   if row["Open"]   == row["Open"] else None,
                float(row["High"])   if row["High"]   == row["High"] else None,
                float(row["Low"])    if row["Low"]    == row["Low"]  else None,
                float(row["Close"])  if row["Close"]  == row["Close"] else None,
                float(row["Adj Close"]) if row["Adj Close"] == row["Adj Close"] else None,
                int(row["Volume"])   if row["Volume"] == row["Volume"] else None,
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

        logger.info("Upserted %d price rows for %s", len(rows), symbol)

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
    Fetch key fundamental metrics from yf.Ticker.info for each symbol and
    insert into bronze.raw_fundamentals.  report_period = today's date as
    a proxy (yfinance doesn't expose precise report quarters via .info).
    """
    run_date = run_date or date.today()
    total_written = 0

    for symbol in symbols:
        nse_symbol = symbol if symbol.endswith(".NS") else f"{symbol}.NS"
        logger.info("Extracting fundamentals for %s", nse_symbol)

        try:
            info = yf.Ticker(nse_symbol).info
        except Exception as exc:
            logger.warning("yfinance fundamentals error for %s: %s", nse_symbol, exc)
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
            rows.append((symbol, run_date, field, metric_value, None))

        if not rows:
            logger.warning("No fundamental data for %s", symbol)
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

        logger.info("Upserted %d fundamental rows for %s", len(rows), symbol)

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
