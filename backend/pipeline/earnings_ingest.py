"""
backend/pipeline/earnings_ingest.py
====================================
Analyze earnings using NSE fundamentals already in database + BART + FinBERT.

Flow:
1. For each symbol, fetch fundamentals from bronze.raw_fundamentals (NSE API data)
2. Extract key metrics: EPS, PE ratio, market cap, book value, etc.
3. Generate natural-language earnings narrative
4. Summarize narrative using BART
5. Score sentiment using FinBERT
6. Store in public.earnings_summaries

Note: Uses NSE API data already fetched by extract.py (Rule 3 — NSE API as single source)
"""

from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Optional, Dict, Any

import psycopg2
import psycopg2.extras

from backend.pipeline.settings import settings
from backend.pipeline.hf_client import summarise, get_sentiment

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | earnings_ingest | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)


def _get_db_connection():
    """Get PostgreSQL connection."""
    return psycopg2.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        dbname=settings.postgres_db,
        user=settings.postgres_user,
        password=settings.postgres_password,
        connect_timeout=10,
    )


def _fetch_nse_fundamentals(symbol: str) -> Optional[Dict[str, Any]]:
    """
    Fetch latest fundamentals for a symbol from bronze.raw_fundamentals.

    Returns dict with fundamentals from NSE API (via extract.py):
    - pe, eps, market_cap, book_value, high52, low52, etc.
    """
    conn = _get_db_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            # Get latest fundamentals row for this symbol
            cur.execute(
                """
                SELECT metric_name, metric_value, ingested_at
                FROM bronze.raw_fundamentals
                WHERE symbol = %s
                ORDER BY ingested_at DESC
                LIMIT 50
                """,
                (symbol,),
            )
            rows = cur.fetchall()

            if not rows:
                logger.warning(f"No fundamentals in DB for {symbol}")
                return None

            # Convert metric rows to dict
            fundamentals = {row["metric_name"]: row["metric_value"] for row in rows}

            return {
                "pe": fundamentals.get("pe"),
                "eps": fundamentals.get("eps"),
                "market_cap": fundamentals.get("marketCap"),
                "book_value": fundamentals.get("bookValue"),
                "price_to_book": fundamentals.get("priceToBook"),
                "high52": fundamentals.get("high52"),
                "low52": fundamentals.get("low52"),
                "sector_pe": fundamentals.get("sectorPE"),
                "ingested_at": rows[0]["ingested_at"],
            }
    except Exception as e:
        logger.error(f"Failed to fetch fundamentals for {symbol}: {e}")
        return None
    finally:
        conn.close()


def _generate_earnings_narrative(symbol: str, fundamentals: Dict[str, Any]) -> str:
    """
    Generate natural-language narrative from NSE fundamentals.
    Format: Easy-to-understand summary of financial health for dashboard.
    """
    lines = [f"{symbol} Financial Summary"]
    lines.append("=" * 50)

    if fundamentals.get("eps"):
        try:
            eps_val = float(fundamentals["eps"])
            lines.append(f"• EPS (Earnings Per Share): ₹{eps_val:.2f}")
        except (ValueError, TypeError):
            pass

    if fundamentals.get("pe"):
        try:
            pe_val = float(fundamentals["pe"])
            lines.append(f"• P/E Ratio: {pe_val:.2f}x")
        except (ValueError, TypeError):
            pass

    if fundamentals.get("market_cap"):
        try:
            mc_val = float(fundamentals["market_cap"])
            mc_label = f"₹{mc_val/1e7:.0f} Cr" if mc_val > 1e7 else f"₹{mc_val:.0f}"
            lines.append(f"• Market Cap: {mc_label}")
        except (ValueError, TypeError):
            pass

    if fundamentals.get("book_value"):
        try:
            bv_val = float(fundamentals["book_value"])
            lines.append(f"• Book Value: ₹{bv_val:.2f}")
        except (ValueError, TypeError):
            pass

    if fundamentals.get("price_to_book"):
        try:
            pb_val = float(fundamentals["price_to_book"])
            lines.append(f"• Price-to-Book Ratio: {pb_val:.2f}x")
        except (ValueError, TypeError):
            pass

    if fundamentals.get("high52"):
        try:
            h52 = float(fundamentals["high52"])
            l52 = float(fundamentals["low52"]) if fundamentals.get("low52") else None
            lines.append(f"• 52-Week High: ₹{h52:.2f}")
            if l52:
                lines.append(f"• 52-Week Low: ₹{l52:.2f}")
        except (ValueError, TypeError):
            pass

    if fundamentals.get("sector_pe"):
        try:
            sec_pe = float(fundamentals["sector_pe"])
            lines.append(f"• Sector P/E: {sec_pe:.2f}x")
        except (ValueError, TypeError):
            pass

    lines.append(f"\nData fetched: {fundamentals.get('ingested_at', 'N/A')}")
    return "\n".join(lines)


def _analyze_earnings(symbol: str, fundamentals: Dict[str, Any]) -> Dict[str, Any]:
    """
    Analyze earnings using BART (summary) + FinBERT (sentiment).

    Args:
        symbol: Stock symbol
        fundamentals: NSE fundamentals dict

    Returns:
        dict with:
        - summary: BART-generated summary
        - sentiment_score: FinBERT sentiment [-1, 1]
    """
    narrative = _generate_earnings_narrative(symbol, fundamentals)

    # Summarize using BART
    summary = summarise(narrative, max_length=200, min_length=50)
    if not summary:
        summary = narrative[:500]  # Fallback to narrative if summarization fails

    # Score sentiment using FinBERT
    sentiment = get_sentiment(narrative)
    if sentiment is None:
        sentiment = 0.0  # Neutral if sentiment analysis fails

    return {
        "summary": summary,
        "sentiment": sentiment,
        "narrative": narrative,
    }


def _store_earnings_summary(
    symbol: str,
    fundamentals: Dict[str, Any],
    analysis: Dict[str, Any],
) -> int:
    """
    Store earnings summary in public.earnings_summaries.
    Uses NSE fundamentals ingested_at as report_date.

    Returns number of rows inserted (1 or 0).
    """
    conn = _get_db_connection()
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            report_date = fundamentals.get("ingested_at", datetime.now())
            source_file = f"nse-fundamentals-{symbol}"

            cur.execute(
                """
                INSERT INTO public.earnings_summaries
                (ticker, report_date, source_file, summary, created_at)
                VALUES (%s, %s, %s, %s, NOW())
                ON CONFLICT (ticker, report_date) DO UPDATE
                SET summary = EXCLUDED.summary, created_at = NOW()
                RETURNING id
                """,
                (symbol, report_date, source_file, analysis["summary"]),
            )
            result = cur.fetchone()
            if result:
                logger.info(
                    f"Stored earnings for {symbol} (date={report_date}, "
                    f"sentiment={analysis['sentiment']:.2f})"
                )
                return 1
            return 0
    except Exception as e:
        logger.error(f"Failed to store earnings for {symbol}: {e}")
        return 0
    finally:
        conn.close()


def run(symbols: list[str]) -> int:
    """
    Main entry point: Process earnings for all symbols using NSE API data.

    Flow:
    1. For each symbol, fetch fundamentals from bronze.raw_fundamentals (NSE API)
    2. Generate narrative
    3. Analyze with BART + FinBERT
    4. Store in public.earnings_summaries

    Args:
        symbols: List of NSE stock symbols (e.g., ['INFY', 'TCS', 'WIPRO'])

    Returns:
        Total number of earnings summaries stored/updated
    """
    logger.info(f"Starting earnings_ingest for {len(symbols)} symbols...")
    total_stored = 0

    for symbol in symbols:
        try:
            logger.info(f"Processing {symbol}...")

            # Fetch NSE fundamentals from database
            fundamentals = _fetch_nse_fundamentals(symbol)
            if not fundamentals:
                logger.warning(f"No fundamentals found for {symbol}")
                continue

            # Analyze earnings
            analysis = _analyze_earnings(symbol, fundamentals)

            # Store in database
            stored = _store_earnings_summary(symbol, fundamentals, analysis)
            total_stored += stored

        except Exception as e:
            logger.error(f"Error processing {symbol}: {e}")
            continue

    logger.info(f"earnings_ingest complete: {total_stored} summaries stored/updated")
    return total_stored


if __name__ == "__main__":
    # Standalone execution for testing
    test_symbols = settings.nse_symbols[:3]  # Test with first 3 symbols
    run(test_symbols)
