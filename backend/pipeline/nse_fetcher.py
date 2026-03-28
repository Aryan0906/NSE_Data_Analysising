"""
backend/pipeline/nse_fetcher.py
================================
Fetches NSE prices and fundamentals using jugaad-data.
Handles session management, rate limiting, and market-hours awareness.

Usage:
    from backend.pipeline.nse_fetcher import get_nse_prices, get_nse_fundamentals
"""

from __future__ import annotations

import logging
import time
from datetime import date, datetime, timedelta
from typing import Optional, Dict, Any

import pandas as pd

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────
# Market hours check
# ─────────────────────────────────────────────────

def _is_market_open() -> bool:
    """Check if NSE is currently open (9:15 AM – 3:30 PM IST weekdays)."""
    from datetime import timezone
    utc_now = datetime.now(timezone.utc)
    ist_now = utc_now + timedelta(hours=5, minutes=30)
    market_open = ist_now.replace(hour=9, minute=15, second=0, microsecond=0)
    market_close = ist_now.replace(hour=15, minute=30, second=0, microsecond=0)
    return ist_now.weekday() < 5 and market_open <= ist_now <= market_close


# ─────────────────────────────────────────────────
# Price fetcher
# ─────────────────────────────────────────────────

def get_nse_prices(
    symbol: str,
    start: date,
    end: date,
    retries: int = 3,
) -> pd.DataFrame:
    """
    Fetch OHLCV data from NSE via jugaad-data.

    Returns DataFrame with columns:
        Date, Open, High, Low, Close, Adj Close, Volume

    Falls back gracefully if NSE is unreachable outside market hours.
    """
    from jugaad_data.nse import stock_df

    symbol_clean = symbol.replace(".NS", "").upper()
    logger.info("Fetching NSE prices for %s [%s → %s]", symbol_clean, start, end)

    for attempt in range(1, retries + 1):
        try:
            df = stock_df(
                symbol=symbol_clean,
                from_date=start,
                to_date=end,
                series="EQ",
            )

            if df is None or df.empty:
                logger.warning("No data returned for %s (attempt %d)", symbol_clean, attempt)
                time.sleep(2 ** attempt)
                continue

            # Normalise column names to match the rest of the pipeline
            # jugaad-data v0.33 returns: DATE, SERIES, OPEN, HIGH, LOW, PREV. CLOSE, LTP, CLOSE, VWAP, VOLUME, VALUE, NO OF TRADES, DELIVERY QTY, DELIVERY %, SYMBOL
            df = df.rename(columns={
                "DATE": "Date",
                "OPEN": "Open",
                "HIGH": "High",
                "LOW": "Low",
                "CLOSE": "Close",
                "LTP": "Adj Close",
                "VOLUME": "Volume",
            })

            # jugaad-data returns newest first — reverse to ascending
            if "Date" in df.columns:
                df = df.sort_values("Date").reset_index(drop=True)

            # Ensure Date column is python date, not string
            if "Date" in df.columns:
                df["Date"] = pd.to_datetime(df["Date"]).dt.date

            # If Adj Close not present, copy Close
            if "Adj Close" not in df.columns and "Close" in df.columns:
                df["Adj Close"] = df["Close"]

            # Keep only required columns
            required = ["Date", "Open", "High", "Low", "Close", "Adj Close", "Volume"]
            available = [c for c in required if c in df.columns]
            df = df[available]

            # Cast numeric columns
            for col in ["Open", "High", "Low", "Close", "Adj Close"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            if "Volume" in df.columns:
                df["Volume"] = pd.to_numeric(df["Volume"], errors="coerce").fillna(0).astype(int)

            logger.info(
                "✓ jugaad-data: %d rows for %s (₹%.2f latest close)",
                len(df), symbol_clean, df["Close"].iloc[-1] if not df.empty else 0
            )
            return df

        except Exception as exc:
            logger.warning(
                "jugaad-data attempt %d/%d failed for %s: %s",
                attempt, retries, symbol_clean, exc
            )
            if attempt < retries:
                time.sleep(2 ** attempt)  # 2s, 4s, 8s
            else:
                logger.error(
                    "All %d attempts failed for %s. "
                    "NSE may be rate-limiting or market is closed.",
                    retries, symbol_clean
                )

    return pd.DataFrame()


# ─────────────────────────────────────────────────
# Fundamentals fetcher
# ─────────────────────────────────────────────────

def get_nse_fundamentals(
    symbol: str,
    retries: int = 3,
) -> Optional[Dict[str, Any]]:
    """
    Fetch fundamental metrics from NSE via jugaad-data.

    Returns a flat dict of metric_name → float value,
    matching the field names expected by extract.py.
    """
    from jugaad_data.nse import NSELive

    symbol_clean = symbol.replace(".NS", "").upper()
    logger.info("Fetching NSE fundamentals for %s", symbol_clean)

    for attempt in range(1, retries + 1):
        try:
            n = NSELive()
            quote = n.stock_quote(symbol_clean)

            if not quote:
                logger.warning("Empty quote for %s (attempt %d)", symbol_clean, attempt)
                time.sleep(2 ** attempt)
                continue

            price_info = quote.get("priceInfo", {})
            security_info = quote.get("securityInfo", {})
            metadata = quote.get("metadata", {})
            trade_info = quote.get("tradeInfo", {})
            week_hl = price_info.get("weekHighLow", {})

            def _f(val) -> Optional[float]:
                """Safe float conversion."""
                try:
                    return float(val) if val not in (None, "", "-") else None
                except (TypeError, ValueError):
                    return None

            fundamentals: Dict[str, Any] = {
                # Price metrics
                "high52": _f(week_hl.get("max")),
                "low52": _f(week_hl.get("min")),
                "pChange365d": _f(price_info.get("pChange365d")),
                "pChange30d": _f(price_info.get("pChange30d")),

                # Valuation
                "pe": _f(price_info.get("pe")),
                "eps": _f(price_info.get("eps")),
                "bookValue": _f(price_info.get("bookValue")),
                "priceToBook": _f(price_info.get("pbRatio")),
                "marketCap": _f(metadata.get("marketCap")),
                "faceValue": _f(security_info.get("faceValue")),
                "industryPE": _f(metadata.get("pdSectorInd")),
                "ffmc": _f(metadata.get("ffmc")),

                # Trading
                "deliveryToTradedQty": _f(
                    trade_info.get("deliveryToTradedQuantity")
                ),
            }

            # Filter out None values
            fundamentals = {k: v for k, v in fundamentals.items() if v is not None}

            logger.info(
                "✓ NSE fundamentals for %s: %d metrics fetched",
                symbol_clean, len(fundamentals)
            )
            return fundamentals

        except Exception as exc:
            logger.warning(
                "Fundamentals attempt %d/%d failed for %s: %s",
                attempt, retries, symbol_clean, exc
            )
            if attempt < retries:
                time.sleep(2 ** attempt)
            else:
                logger.error(
                    "All %d fundamentals attempts failed for %s.",
                    retries, symbol_clean
                )

    return None
