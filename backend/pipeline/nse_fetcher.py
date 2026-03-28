"""
backend/pipeline/nse_fetcher.py
================================
Fetches real-time NSE prices using official NSE India data source.
Faster and more accurate than yfinance.

Usage:
    from backend.pipeline.nse_fetcher import get_nse_prices
    df = get_nse_prices("HDFCBANK", start_date, end_date)
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Optional

import pandas as pd
import requests

logger = logging.getLogger(__name__)

# NSE endpoints
NSE_QUOTE_URL = "https://www.nseindia.com/api/quote-equity"
NSE_TIMESERIES_URL = "https://www.nseindia.com/api/historical/cm/equity"
NSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/91.0.4472.124"
}


def get_nse_prices(
    symbol: str,
    start: date,
    end: date,
) -> pd.DataFrame:
    """
    Fetch real-time NSE prices (current quote is most important).

    Args:
        symbol: Stock symbol without .NS suffix (e.g., "HDFCBANK")
        start: Start date (used for historical, may be ignored)
        end: End date (used for historical, may be ignored)

    Returns:
        DataFrame with columns: Date, Open, High, Low, Close, Volume, Adj Close
    """
    logger.info(f"Fetching NSE prices for {symbol}")

    # For current trading day, fetch current quote (real-time)
    # Historical API may fail if market is closed, so prioritize current quote
    current_quote = _fetch_current_quote(symbol)

    if current_quote is not None and not current_quote.empty:
        logger.info(f"✓ Got real-time quote for {symbol}: ₹{current_quote.iloc[0]['Close']}")
        return current_quote
    else:
        logger.warning(f"Could not fetch current quote for {symbol}, trying historical...")

    # If current quote fails, try historical (for backtesting/replay scenarios)
    try:
        historical = _fetch_historical_data(symbol, start, end)
        if not historical.empty:
            logger.info(f"Got {len(historical)} historical records for {symbol}")
            return historical
    except Exception as e:
        logger.debug(f"Historical fetch failed: {e}")

    logger.warning(f"No data found for {symbol}")
    return pd.DataFrame()


def _fetch_current_quote(symbol: str) -> Optional[pd.DataFrame]:
    """Fetch current market quote from NSE (real-time price)."""
    try:
        params = {"symbol": symbol}
        response = requests.get(
            NSE_QUOTE_URL,
            params=params,
            headers=NSE_HEADERS,
            timeout=10
        )
        response.raise_for_status()
        data = response.json()

        # NSE API returns data in "priceInfo" key
        if "priceInfo" not in data:
            logger.debug(f"No priceInfo for {symbol}")
            return None

        price_info = data["priceInfo"]
        last_price = _parse_float(price_info.get("lastPrice"))

        # Only return if we have a valid last price
        if last_price is None:
            logger.debug(f"No lastPrice for {symbol}")
            return None

        return pd.DataFrame([{
            "Date": datetime.now().date(),
            "Open": _parse_float(price_info.get("open")),
            "High": _parse_float(price_info.get("intraDayHighLow", {}).get("max")),
            "Low": _parse_float(price_info.get("intraDayHighLow", {}).get("min")),
            "Close": last_price,  # Real-time last price
            "Volume": _parse_int(data.get("tradeInfo", {}).get("totalTradedVolume")),
            "Adj Close": last_price,
        }])
    except Exception as e:
        logger.debug(f"Could not fetch current quote for {symbol}: {e}")
        return None


def _fetch_historical_data(
    symbol: str,
    start: date,
    end: date,
    series: str = "EQ"
) -> pd.DataFrame:
    """
    Fetch historical OHLCV data from NSE.

    Args:
        symbol: Stock symbol (without .NS)
        start: Start date
        end: End date
        series: Series type (EQ = Equity, default)

    Returns:
        DataFrame with OHLCV data
    """
    try:
        # NSE API requires YYYY-MM-DD format
        from_date = start.strftime("%d-%b-%Y")
        to_date = end.strftime("%d-%b-%Y")

        params = {
            "symbol": symbol,
            "series": series,
            "from": from_date,
            "to": to_date,
        }

        response = requests.get(
            NSE_TIMESERIES_URL,
            params=params,
            headers=NSE_HEADERS,
            timeout=10
        )
        response.raise_for_status()
        data = response.json()

        if "data" not in data or not data["data"]:
            logger.warning(f"No historical data for {symbol}")
            return pd.DataFrame()

        rows = []
        for record in data["data"]:
            rows.append({
                "Date": pd.to_datetime(record.get("CH_TRADING_DATE", "")).date(),
                "Open": _parse_float(record.get("CH_OPENING_PRICE")),
                "High": _parse_float(record.get("CH_HIGH_PRICE")),
                "Low": _parse_float(record.get("CH_LOW_PRICE")),
                "Close": _parse_float(record.get("CH_CLOSING_PRICE")),
                "Volume": _parse_int(record.get("CH_TOT_TRADED_QTY")),
                "Adj Close": _parse_float(record.get("CH_CLOSING_PRICE")),
            })

        return pd.DataFrame(rows) if rows else pd.DataFrame()

    except Exception as e:
        logger.warning(f"Error fetching historical data for {symbol}: {e}")
        return pd.DataFrame()


def _parse_float(value) -> Optional[float]:
    """Safely parse float values."""
    if value is None or value == "" or value == "-":
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def _parse_int(value) -> Optional[int]:
    """Safely parse integer values."""
    if value is None or value == "" or value == "-":
        return None
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return None
