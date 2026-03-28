"""
backend/pipeline/nse_fetcher.py
================================
Fetches real-time NSE prices and fundamentals using official NSE India API.
Primary data source for all market data (no yfinance dependency).

Usage:
    from backend.pipeline.nse_fetcher import get_nse_prices, get_nse_fundamentals
    df = get_nse_prices("HDFCBANK", start_date, end_date)
    info = get_nse_fundamentals("HDFCBANK")
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Optional, Dict, Any

import pandas as pd
import requests

logger = logging.getLogger(__name__)

# NSE endpoints
NSE_BASE_URL = "https://www.nseindia.com"
NSE_QUOTE_URL = f"{NSE_BASE_URL}/api/quote-equity"
NSE_TIMESERIES_URL = f"{NSE_BASE_URL}/api/historical/cm/equity"
NSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://www.nseindia.com/",
}

# Session for maintaining cookies
_session: Optional[requests.Session] = None


def _get_session() -> requests.Session:
    """Get or create a requests session with NSE cookies."""
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update(NSE_HEADERS)
        # Visit main page to get cookies
        try:
            _session.get(NSE_BASE_URL, timeout=10)
        except Exception as e:
            logger.debug(f"Could not initialize NSE session: {e}")
    return _session


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
        session = _get_session()
        params = {"symbol": symbol}
        response = session.get(
            NSE_QUOTE_URL,
            params=params,
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
        session = _get_session()
        # NSE API requires YYYY-MM-DD format
        from_date = start.strftime("%d-%b-%Y")
        to_date = end.strftime("%d-%b-%Y")

        params = {
            "symbol": symbol,
            "series": series,
            "from": from_date,
            "to": to_date,
        }

        response = session.get(
            NSE_TIMESERIES_URL,
            params=params,
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


def get_nse_fundamentals(symbol: str) -> Optional[Dict[str, Any]]:
    """
    Fetch fundamental data from NSE API for a given symbol.

    Args:
        symbol: Stock symbol without .NS suffix (e.g., "HDFCBANK")

    Returns:
        Dictionary with fundamental metrics or None if unavailable
    """
    logger.info(f"Fetching NSE fundamentals for {symbol}")

    try:
        session = _get_session()
        params = {"symbol": symbol}

        response = session.get(
            NSE_QUOTE_URL,
            params=params,
            timeout=10
        )
        response.raise_for_status()
        data = response.json()

        if not data:
            logger.warning(f"No data returned for {symbol}")
            return None

        # Extract fundamentals from various sections of NSE response
        fundamentals: Dict[str, Any] = {}

        # Price info section
        price_info = data.get("priceInfo", {})
        fundamentals["high52"] = _parse_float(price_info.get("weekHighLow", {}).get("max"))
        fundamentals["low52"] = _parse_float(price_info.get("weekHighLow", {}).get("min"))
        fundamentals["pChange365d"] = _parse_float(price_info.get("pChange365d"))
        fundamentals["pChange30d"] = _parse_float(price_info.get("pChange30d"))

        # Security info section
        security_info = data.get("securityInfo", {})
        fundamentals["faceValue"] = _parse_float(security_info.get("faceValue"))

        # Metadata section (contains PE, market cap, etc.)
        metadata = data.get("metadata", {})
        fundamentals["industryPE"] = _parse_float(metadata.get("pdSectorInd"))

        # Pre-open market section
        preopen = data.get("preOpenMarket", {})
        fundamentals["totalMarketCap"] = _parse_float(preopen.get("totalSellQuantity"))

        # Industry info
        industry_info = data.get("industryInfo", {})
        fundamentals["sectorPE"] = _parse_float(industry_info.get("pe"))

        # Trade info section
        trade_info = data.get("tradeInfo", {})
        fundamentals["deliveryToTradedQty"] = _parse_float(trade_info.get("deliveryToTradedQuantity"))
        fundamentals["totalTradedVolume"] = _parse_int(trade_info.get("totalTradedVolume"))
        fundamentals["totalTradedValue"] = _parse_float(trade_info.get("totalTradedValue"))

        # Corporate info section (for PE, EPS, Book Value)
        corp_info = data.get("corporateInfo", {})

        # Try to get additional metrics from priceInfo
        fundamentals["pe"] = _parse_float(price_info.get("pe"))
        fundamentals["eps"] = _parse_float(price_info.get("eps"))
        fundamentals["bookValue"] = _parse_float(price_info.get("bookValue"))
        fundamentals["priceToBook"] = _parse_float(price_info.get("pbRatio"))

        # Market cap from metadata
        fundamentals["marketCap"] = _parse_float(metadata.get("marketCap"))

        # Security-wide data
        sec_wise = data.get("securityWiseDP", {})
        fundamentals["securityWiseDP"] = _parse_float(sec_wise.get("quantityTraded"))

        # Free float market cap
        fundamentals["ffmc"] = _parse_float(metadata.get("ffmc"))

        logger.info(f"✓ Got {len([v for v in fundamentals.values() if v is not None])} fundamental metrics for {symbol}")
        return fundamentals

    except Exception as e:
        logger.warning(f"Error fetching fundamentals for {symbol}: {e}")
        return None
