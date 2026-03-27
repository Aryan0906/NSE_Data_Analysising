import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

import streamlit as st
import pandas as pd

from frontend.components.db_connector import fetch_data, get_tickers
from frontend.components.charts import plot_candlestick, plot_volume_bar

st.set_page_config(page_title="Price Analytics", layout="wide")

st.title("📊 Technical Price Analytics")

tickers = get_tickers()
if not tickers:
    st.error("No active tickers found in the database. Has the pipeline run?")
    st.stop()

selected_ticker = st.sidebar.selectbox("Select Ticker", tickers)
if not selected_ticker:
    st.stop()


# Fetch data from silver.prices (pipeline populates this table)
query = f"""
    SELECT 
        trade_date as trading_date,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        sma_20,
        sma_50
    FROM silver.prices
    WHERE symbol = '{selected_ticker}'
    ORDER BY trade_date ASC
"""

df = fetch_data(query)

if df.empty:
    st.warning(f"No price data available for {selected_ticker}.")
else:
    # Latest stats
    latest = df.iloc[-1]
    st.subheader(f"{selected_ticker} - Latest Metrics")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Close Price", f"₹{latest['close_price']:.2f}")
    col2.metric("20-Day SMA", f"₹{latest['sma_20']:.2f}" if not pd.isna(latest['sma_20']) else "N/A")
    col3.metric("50-Day SMA", f"₹{latest['sma_50']:.2f}" if not pd.isna(latest['sma_50']) else "N/A")
    col4.metric("Volume", f"{int(latest['volume']):,}")
    
    ticker_val: str = str(selected_ticker) if selected_ticker else ""
    st.plotly_chart(plot_candlestick(df, ticker_val), use_container_width=True)
    st.plotly_chart(plot_volume_bar(df), use_container_width=True)
#
