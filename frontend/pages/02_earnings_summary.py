"""
frontend/pages/02_earnings_summary.py
======================================
Displays BART-generated earnings summaries from public.earnings_summaries table.
"""

import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

import streamlit as st
import pandas as pd

from frontend.components.db_connector import fetch_data

st.set_page_config(page_title="Earnings Summaries", layout="wide")

st.title("📊 Earnings Summaries")
st.markdown("BART-generated summaries of earnings call transcripts and reports.")

# Query earnings summaries from public schema
query = """
    SELECT 
        ticker,
        report_date,
        source_file,
        summary,
        created_at
    FROM public.earnings_summaries
    ORDER BY report_date DESC
"""

try:
    df = fetch_data(query)
    
    if df.empty:
        st.info("📭 No earnings summaries found. Drop a PDF into data/earnings/ and run the pipeline.")
    else:
        # Sidebar filter by ticker
        st.sidebar.header("🔍 Filter")
        tickers = ["All Tickers"] + sorted(df['ticker'].unique().tolist())
        selected_ticker = st.sidebar.selectbox("Select Ticker", tickers)
        
        if selected_ticker != "All Tickers":
            df = df[df['ticker'] == selected_ticker]
        
        # Summary stats
        col1, col2 = st.columns(2)
        col1.metric("📄 Total Summaries", len(df))
        col2.metric("🏢 Companies", df['ticker'].nunique())
        
        st.divider()
        
        # Display summaries in expanders
        for _, row in df.iterrows():
            ticker = row['ticker']
            report_date = row['report_date']
            source_file = row['source_file']
            summary = row['summary']
            created_at = row['created_at']
            
            # Format report_date
            if pd.notna(report_date):
                if hasattr(report_date, 'strftime'):
                    date_str = report_date.strftime("%Y-%m-%d")
                else:
                    date_str = str(report_date)[:10]
            else:
                date_str = "Unknown date"
            
            with st.expander(f"📈 {ticker} — {date_str} ({source_file})"):
                st.markdown(summary)
                if pd.notna(created_at):
                    st.caption(f"Generated: {created_at}")
                    
except Exception as e:
    st.error(f"Failed to load earnings summaries: {e}")
