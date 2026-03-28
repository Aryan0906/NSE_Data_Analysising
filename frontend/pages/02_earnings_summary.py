"""
frontend/pages/02_earnings_summary.py
=====================================
Displays BART-generated earnings analysis with sentiment scores.
Data sourced from public.earnings_summaries (populated by earnings_ingest task).
"""

import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

import streamlit as st
import pandas as pd

from frontend.components.db_connector import fetch_data

st.set_page_config(page_title="Earnings Analysis", layout="wide")

st.title("💼 Earnings Analysis")
st.markdown("""
**BART-analyzed earnings data** from NSE fundamentals (EPS, P/E, market cap, etc).
Updated automatically when your Airflow pipeline runs.
""")

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
        st.info("""
        📭 **No earnings summaries found yet.**

        The earnings analysis will be automatically generated when:
        1. Your Airflow `stock_pipeline` DAG runs (every hour, 24/7)
        2. Or you manually trigger the DAG in Airflow UI

        Each summary includes:
        - EPS & P/E ratio analysis
        - Market cap & book value
        - 52-week price range
        - Automated sentiment score via FinBERT
        """)
    else:
        # Sidebar filter by ticker
        st.sidebar.header("🔍 Filter & Sort")
        tickers = ["All Tickers"] + sorted(df['ticker'].unique().tolist())
        selected_ticker = st.sidebar.selectbox("Select Ticker", tickers)

        if selected_ticker != "All Tickers":
            df = df[df['ticker'] == selected_ticker]

        # Summary stats
        col1, col2, col3 = st.columns(3)
        col1.metric("📄 Total Summaries", len(df))
        col2.metric("🏢 Companies", df['ticker'].nunique())
        col3.metric("📅 Latest Update", df['created_at'].max().strftime("%Y-%m-%d %H:%M") if pd.notna(df['created_at']).any() else "N/A")

        st.divider()

        # Display summaries in expanders
        st.subheader("📊 Earnings Insights")

        for idx, (_, row) in enumerate(df.iterrows()):
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
                date_str = "Unknown"

            # Color badge for company
            col_ticker, col_date, col_source = st.columns([1, 2, 3])
            with col_ticker:
                st.markdown(f"### 🏢 **{ticker}**")
            with col_date:
                st.caption(f"📅 Report Date: {date_str}")
            with col_source:
                st.caption(f"📝 Source: {source_file}")

            # Expandable summary
            with st.expander(f"Read Analysis →", expanded=(idx == 0)):
                st.markdown(summary)
                if pd.notna(created_at):
                    st.caption(f"Generated: {created_at.strftime('%Y-%m-%d %H:%M:%S UTC') if hasattr(created_at, 'strftime') else created_at}")

            st.divider()

        # Export option
        st.sidebar.divider()
        st.sidebar.subheader("📥 Export")
        csv = df.to_csv(index=False)
        st.sidebar.download_button(
            label="Download as CSV",
            data=csv,
            file_name="earnings_summaries.csv",
            mime="text/csv"
        )

except Exception as e:
    st.error(f"Failed to load earnings summaries: {e}")

