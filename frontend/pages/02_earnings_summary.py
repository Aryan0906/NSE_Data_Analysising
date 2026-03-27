import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

import streamlit as st
import pandas as pd
from datetime import datetime

from frontend.components.db_connector import fetch_data

st.set_page_config(page_title="Market News", layout="wide")

st.title("📰 Market News Feed")
st.markdown("Latest news for your tracked stocks")

query = """
    SELECT 
        symbol as ticker,
        published_at,
        headline,
        source,
        url,
        COALESCE(summary, headline) as summary,
        sentiment_score
    FROM gold.news_feed
    WHERE published_at IS NOT NULL
    ORDER BY published_at DESC
    LIMIT 200
"""

try:
    df = fetch_data(query)
    
    if df.empty:
        st.info("📭 No news yet. Add NEWS_API_KEY to .env and run the pipeline.")
    else:
        # Sidebar filters
        st.sidebar.header("🔍 Filters")
        companies = ["All Stocks"] + sorted(df['ticker'].unique().tolist())
        selected = st.sidebar.selectbox("Stock", companies)
        
        if selected != "All Stocks":
            df = df[df['ticker'] == selected]
        
        # Summary stats
        col1, col2, col3 = st.columns(3)
        col1.metric("📊 Total Articles", len(df))
        col2.metric("🏢 Companies", df['ticker'].nunique())
        
        # Sentiment overview
        if 'sentiment_score' in df.columns and df['sentiment_score'].notna().any():
            avg_sentiment = df['sentiment_score'].mean()
            sentiment_label = "🟢 Positive" if avg_sentiment > 0.1 else "🔴 Negative" if avg_sentiment < -0.1 else "🟡 Neutral"
            col3.metric("📈 Sentiment", sentiment_label)
        else:
            col3.metric("📈 Sentiment", "N/A")
        
        st.divider()
        
        # News cards
        for _, row in df.iterrows():
            # Format date nicely
            pub_date = row['published_at']
            if pd.notna(pub_date):
                if isinstance(pub_date, str):
                    try:
                        pub_date = datetime.fromisoformat(pub_date.replace('Z', '+00:00'))
                    except:
                        pass
                if hasattr(pub_date, 'strftime'):
                    date_str = pub_date.strftime("%b %d, %Y • %I:%M %p")
                else:
                    date_str = str(pub_date)[:16]
            else:
                date_str = "Unknown date"
            
            # Sentiment indicator
            sentiment = row.get('sentiment_score')
            if pd.notna(sentiment):
                if sentiment > 0.1:
                    sentiment_icon = "🟢"
                elif sentiment < -0.1:
                    sentiment_icon = "🔴"
                else:
                    sentiment_icon = "🟡"
            else:
                sentiment_icon = "⚪"
            
            # News card
            with st.container():
                st.markdown(f"""
                <div style="padding: 1rem; border-radius: 8px; border: 1px solid #ddd; margin-bottom: 1rem; background: #fafafa;">
                    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.5rem;">
                        <span style="background: #1f77b4; color: white; padding: 2px 8px; border-radius: 4px; font-size: 0.8rem; font-weight: bold;">{row['ticker']}</span>
                        <span style="color: #666; font-size: 0.85rem;">{date_str} {sentiment_icon}</span>
                    </div>
                    <h4 style="margin: 0.5rem 0; color: #333;">{row['headline']}</h4>
                    <p style="color: #555; font-size: 0.9rem; margin: 0.5rem 0;">{row['summary']}</p>
                    <div style="color: #888; font-size: 0.8rem;">Source: {row['source'] or 'Unknown'}</div>
                </div>
                """, unsafe_allow_html=True)
                
                if row.get('url'):
                    st.link_button("🔗 Read Full Article", row['url'], use_container_width=False)
                
                st.write("")  # spacing
                
except Exception as e:
    st.error(f"Failed to load news: {e}")
