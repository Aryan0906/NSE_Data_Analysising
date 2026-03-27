import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

import streamlit as st
import pandas as pd

from frontend.components.db_connector import fetch_data

st.set_page_config(page_title="Stock Insights", layout="wide")

st.title("🧠 Ask About Stocks")
st.markdown("Ask questions about your tracked stocks and get instant answers.")

# Sidebar info
st.sidebar.markdown("""
### Available Data
- **Price:** Current, 52W High/Low
- **Returns:** Daily change
- **Fundamentals:** P/E, Market Cap, ROE
- **Dividends:** Yield percentage
- **News:** Recent headlines

### Example Questions
- "What is HDFC price?"
- "Compare all stocks"
- "Best performer today"
- "TCS P/E ratio"
""")

# Question input section
st.subheader("💬 Ask a Question")

question = st.text_input(
    "Type your question", 
    placeholder="e.g., What is HDFC Bank's current price?"
)

col1, col2 = st.columns([1, 4])
ask_button = col1.button("🔍 Ask", type="primary", use_container_width=True)

# Quick question buttons
st.markdown("**Or try:**")
qcol1, qcol2, qcol3, qcol4 = st.columns(4)
quick_q1 = qcol1.button("📊 Compare")
quick_q2 = qcol2.button("📈 Best Stock")
quick_q3 = qcol3.button("💰 Market Cap")
quick_q4 = qcol4.button("📉 Worst Stock")

# Handle quick questions
if quick_q1:
    question = "compare all stocks"
if quick_q2:
    question = "best performer"
if quick_q3:
    question = "highest market cap"
if quick_q4:
    question = "worst performer"

def answer_question(question, all_stocks):
    """Answer a question based on available stock data."""
    question_lower = question.lower()
    
    # Stock name aliases and display names
    stock_info = {
        'RELIANCE.NS': {'aliases': ['reliance', 'ril', 'reliance industries'], 'display': 'Reliance Industries'},
        'HDFCBANK.NS': {'aliases': ['hdfc', 'hdfcbank', 'hdfc bank'], 'display': 'HDFC Bank'},
        'TCS.NS': {'aliases': ['tcs', 'tata consultancy', 'tata'], 'display': 'TCS'},
        'INFY.NS': {'aliases': ['infosys', 'infy'], 'display': 'Infosys'},
        'ITC.NS': {'aliases': ['itc'], 'display': 'ITC'},
    }
    
    def get_display_name(symbol):
        return stock_info.get(symbol, {}).get('display', symbol.replace('.NS', ''))
    
    # Check if asking about a specific stock
    stock_found = None
    
    # Try to match using aliases
    for _, stock in all_stocks.iterrows():
        symbol = stock['symbol']
        info = stock_info.get(symbol, {'aliases': [symbol.replace('.NS', '').lower()]})
        for alias in info['aliases']:
            if alias in question_lower:
                stock_found = stock
                break
        if stock_found is not None:
            break
    
    # If asking about a specific stock
    if stock_found is not None:
        display_name = get_display_name(stock_found['symbol'])
        st.subheader(f"📊 {display_name}")
        
        # Check what they're asking about
        if "profit" in question_lower or "earnings" in question_lower or "quarter" in question_lower or "revenue" in question_lower:
            st.warning(f"⚠️ Quarterly earnings/profit data is not available. Here's what we have for **{display_name}**:")
        
        # Helper function to safely format values
        def fmt_price(val):
            try:
                return f"₹{float(val):,.2f}" if pd.notna(val) else "N/A"
            except:
                return "N/A"
        
        def fmt_pct(val):
            try:
                return f"{float(val)*100:.2f}%" if pd.notna(val) else "N/A"
            except:
                return "N/A"
        
        def fmt_mcap(val):
            try:
                if pd.notna(val):
                    v = float(val)
                    return f"₹{v/1e12:.2f}T" if v >= 1e12 else f"₹{v/1e9:.1f}B"
                return "N/A"
            except:
                return "N/A"
        
        def fmt_num(val):
            try:
                return f"{float(val):.2f}" if pd.notna(val) else "N/A"
            except:
                return "N/A"
        
        col1, col2 = st.columns(2)
        with col1:
            st.metric("💰 Current Price", fmt_price(stock_found['close_price']))
            st.metric("📈 Daily Return", fmt_pct(stock_found['daily_return']))
            st.metric("📊 P/E Ratio", fmt_num(stock_found['trailing_pe']))
            st.metric("🏦 Market Cap", fmt_mcap(stock_found['market_cap']))
        with col2:
            st.metric("📈 52W High", fmt_price(stock_found['fifty_two_week_high']))
            st.metric("📉 52W Low", fmt_price(stock_found['fifty_two_week_low']))
            st.metric("💵 Dividend Yield", fmt_pct(stock_found['dividend_yield']))
            st.metric("📊 ROE", fmt_pct(stock_found['return_on_equity']))
        return True
    
    # Compare all stocks
    if "compare" in question_lower or "all" in question_lower:
        st.subheader("📊 Stock Comparison")
        display_df = all_stocks.copy()
        display_df['close_price'] = display_df['close_price'].apply(lambda x: f"₹{float(x):,.2f}" if pd.notna(x) else "N/A")
        display_df['daily_return'] = display_df['daily_return'].apply(lambda x: f"{float(x)*100:.2f}%" if pd.notna(x) else "N/A")
        display_df['trailing_pe'] = display_df['trailing_pe'].apply(lambda x: f"{float(x):.2f}" if pd.notna(x) else "N/A")
        display_df['market_cap'] = display_df['market_cap'].apply(lambda x: f"₹{float(x)/1e9:.1f}B" if pd.notna(x) else "N/A")
        display_df['symbol'] = display_df['symbol'].apply(get_display_name)
        display_df.columns = ['Stock', 'Price', 'Daily Return', 'P/E', 'Market Cap', '52W High', '52W Low', 'Div Yield', 'ROE']
        st.dataframe(display_df[['Stock', 'Price', 'Daily Return', 'P/E', 'Market Cap']], use_container_width=True)
        return True
    
    # Best performer
    if "best" in question_lower or ("top" in question_lower and "performer" in question_lower):
        st.subheader("🏆 Best Performer Today")
        all_stocks['daily_return_val'] = all_stocks['daily_return'].apply(lambda x: float(x) if pd.notna(x) else -999)
        best = all_stocks.loc[all_stocks['daily_return_val'].idxmax()]
        best_name = get_display_name(best['symbol'])
        st.success(f"**{best_name}** with **{float(best['daily_return'])*100:.2f}%** daily return")
        col1, col2, col3 = st.columns(3)
        col1.metric("Stock", best_name)
        col2.metric("Daily Return", f"{float(best['daily_return'])*100:.2f}%")
        col3.metric("Price", f"₹{float(best['close_price']):,.2f}")
        return True
    
    # Worst performer
    if "worst" in question_lower or "lowest" in question_lower:
        st.subheader("📉 Worst Performer Today")
        all_stocks['daily_return_val'] = all_stocks['daily_return'].apply(lambda x: float(x) if pd.notna(x) else 999)
        worst = all_stocks.loc[all_stocks['daily_return_val'].idxmin()]
        worst_name = get_display_name(worst['symbol'])
        st.error(f"**{worst_name}** with **{float(worst['daily_return'])*100:.2f}%** daily return")
        col1, col2, col3 = st.columns(3)
        col1.metric("Stock", worst_name)
        col2.metric("Daily Return", f"{float(worst['daily_return'])*100:.2f}%")
        col3.metric("Price", f"₹{float(worst['close_price']):,.2f}")
        return True
    
    # Highest market cap
    if "market cap" in question_lower or "largest" in question_lower or "biggest" in question_lower:
        st.subheader("🏦 Highest Market Cap")
        all_stocks['mcap_val'] = all_stocks['market_cap'].apply(lambda x: float(x) if pd.notna(x) else 0)
        largest = all_stocks.loc[all_stocks['mcap_val'].idxmax()]
        largest_name = get_display_name(largest['symbol'])
        mcap = float(largest['market_cap'])
        mcap_str = f"₹{mcap/1e12:.2f}T" if mcap >= 1e12 else f"₹{mcap/1e9:.1f}B"
        st.success(f"**{largest_name}** with **{mcap_str}** market cap")
        return True
    
    # P/E ratios
    if "pe" in question_lower or "p/e" in question_lower or "valuation" in question_lower:
        st.subheader("📊 P/E Ratios")
        for _, stock in all_stocks.iterrows():
            pe = f"{float(stock['trailing_pe']):.2f}" if pd.notna(stock['trailing_pe']) else "N/A"
            st.write(f"**{stock['symbol']}**: P/E = {pe}")
        return True
    
    # Dividend
    if "dividend" in question_lower:
        st.subheader("💵 Dividend Yields")
        for _, stock in all_stocks.iterrows():
            div = f"{float(stock['dividend_yield'])*100:.2f}%" if pd.notna(stock['dividend_yield']) else "N/A"
            st.write(f"**{stock['symbol']}**: {div}")
        return True
    
    # Price query
    if "price" in question_lower:
        st.subheader("💰 Stock Prices")
        for _, stock in all_stocks.iterrows():
            price = f"₹{float(stock['close_price']):,.2f}" if pd.notna(stock['close_price']) else "N/A"
            ret = f"({float(stock['daily_return'])*100:+.2f}%)" if pd.notna(stock['daily_return']) else ""
            st.write(f"**{stock['symbol']}**: {price} {ret}")
        return True
    
    return False

# Process question
if ask_button or quick_q1 or quick_q2 or quick_q3 or quick_q4:
    if not question or not question.strip():
        st.warning("Please enter a question.")
    else:
        st.divider()
        
        with st.spinner("Analyzing..."):
            try:
                all_stocks_query = """
                    SELECT symbol, close_price, daily_return, trailing_pe, market_cap,
                           fifty_two_week_high, fifty_two_week_low, dividend_yield, return_on_equity
                    FROM gold.stock_summary
                    ORDER BY symbol
                """
                all_stocks = fetch_data(all_stocks_query)
                
                answered = answer_question(question, all_stocks)
                
                if not answered:
                    st.info("""
                    **I couldn't understand that question.** Try asking:
                    - "What is HDFC price?" (or TCS, RELIANCE, INFY, ITC)
                    - "Compare all stocks"
                    - "Which is the best performer?"
                    - "Show P/E ratios"
                    - "Dividend yields"
                    """)
                    
            except Exception as e:
                st.error(f"Error: {e}")

st.divider()

# Stock selector for detailed view
st.subheader("📋 Detailed Stock View")
query_tickers = """SELECT DISTINCT symbol FROM gold.stock_summary ORDER BY symbol"""
try:
    tickers_df = fetch_data(query_tickers)
    tickers = tickers_df['symbol'].tolist() if not tickers_df.empty else []
except:
    tickers = []

if tickers:
    selected_stock = st.selectbox("Select a stock", tickers)
    
    if selected_stock:
        base_symbol = selected_stock.replace('.NS', '')

        prices_query = f"""
            SELECT trade_date, close_price
            FROM silver.prices
            WHERE symbol = '{base_symbol}'
            ORDER BY trade_date DESC
            LIMIT 30
        """
        news_query = f"""
            SELECT headline, published_at, sentiment_score, source
            FROM gold.news_feed 
            WHERE symbol = '{base_symbol}'
            ORDER BY published_at DESC
            LIMIT 5
        """
        
        try:
            prices_df = fetch_data(prices_query)
            news_df = fetch_data(news_query)
            
            if not prices_df.empty:
                st.subheader(f"📈 {selected_stock} - 30 Day Price")
                prices_df = prices_df.sort_values('trade_date')
                st.line_chart(prices_df.set_index('trade_date')['close_price'])
            
            if not news_df.empty:
                st.subheader("📰 Recent News")
                for _, news in news_df.iterrows():
                    sentiment = news.get('sentiment_score')
                    icon = "🟢" if pd.notna(sentiment) and float(sentiment) > 0.1 else "🔴" if pd.notna(sentiment) and float(sentiment) < -0.1 else "🟡"
                    pub_date = str(news.get('published_at', ''))[:10]
                    st.markdown(f"{icon} **{news['headline']}**")
                    st.caption(f"{pub_date} • {news.get('source', 'Unknown')}")
                    
        except Exception as e:
            st.error(f"Error: {e}")
