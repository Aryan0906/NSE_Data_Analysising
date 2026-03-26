import streamlit as st

st.set_page_config(
    page_title="NSE Stock Pipeline",
    page_icon="📈",
    layout="wide"
)

st.title("📈 NSE Stock Market Pipeline")
st.markdown("""
Welcome to the production-ready NSE Stock Data platform. 
Use the sidebar to navigate between different analytical modules.

### Modules Available:
1. **Price Analytics:** View interactive Candlestick charts, 20-day / 50-day SMAs built off gold layer views.
2. **Earnings Summaries:** Browse HF BART generated summaries of PDF earnings call transcripts.
3. **Ask Question (RAG):** Query an LLM powered by Mistral-7B over localized chromadb news embeddings for specific financial insights.
""")

st.info("👈 Please select a page from the sidebar to begin.")
