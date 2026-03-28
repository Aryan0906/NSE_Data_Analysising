"""
frontend/pages/03_ask_question.py
==================================
RAG-based Q&A interface using ChromaDB embeddings and Mistral generation.
Implements Rule 4 (Hallucination Guard) via similarity threshold.
"""

import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

import streamlit as st

from backend.ml.rag_query import RAGQueryEngine

st.set_page_config(page_title="Ask Questions", layout="wide")

st.title("🧠 Ask About Stocks")
st.markdown("Ask questions about your tracked stocks using RAG-powered semantic search.")

# Sidebar info
st.sidebar.header("⚙️ RAG Configuration")
st.sidebar.markdown("""
**Similarity Threshold:** 0.4

If retrieved documents are below this threshold, 
the system returns "Insufficient data" instead of hallucinating.

**Data Source:**
- ChromaDB vector store
- Financial news embeddings
- Mistral-7B generation

**Rule 4 (Hallucination Guard):**
Every answer includes source URLs and publication dates.
""")

# Question input
st.subheader("💬 Ask a Question")

question = st.text_input(
    "Type your question",
    placeholder="e.g., What are the recent developments for HDFC Bank?"
)

ask_button = st.button("🔍 Ask", type="primary")

if ask_button:
    if not question or not question.strip():
        st.warning("Please enter a question.")
    else:
        st.divider()
        
        with st.spinner("Searching knowledge base..."):
            try:
                engine = RAGQueryEngine()
                response = engine.query(question)
                
                answer = response.get("answer", "")
                sources = response.get("sources", [])
                
                if answer == "Insufficient data":
                    st.warning("""
                    ⚠️ **Insufficient data to answer this question.**
                    
                    This means either:
                    - No relevant documents were found in the knowledge base
                    - Retrieved documents did not meet the similarity threshold (0.4)
                    
                    Try rephrasing your question or ensure the pipeline has ingested relevant news.
                    """)
                else:
                    st.success("### Answer")
                    st.markdown(answer)
                    
                    if sources:
                        st.markdown("---")
                        st.markdown("### 📚 Sources")
                        for i, source in enumerate(sources, 1):
                            url = source.get("url", "N/A")
                            published_at = source.get("published_at", "N/A")
                            ticker = source.get("ticker", "")
                            
                            ticker_badge = f"**[{ticker}]** " if ticker else ""
                            st.markdown(f"{i}. {ticker_badge}[{url}]({url})")
                            st.caption(f"Published: {published_at}")
                    else:
                        st.info("No source metadata available for this response.")
                        
            except Exception as e:
                st.error(f"Error querying RAG engine: {e}")
                st.info("Ensure ChromaDB is running and the pipeline has ingested news data.")
