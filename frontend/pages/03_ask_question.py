import streamlit as st

from backend.ml.rag_query import RAGQueryEngine

st.set_page_config(page_title="Ask Question (RAG)", layout="wide")

st.title("🧠 Ask a Financial Question")
st.markdown("Retrieval-Augmented Generation using local **MiniLM** embeddings and **Mistral-7B**.")

# Expose threshold mapping to show Rule 4 visually (read-only)
st.sidebar.markdown("""
### RAG Guardrails
- **Cosine Similarity Threshold:** 0.4
- **Failure state:** *"Insufficient data"*
- Vectors strictly derived from vetted Yahoo Finance RSS feeds.
""")

question = st.text_input("Enter your question (e.g., 'What is TCS net profit for this quarter?')")

if st.button("Ask"):
    if not question.strip():
        st.warning("Please enter a question.")
    else:
        with st.spinner("Searching vector database and generating response..."):
            try:
                engine = RAGQueryEngine()
                response = engine.query(question)
                
                # Rule 4: If similarity threshold fails, we must output insufficient data
                if response["answer"] == "Insufficient data":
                    st.warning("We don't have enough confidence in the context to answer this question. (Rule 4 Guardrail Triggered)")
                else:
                    st.success("Response Generated!")
                    st.markdown(f"**Answer:** {response['answer']}")
                    
                    if response["sources"]:
                        st.subheader("Sources Cited:")
                        for s in response["sources"]:
                            st.write(f"- [{s['ticker']}]({s['url']}) (Published: {s['published_at']})")
            except Exception as e:
                st.error(f"Failed to query model: {e}")
