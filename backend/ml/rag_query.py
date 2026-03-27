import logging
from typing import Any, Dict

import chromadb
from chromadb.config import Settings as ChromaSettings
from sentence_transformers import SentenceTransformer

from backend.ml.embeddings import EMBEDDING_MODEL_NAME
from backend.pipeline.hf_client import HFClient
from backend.pipeline.settings import settings

logger = logging.getLogger(__name__)

# Wait, HF Free API doesn't support Mistral-7B nicely sometimes without text-generation.
# We'll use "mistralai/Mistral-7B-Instruct-v0.2"
MISTRAL_MODEL = "mistralai/Mistral-7B-Instruct-v0.2"

class RAGQueryEngine:
    """
    RAG engine using ChromaDB for context and Mistral for generation.
    Enforces Rule 4 (Hallucination Guard).
    """
    
    def __init__(self, db_engine=None):
        self.chroma_client = chromadb.HttpClient(
            host=settings.chromadb_host,
            port=settings.chromadb_port,
            settings=ChromaSettings(allow_reset=True)
        )
        self.collection = self.chroma_client.get_or_create_collection(
            name="financial_news", 
            metadata={"hnsw:space": "cosine"}
        )
        self.embed_model = SentenceTransformer(EMBEDDING_MODEL_NAME)
        self.hf_client = HFClient(db_engine)
        
        # Rule 4 threshold
        self.similarity_threshold = 0.4
        
    def query(self, question: str) -> Dict[str, Any]:
        """
        Answers a user question based on semantic search over local news embeddings.
        """
        logger.info(f"RAG querying for: {question}")
        
        question_embedding = self.embed_model.encode([question])[0].tolist()
        
        # Search ChromaDB
        results = self.collection.query(
            query_embeddings=[question_embedding],
            n_results=3,
            include=["documents", "metadatas", "distances"]
        )
        
        docs_res = results.get('documents') or [[]]
        if not docs_res[0]:
            logger.warning("No documents found in ChromaDB collection.")
            return {"answer": "Insufficient data", "sources": []}
            
        docs = docs_res[0]
        
        dist_res = results.get('distances') or [[1.0]*len(docs)]
        distances = dist_res[0]
        
        meta_res = results.get('metadatas') or [[{}]*len(docs)]
        metadatas = meta_res[0]
        
        # ChromaDB cosine distance: smaller is more similar.
        # But wait, distance is (1 - cosine_similarity). So similarity = 1 - distance.
        # Rule 4: similarity >= 0.4.
        
        valid_docs = []
        sources = []
        
        for doc, distance, meta in zip(docs, distances, metadatas):
            similarity = 1.0 - distance
            if similarity >= self.similarity_threshold:
                valid_docs.append(doc)
                sources.append({
                    "url": meta.get("url", ""),
                    "published_at": meta.get("published_at", ""),
                    "ticker": meta.get("ticker", "")
                })
            else:
                logger.debug(f"Document skipped. Similarity {similarity:.3f} < {self.similarity_threshold}")
                
        # Rule 4: If below threshold -> return "Insufficient data"
        if not valid_docs:
            logger.warning("All retrieved documents below similarity threshold.")
            return {"answer": "Insufficient data", "sources": []}
            
        context = "\n\n".join(valid_docs)
        prompt = f"[INST] You are a helpful financial assistant. Answer the user's question purely based on the context below. If the context does not contain the answer, say 'Insufficient data'.\n\nContext:\n{context}\n\nQuestion:\n{question} [/INST]"
        
        payload = {
            "inputs": prompt,
            "parameters": {"max_new_tokens": 250, "temperature": 0.1, "return_full_text": False}
        }
        
        response = self.hf_client.query(MISTRAL_MODEL, payload)
        answer = "Error generating response"
        
        if response and isinstance(response, list) and "generated_text" in response[0]:
            answer = response[0]["generated_text"].strip()
            
        # Every answer must include source URLs and published dates (Rule 4)
        return {
            "answer": answer,
            "sources": sources
        }
#
