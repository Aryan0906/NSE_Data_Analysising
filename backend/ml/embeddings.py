import logging
import os
from typing import List, Dict, Any

from sqlalchemy import create_engine, text
import chromadb
from chromadb.config import Settings as ChromaSettings
from sentence_transformers import SentenceTransformer

from backend.pipeline.settings import settings

logger = logging.getLogger(__name__)

# Rule 7: Model configuration
EMBEDDING_MODEL_NAME = "all-MiniLM-L6-v2"
EMBEDDING_DIMENSION = 384

class EmbeddingManager:
    """
    Manages generating embeddings for raw_news and storing them in ChromaDB.
    Enforces Rule 2 (ChromaDB Persistence) and Rule 7 (Model Metadata check).
    """

    def __init__(self, db_engine=None):
        self.db_engine = db_engine or create_engine(settings.postgres_dsn, pool_pre_ping=True)
        self._ensure_pipeline_metadata()
        self._check_model_metadata()
        
        # Rule 2: Never use in-memory client; connect to persistent service
        logger.info(f"Connecting to ChromaDB at {settings.chromadb_host}:{settings.chromadb_port}")
        self.chroma_client = chromadb.HttpClient(
            host=settings.chromadb_host,
            port=settings.chromadb_port,
            settings=ChromaSettings(allow_reset=True)
        )
        
        self.collection = self.chroma_client.get_or_create_collection(
            name="financial_news",
            metadata={"hnsw:space": "cosine"}
        )
        
        logger.info(f"Loading local SentenceTransformer model: {EMBEDDING_MODEL_NAME}")
        self.model = SentenceTransformer(EMBEDDING_MODEL_NAME)
        
    def _ensure_pipeline_metadata(self):
        """Creates the pipeline_metadata table if it doesn't exist."""
        with self.db_engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS public.pipeline_metadata (
                    config_key VARCHAR(50) PRIMARY KEY,
                    config_value VARCHAR(255) NOT NULL,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """))

    def _check_model_metadata(self):
        """
        Rule 7 check: 
        If stored model != current model -> raise Exception, block embedding run.
        """
        with self.db_engine.begin() as conn:
            # Check model name
            result_name = conn.execute(text("""
                SELECT config_value FROM public.pipeline_metadata WHERE config_key = 'embedding_model_name';
            """)).fetchone()
            
            if not result_name:
                # First run, insert metadata
                conn.execute(text("""
                    INSERT INTO public.pipeline_metadata (config_key, config_value) 
                    VALUES ('embedding_model_name', :model_name),
                           ('embedding_dimension', :dimension);
                """), {
                    "model_name": EMBEDDING_MODEL_NAME, 
                    "dimension": str(EMBEDDING_DIMENSION)
                })
                logger.info(f"Initialized pipeline_metadata with model={EMBEDDING_MODEL_NAME}, dim={EMBEDDING_DIMENSION}")
            else:
                stored_model_name = result_name[0]
                if stored_model_name != EMBEDDING_MODEL_NAME:
                    logger.error(f"Model mismatch! Stored: {stored_model_name}, Current: {EMBEDDING_MODEL_NAME}")
                    raise RuntimeError(
                        f"Rule 7 Violation: Stored embedding model '{stored_model_name}' "
                        f"does not match current model '{EMBEDDING_MODEL_NAME}'."
                    )
                
                result_dim = conn.execute(text("""
                    SELECT config_value FROM public.pipeline_metadata WHERE config_key = 'embedding_dimension';
                """)).fetchone()
                stored_dim = int(result_dim[0]) if result_dim else 0
                
                logger.info(f"Model metadata validated: {stored_model_name} (dim={stored_dim})")

    def get_unembedded_news(self) -> List[Dict[str, Any]]:
        """Fetch news articles that haven't been embedded yet."""
        with self.db_engine.connect() as conn:
            result = conn.execute(text("""
                SELECT id, ticker, title, summary, published_at, url, url_hash 
                FROM public.raw_news 
                WHERE embedded = FALSE
                ORDER BY published_at ASC
                LIMIT 500;
            """))
            return [dict(row._mapping) for row in result]

    def mark_as_embedded(self, ids: List[int]):
        """Mark articles as embedded in PostgreSQL."""
        if not ids:
            return
            
        with self.db_engine.begin() as conn:
            conn.execute(text("""
                UPDATE public.raw_news 
                SET embedded = TRUE 
                WHERE id = ANY(:ids);
            """), {"ids": list(ids)})

    def run_embedding_pipeline(self):
        """End-to-end embedding generation & storage."""
        news_items = self.get_unembedded_news()
        if not news_items:
            logger.info("No new articles to embed.")
            return

        texts_to_embed = []
        ids = []
        metadatas = []
        db_ids = []

        for item in news_items:
            # Combine title and summary for richer embedding
            content = f"{item['title']}. {item['summary'] or ''}"
            texts_to_embed.append(content)
            
            # Rule 6 (cont): ChromaDB upsert uses url_hash as document ID
            ids.append(item['url_hash'])
            db_ids.append(item['id'])
            
            metadatas.append({
                "ticker": item['ticker'],
                "published_at": item['published_at'].isoformat() if item['published_at'] else "",
                "url": item['url'],
                "title": item['title']
            })

        logger.info(f"Generating embeddings for {len(texts_to_embed)} articles...")
        embeddings = self.model.encode(texts_to_embed, convert_to_numpy=True).tolist()

        logger.info(f"Upserting to ChromaDB collection '{self.collection.name}'...")
        self.collection.upsert(
            ids=ids,
            embeddings=embeddings,
            metadatas=metadatas,
            documents=texts_to_embed
        )

        logger.info("Marking articles as embedded in PostgreSQL...")
        self.mark_as_embedded(db_ids)
        logger.info("Embedding pipeline completed successfully.")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    manager = EmbeddingManager()
    manager.run_embedding_pipeline()
