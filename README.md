# Production NSE Stock Market Pipeline

A production-ready data pipeline and ML layer capturing NSE stock data, analyzing PDF earnings statements, and embedding daily news for a locally-served RAG application.

## 🏗 Stack Architecture

```mermaid
flowchart TD
    %% Data Sources
    NSE[(NSE India API)]
    RSS[(Yahoo RSS)]
    PDF[(Earnings PDFs)]

    %% Orchestration
    Airflow[Apache Airflow DAG]

    %% Ingestion Layer
    subgraph Bronze [Raw / Bronze Layer]
        Ex[extract.py]
        News[news_ingest.py]
        PDFEx[pdf_extract.py\n+ OCR Fallback]
    end

    %% Transformation Layer
    subgraph Silver [Clean / Silver Layer]
        Tx[transform.py\nPydantic Validation]
        Emb[embeddings.py\nMiniLM]
    end

    %% Storage Layer
    subgraph Storage [Databases]
        PG[(PostgreSQL 15)]
        VDB[(ChromaDB\nPersistent)]
    end

    %% Gold & ML Layer
    subgraph Gold [Gold Layer & ML]
        Ld[load.py\nSCD Type 2]
        Sum[summarise.py\nBART via HF]
    end

    %% Frontend UI
    UI[[Streamlit App]]
    RAG[rag_query.py\nMistral-7B]

    %% Edges
    NSE --> Ex
    RSS --> News
    PDF --> PDFEx

    Ex --> Tx
    Tx --> Ld
    Ld --> PG

    News --> Emb
    Emb --> VDB
    VDB <--> RAG

    PDFEx --> Sum
    Sum --> PG

    PG --> UI
    RAG --> UI
    
    Airflow -.- Bronze
    Airflow -.- Silver
    Airflow -.- Gold
```

## 🔒 The 8 Hardened Production Rules Evaluated

1. **HF API Budget:** Hard-capped at 80 calls/day across embeddings/summaries, managed transactionally in Postgres (`hf_api_budget`).
2. **ChromaDB Persistence:** Runs explicitly as a distinct `chromadb_data` Docker volume. Ephemeral clients strictly rejected.
3. **OCR Fallback Guard:** `pdf_extract.py` checks parsing text density (`>= 200` chars); falls back to `pytesseract` automatically, raising safe errors upon total unreadability.
4. **RAG Hallucination Guard:** Embeddings enforce a threshold mapping (`cosine similarity >= 0.4`). Empty/unrelated context hard-returns *"Insufficient Data"* over wild guessing, and always links back to specific hashed RSS URLs.
5. **Idempotent DAG Dependencies:** Explicit chains ensure `db_init` spins up cleanly before ANY table dependencies, and ML summary jobs cannot run until Gold dimension loads complete successfully.
6. **News Deduplication:** Feed ingestion strictly hashes source URLs via SHA256 mapping. Postgres unique constraints handle `ON CONFLICT DO NOTHING`.
7. **Model Metadata Guard:** Pipeline checks `pipeline_metadata` dimensions and model names at startup to block rogue local model dimension swaps into ChromaDB.
8. **SQL Injection Guard:** Streamlit UI utilizes a distinct scoped `readonly_user`. Internal ML mapping queries use Regex filtering to drop `DELETE`/`DROP` statements and safely inject `LIMIT 1000`.

## 🚀 Quickstart Development

```bash
# 1. Spin up dependencies
docker-compose up -d

# 2. Setup Virtual Environment
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt

# 3. Create a .env file (see settings.py for variables)
# POSTGRES_USER, POSTGRES_PASSWORD, HF_API_TOKEN, etc.

# 4. Initialize Database
python -m backend.pipeline.db_init

# 5. Run the Streamlit UI
streamlit run frontend/app.py
```
