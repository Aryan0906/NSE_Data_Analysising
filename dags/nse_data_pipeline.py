"""
dags/nse_data_pipeline.py
==========================
Airflow DAG: nse_data_pipeline
Schedule: Mon-Fri at 18:30 IST (13:00 UTC) — after NSE market closes.

Task order (left → right):
  extract  →  news_ingest  →  transform  →  load

All tasks run via PythonOperator so the same virtual-env/Docker image used
everywhere. XCom is *not* used — each task reads from the DB layer
written by the previous task (bronze → silver → gold).

Environment variables required (set in Airflow Variables or .env):
  NSE_SYMBOLS, POSTGRES_*, HUGGING_FACE_API_KEY, CHROMADB_*  (see settings.py)
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# Pipeline modules (must be importable inside the Airflow worker)
from backend.pipeline import extract, news_ingest, transform, load
from backend.pipeline.settings import settings

# ─────────────────────────────────────────────────
# Default arguments
# ─────────────────────────────────────────────────

DEFAULT_ARGS = {
    "owner": "nse-data-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

# ─────────────────────────────────────────────────
# DAG definition
# ─────────────────────────────────────────────────

with DAG(
    dag_id="nse_data_pipeline",
    description="Daily ETL: extract → news_ingest → transform → load for NSE instruments",
    default_args=DEFAULT_ARGS,
    schedule="30 13 * * 1-5",   # 18:30 IST = 13:00 UTC, weekdays only
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["nse", "finance", "etl"],
) as dag:

    # ── Task 1: Extract prices + fundamentals from Yahoo Finance / NSE ──

    t_extract = PythonOperator(
        task_id="extract",
        python_callable=extract.run,
        op_kwargs={
            "symbols": settings.nse_symbols,
            # run_date defaults to date.today() inside extract.run()
        },
        doc_md="""
**Extract** — pulls OHLCV + fundamentals from Yahoo Finance for every symbol in
`NSE_SYMBOLS` and writes raw rows into `bronze.raw_prices` and
`bronze.raw_fundamentals`.
        """,
    )

    # ── Task 2: Ingest news from HF datasets ──

    t_news_ingest = PythonOperator(
        task_id="news_ingest",
        python_callable=news_ingest.run,
        op_kwargs={"symbols": settings.nse_symbols},
        doc_md="""
**News Ingest** — streams articles from HuggingFace datasets and writes to
`bronze.raw_news`.  Rate-budgeted by `hf_client.py` (Rule 1).
        """,
    )

    # ── Task 3: Transform bronze → silver ──

    t_transform = PythonOperator(
        task_id="transform",
        python_callable=transform.run,
        op_kwargs={"symbols": settings.nse_symbols},
        doc_md="""
**Transform** — cleans OHLCV, computes technical indicators (SMA/EMA/RSI),
52-week normalisation (Rule 4), z-score outlier flagging (Rule 7), and
enriches news with HF summary + sentiment (Rule 1).
Writes to `silver.prices`, `silver.fundamentals`, `silver.news`.
        """,
    )

    # ── Task 4: Load silver → gold + ChromaDB ──

    t_load = PythonOperator(
        task_id="load",
        python_callable=load.run,
        op_kwargs={"symbols": settings.nse_symbols},
        doc_md="""
**Load** — materialises `gold.stock_summary` and `gold.news_feed`, then
upserts sentence-transformer embeddings into ChromaDB (Rule 2).
        """,
    )

    # ── Wire dependencies ──

    t_extract >> t_news_ingest >> t_transform >> t_load
