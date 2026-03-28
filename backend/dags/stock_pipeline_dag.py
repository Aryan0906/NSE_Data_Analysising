"""
backend/dags/stock_pipeline_dag.py
====================================
Airflow DAG: stock_pipeline  (Sprint 4)
Schedule : Mon-Fri at 18:30 IST (13:00 UTC) — after NSE market closes.

8-task topology (fan-out after db_init, fan-in before load_gold):

        ┌─────────────────┐
        │    db_init      │   Task 1 — idempotent schema DDL
        └────────┬────────┘
        ┌────────┴────────┐
        ▼                 ▼
  extract_prices    news_ingest         Tasks 2 & 3 — run in parallel
        ▼                 ▼
  transform_prices  transform_news      Tasks 4 & 5 — run in parallel
        └────────┬────────┘
                 ▼
            load_gold                   Task 6 — silver → gold + ChromaDB
                 ▼
           create_views                 Task 7 — materialise analytical SQL views
                 ▼
         validate_schema                Task 8 — star-schema integrity checks

Design decisions
─────────────────
* PythonOperator used throughout so tasks run in the same venv/Docker image.
* No XCom between tasks — each stage reads what the previous one persisted in
  PostgreSQL (bronze → silver → gold).
* All python_callable functions are idempotent (UPSERT / CREATE OR REPLACE).
* transform_prices calls transform_prices() + transform_fundamentals(); transform_news
  calls transform_news() — each only touches its own silver table for clean retries.
* Tasks 7+8 use a lightweight SqlScriptOperator shim that executes the
  checked-in SQL files — keeps SQL out of the DAG file.

Environment variables required (set in Airflow Variables or .env):
  NSE_SYMBOLS, POSTGRES_*, HUGGING_FACE_API_KEY / HF_API_TOKEN,
  CHROMADB_*, NEWS_API_KEY  (see backend/pipeline/settings.py)
"""

from __future__ import annotations

import logging
import os
import pathlib
from datetime import datetime, timedelta
from typing import Optional

from airflow import DAG
from airflow.operators.python import PythonOperator

# Pipeline modules — must be importable inside the Airflow worker container.
# The docker-compose mounts ./backend → /opt/airflow/backend so the import
# path is backend.pipeline.*
from backend.pipeline import db_init, extract, news_ingest, load
from backend.pipeline import transform
from backend.pipeline.transform import (
    transform_prices,
    transform_fundamentals,
    transform_news as _transform_news_fn,
)
from backend.pipeline.settings import settings

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Paths to checked-in SQL helper files
# ─────────────────────────────────────────────────────────────────────────────

_SQL_DIR = pathlib.Path(__file__).parent.parent / "sql"
_CREATE_VIEWS_SQL = _SQL_DIR / "create_views.sql"
_VALIDATE_SQL = _SQL_DIR / "validate_star_schema.sql"

# ─────────────────────────────────────────────────────────────────────────────
# Default arguments (applied to every task unless overridden)
# ─────────────────────────────────────────────────────────────────────────────

DEFAULT_ARGS: dict = {
    "owner": "nse-data-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

# ─────────────────────────────────────────────────────────────────────────────
# Task callables
# ─────────────────────────────────────────────────────────────────────────────


def _task_db_init(**_: object) -> None:
    """Task 1 — Ensure all schemas, tables, and roles exist (idempotent DDL).

    Delegates to backend.pipeline.db_init.run() which executes every DDL
    statement inside DDL_STATEMENTS with autocommit=True.  Safe to run on
    every DAG execution; CREATE … IF NOT EXISTS guards prevent collisions.
    """
    db_init.run()


def _task_extract_prices(symbols: list[str], **_: object) -> int:
    """Task 2 — Pull OHLCV + fundamentals from Yahoo Finance → bronze layer."""
    return extract.run(symbols=symbols)


def _task_news_ingest(symbols: list[str], **_: object) -> int:
    """Task 3 — Fetch latest headlines from NewsAPI → bronze.raw_news."""
    return news_ingest.run(symbols=symbols)


def _task_transform_prices(symbols: list[str], **_: object) -> dict[str, int]:
    """Task 4 — Clean prices/fundamentals, compute indicators → silver layer.

    Calls the module-level transform_prices() + transform_fundamentals() for
    each symbol so it is independently retriable without re-running news NLP.
    """
    from datetime import date
    run_date = date.today()
    totals: dict[str, int] = {"prices": 0, "fundamentals": 0}
    for sym in symbols:
        totals["prices"] += transform_prices(sym, run_date)
        totals["fundamentals"] += transform_fundamentals(sym)
    logger.info("transform_prices complete: %s", totals)
    return totals


def _task_transform_news(symbols: list[str], **_: object) -> int:
    """Task 5 — Summarise + sentiment-score news via HF API → silver.news.

    Calls transform_news() for each symbol.  Completely independent of the
    prices path so it can retry in isolation after a NewsAPI or HF hiccup.
    """
    total = 0
    for sym in symbols:
        total += _transform_news_fn(sym)
    logger.info("transform_news complete: %d rows", total)
    return total


def _task_load_gold(symbols: list[str], **_: object) -> int:
    """Task 6 — Materialise gold.stock_summary, gold.news_feed, upsert ChromaDB."""
    return load.run(symbols=symbols)


def _task_create_views(**_: object) -> None:
    """Task 7 — (Re)create the five analytical gold.v_* views from SQL file.

    Reads backend/sql/create_views.sql and executes it via psycopg2 so the
    views are always in sync with the checked-in SQL definition.
    """
    import psycopg2

    sql_text = _CREATE_VIEWS_SQL.read_text(encoding="utf-8")
    conn = psycopg2.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        dbname=settings.postgres_db,
        user=settings.postgres_user,
        password=settings.postgres_password,
        connect_timeout=10,
    )
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(sql_text)
        logger.info("create_views: executed %s successfully", _CREATE_VIEWS_SQL.name)
    finally:
        conn.close()


def _task_validate_schema(**_: object) -> None:
    """Task 8 — Run star-schema integrity checks; RAISE WARNING if issues found.

    Executes backend/sql/validate_star_schema.sql which uses DO $$ … $$
    blocks to assert FK integrity, check for orphans, duplicate facts,
    silver→gold coverage gaps, and dim_date calendar continuity.
    Logs any WARNING notices emitted by PostgreSQL; does NOT fail the DAG
    (warnings are advisory) unless the SQL itself raises an exception.
    """
    import psycopg2
    import psycopg2.extras

    sql_text = _VALIDATE_SQL.read_text(encoding="utf-8")

    conn = psycopg2.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        dbname=settings.postgres_db,
        user=settings.postgres_user,
        password=settings.postgres_password,
        connect_timeout=10,
    )
    # Surface RAISE NOTICE / RAISE WARNING messages from PostgreSQL as Python logs
    conn.notices.clear()  # type: ignore[attr-defined]

    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(sql_text)
        for notice in conn.notices:  # type: ignore[attr-defined]
            level = logging.WARNING if "WARNING" in notice.upper() else logging.INFO
            logger.log(level, "PG: %s", notice.strip())
        logger.info("validate_schema: %s completed", _VALIDATE_SQL.name)
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# DAG definition
# ─────────────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="stock_pipeline",
    description=(
        "NSE daily ETL — 8 tasks: db_init → [extract_prices ‖ news_ingest] → "
        "[transform_prices ‖ transform_news] → load_gold → create_views → validate_schema"
    ),
    default_args=DEFAULT_ARGS,
    schedule="30 13 * * 1-5",   # 18:30 IST = 13:00 UTC, weekdays only
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["nse", "finance", "etl", "sprint4"],
) as dag:

    # ── Shared op_kwargs ──────────────────────────────────────────────────────
    _symbols_kwargs = {"symbols": settings.nse_symbols}

    # ── Task 1: Schema initialisation (must always run first) ─────────────────

    t1_db_init = PythonOperator(
        task_id="db_init",
        python_callable=_task_db_init,
        doc_md="""\
**db_init** — Idempotent DDL runner.
Creates/migrates bronze, silver, gold schemas; tables; roles; grants.
Must complete before any data-touching task starts.
""",
    )

    # ── Task 2: Extract prices + fundamentals (runs in parallel with Task 3) ──

    t2_extract_prices = PythonOperator(
        task_id="extract_prices",
        python_callable=_task_extract_prices,
        op_kwargs=_symbols_kwargs,
        doc_md="""\
**extract_prices** — Pulls OHLCV history and key fundamentals from
official NSE India API for every symbol in `NSE_SYMBOLS`.
Writes to `bronze.raw_prices` and `bronze.raw_fundamentals` (UPSERT).
""",
    )

    # ── Task 3: News ingestion (parallel with Task 2) ─────────────────────────

    t3_news_ingest = PythonOperator(
        task_id="news_ingest",
        python_callable=_task_news_ingest,
        op_kwargs=_symbols_kwargs,
        doc_md="""\
**news_ingest** — Fetches stock-market headlines from NewsAPI.
Deduplicates on SHA-256 content hash (Rule 6) and writes to
`bronze.raw_news`.  Exits cleanly if `NEWS_API_KEY` is absent.
""",
    )

    # ── Task 4: Transform prices (runs after Task 2 only) ────────────────────

    t4_transform_prices = PythonOperator(
        task_id="transform_prices",
        python_callable=_task_transform_prices,
        op_kwargs=_symbols_kwargs,
        doc_md="""\
**transform_prices** — Cleans OHLCV, computes SMA/EMA/RSI, normalises
52-week returns (Rule 4), flags z-score outliers (Rule 7).
Writes to `silver.prices` and `silver.fundamentals`.
""",
    )

    # ── Task 5: Transform news with HF sentiment (runs after Task 3 only) ────

    t5_transform_news = PythonOperator(
        task_id="transform_news",
        python_callable=_task_transform_news,
        op_kwargs=_symbols_kwargs,
        doc_md="""\
**transform_news** — Sends headlines through HuggingFace inference API
(rate-budgeted, Rule 1) for BART summarisation and FinBERT sentiment.
Deduplicates and writes to `silver.news`.
""",
    )

    # ── Task 6: Load silver → gold + ChromaDB ────────────────────────────────

    t6_load_gold = PythonOperator(
        task_id="load_gold",
        python_callable=_task_load_gold,
        op_kwargs=_symbols_kwargs,
        doc_md="""\
**load_gold** — Materialises `gold.stock_summary`, `gold.news_feed`,
and the star-schema dimension/fact tables.  Upserts sentence-transformer
embeddings into ChromaDB (Rule 2).
""",
    )

    # ── Task 7: Create / refresh analytical views ─────────────────────────────

    t7_create_views = PythonOperator(
        task_id="create_views",
        python_callable=_task_create_views,
        doc_md="""\
**create_views** — Executes `backend/sql/create_views.sql` to
`CREATE OR REPLACE` the five analytical gold views:
`v_latest_prices`, `v_market_pulse`, `v_sector_momentum`,
`v_news_sentiment`, `v_portfolio_risk`.
""",
    )

    # ── Task 8: Star-schema integrity validation ───────────────────────────────

    t8_validate_schema = PythonOperator(
        task_id="validate_schema",
        python_callable=_task_validate_schema,
        doc_md="""\
**validate_schema** — Executes `backend/sql/validate_star_schema.sql`.
Checks FK integrity, orphan dimensions, duplicate facts, silver→gold
coverage gaps, and dim_date calendar continuity.  Emits PostgreSQL
NOTICE/WARNING messages as Airflow log entries.
""",
    )

    # ─────────────────────────────────────────────────────────────────────────
    # Explicit dependency wiring
    # ─────────────────────────────────────────────────────────────────────────
    #
    # Phase A — init:          db_init
    # Phase B — ingest:        extract_prices   news_ingest        (parallel)
    # Phase C — transform:     transform_prices transform_news     (parallel)
    # Phase D — consolidate:   load_gold
    # Phase E — finalise:      create_views → validate_schema      (serial)
    #
    # Fan-out after db_init:
    t1_db_init >> [t2_extract_prices, t3_news_ingest]

    # extract_prices feeds only its transform; news_ingest feeds only its transform:
    t2_extract_prices >> t4_transform_prices
    t3_news_ingest    >> t5_transform_news

    # Fan-in: load_gold waits for BOTH transforms to finish:
    [t4_transform_prices, t5_transform_news] >> t6_load_gold

    # Serial finalisation:
    t6_load_gold >> t7_create_views >> t8_validate_schema
