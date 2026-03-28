"""
backend/pipeline/db_init.py
============================
Task 1 of the Airflow DAG — runs every execution to ensure the schema is
correct before any ETL step proceeds.

Rules enforced:
  Rule 5 : This task is ALWAYS the first in the DAG chain.
  Rule 8 : SQL guard — SELECT-only role (nse_reader) granted here.

Design decisions:
  - Idempotent: safe to re-run (CREATE TABLE IF NOT EXISTS, etc.)
  - Star-schema: fact_prices + dimension tables + SCD Type 2 on dim_companies
  - All timestamps stored as TIMESTAMP WITH TIME ZONE (UTC)
  - Composite unique constraints to prevent duplicate rows on re-runs
"""

from __future__ import annotations

import logging
import os
import sys
from contextlib import contextmanager
from typing import Generator

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import connection as PgConnection

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | db_init | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)

# ─────────────────────────────────────────────────
# Connection helpers
# ─────────────────────────────────────────────────

def _conn_params() -> dict:
    return {
        "host":     os.environ["POSTGRES_HOST"],
        "port":     int(os.getenv("POSTGRES_PORT", 5432)),
        "dbname":   os.environ["POSTGRES_DB"],
        "user":     os.environ["POSTGRES_USER"],
        "password": os.environ["POSTGRES_PASSWORD"],
        "connect_timeout": 10,
    }


@contextmanager
def get_connection() -> Generator[PgConnection, None, None]:
    conn = psycopg2.connect(**_conn_params())
    conn.autocommit = True          # DDL statements need autocommit
    try:
        yield conn
    finally:
        conn.close()


# ─────────────────────────────────────────────────
# Schema DDL
# ─────────────────────────────────────────────────

DDL_STATEMENTS = [
    # ── Extensions ────────────────────────────────────────
    "CREATE EXTENSION IF NOT EXISTS pg_trgm;",   # fuzzy text search
    "CREATE EXTENSION IF NOT EXISTS pgcrypto;",  # gen_random_uuid()

    # ── Schemas ───────────────────────────────────────────
    "CREATE SCHEMA IF NOT EXISTS bronze;",   # raw ingest
    "CREATE SCHEMA IF NOT EXISTS silver;",   # cleaned / validated
    "CREATE SCHEMA IF NOT EXISTS gold;",     # star-schema facts & dims

    # ═══════════════════════════════════════════════════════
    # BRONZE — raw, append-only landing tables
    # ═══════════════════════════════════════════════════════

    """
    CREATE TABLE IF NOT EXISTS bronze.raw_prices (
        id              BIGSERIAL PRIMARY KEY,
        symbol          VARCHAR(20)   NOT NULL,
        trade_date      DATE          NOT NULL,
        open_price      NUMERIC(14,4),
        high_price      NUMERIC(14,4),
        low_price       NUMERIC(14,4),
        close_price     NUMERIC(14,4),
        adj_close_price NUMERIC(14,4),
        volume          BIGINT,
        source          VARCHAR(50)   NOT NULL DEFAULT 'nse',
        ingested_at     TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
        CONSTRAINT uq_bronze_prices UNIQUE (symbol, trade_date, source)
    );
    """,

    """
    CREATE TABLE IF NOT EXISTS bronze.raw_fundamentals (
        id              BIGSERIAL PRIMARY KEY,
        symbol          VARCHAR(20)   NOT NULL,
        report_period   DATE          NOT NULL,   -- quarter-end date
        metric_name     VARCHAR(100)  NOT NULL,
        metric_value    NUMERIC(20,4),
        unit            VARCHAR(20),
        source          VARCHAR(50)   NOT NULL DEFAULT 'nse',
        ingested_at     TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
        CONSTRAINT uq_bronze_fundamentals UNIQUE (symbol, report_period, metric_name, source)
    );
    """,

    """
    CREATE TABLE IF NOT EXISTS bronze.raw_news (
        id              BIGSERIAL PRIMARY KEY,
        content_hash    CHAR(64)      NOT NULL,   -- Rule 6: SHA-256 dedup
        symbol          VARCHAR(20)   NOT NULL,
        headline        TEXT          NOT NULL,
        source          VARCHAR(200),
        published_at    TIMESTAMPTZ,
        url             TEXT,
        raw_body        TEXT,
        ingested_at     TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
        CONSTRAINT uq_bronze_news_hash UNIQUE (content_hash)
    );
    """,

    # ═══════════════════════════════════════════════════════
    # SILVER — cleaned / validated
    # ═══════════════════════════════════════════════════════

    """
    CREATE TABLE IF NOT EXISTS silver.prices (
        id              BIGSERIAL PRIMARY KEY,
        symbol          VARCHAR(20)   NOT NULL,
        trade_date      DATE          NOT NULL,
        -- OHLCV (nullable: transform writes None for missing bars)
        open_price      NUMERIC(14,4),
        high_price      NUMERIC(14,4),
        low_price       NUMERIC(14,4),
        close_price     NUMERIC(14,4),
        adj_close_price NUMERIC(14,4),
        volume          BIGINT        CHECK (volume >= 0),
        -- Returns (Rule 7: is_outlier flagged when |z-score| > 4)
        daily_return    NUMERIC(10,6),
        log_return      NUMERIC(10,6),
        is_outlier      BOOLEAN       NOT NULL DEFAULT FALSE,
        -- Technical indicators
        sma_20          NUMERIC(14,4),
        sma_50          NUMERIC(14,4),
        sma_200         NUMERIC(14,4),
        ema_20          NUMERIC(14,4),
        rsi_14          NUMERIC(6,4),
        -- Normalised close (Rule 4: 52-week rolling [0, 1])
        normalised_close NUMERIC(8,4),
        -- 52-week rolling window
        high_52w        NUMERIC(14,4),
        low_52w         NUMERIC(14,4),
        price_range_52w NUMERIC(14,4),
        -- Audit
        validated_at    TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
        updated_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
        CONSTRAINT uq_silver_prices UNIQUE (symbol, trade_date)
    );
    """,

    """
    CREATE TABLE IF NOT EXISTS silver.fundamentals (
        id              BIGSERIAL PRIMARY KEY,
        symbol          VARCHAR(20)   NOT NULL,
        report_period   DATE          NOT NULL,
        metric_name     VARCHAR(100)  NOT NULL,
        metric_value    NUMERIC(20,4),
        unit            VARCHAR(20),
        validated_at    TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
        CONSTRAINT uq_silver_fundamentals UNIQUE (symbol, report_period, metric_name)
    );
    """,

    """
    CREATE TABLE IF NOT EXISTS silver.news (
        id                   BIGSERIAL PRIMARY KEY,
        content_hash         CHAR(64)     NOT NULL,
        symbol               VARCHAR(20)  NOT NULL,
        headline             TEXT         NOT NULL,
        source               VARCHAR(200),
        published_at         TIMESTAMPTZ,
        url                  TEXT,
        -- HF enrichment (Rule 1)
        summary              TEXT,                     -- BART / summarisation output
        sentiment_score      NUMERIC(5,4),             -- [-1, 1]
        -- ChromaDB embedding tracking (Rule 2)
        embedding_updated_at TIMESTAMPTZ,
        -- Audit
        processed_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
        updated_at           TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
        CONSTRAINT uq_silver_news UNIQUE (content_hash)
    );
    """,

    # ═══════════════════════════════════════════════════════
    # GOLD — star schema (fact + dimensions)
    # ═══════════════════════════════════════════════════════

    # Dimension: companies (SCD Type 2)
    """
    CREATE TABLE IF NOT EXISTS gold.dim_companies (
        company_key     BIGSERIAL PRIMARY KEY,
        symbol          VARCHAR(20)   NOT NULL,
        company_name    VARCHAR(255),
        sector          VARCHAR(100),
        industry        VARCHAR(100),
        market_cap_tier VARCHAR(20),             -- LARGE / MID / SMALL
        exchange        VARCHAR(20)   NOT NULL DEFAULT 'NSE',
        -- SCD Type 2 tracking columns
        effective_from  DATE          NOT NULL,
        effective_to    DATE,                    -- NULL = current record
        is_current      BOOLEAN       NOT NULL DEFAULT TRUE,
        created_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW()
    );
    """,
    "CREATE INDEX IF NOT EXISTS ix_dim_companies_symbol ON gold.dim_companies (symbol) WHERE is_current;",

    # Dimension: calendar
    """
    CREATE TABLE IF NOT EXISTS gold.dim_date (
        date_key        INTEGER PRIMARY KEY,     -- YYYYMMDD integer
        full_date       DATE          NOT NULL UNIQUE,
        year            SMALLINT      NOT NULL,
        quarter         SMALLINT      NOT NULL,
        month           SMALLINT      NOT NULL,
        week_of_year    SMALLINT      NOT NULL,
        day_of_week     SMALLINT      NOT NULL,  -- 0=Mon … 6=Sun
        is_weekend      BOOLEAN       NOT NULL,
        is_nse_holiday  BOOLEAN       NOT NULL DEFAULT FALSE
    );
    """,

    # Fact: daily prices
    """
    CREATE TABLE IF NOT EXISTS gold.fact_prices (
        price_key       BIGSERIAL PRIMARY KEY,
        date_key        INTEGER       NOT NULL REFERENCES gold.dim_date(date_key),
        company_key     BIGINT        NOT NULL REFERENCES gold.dim_companies(company_key),
        open_price      NUMERIC(14,4) NOT NULL,
        high_price      NUMERIC(14,4) NOT NULL,
        low_price       NUMERIC(14,4) NOT NULL,
        close_price     NUMERIC(14,4) NOT NULL,
        adj_close_price NUMERIC(14,4) NOT NULL,
        volume          BIGINT        NOT NULL,
        daily_return    NUMERIC(10,6),
        CONSTRAINT uq_fact_prices UNIQUE (date_key, company_key)
    );
    """,
    "CREATE INDEX IF NOT EXISTS ix_fact_prices_company ON gold.fact_prices (company_key, date_key DESC);",

    # Fact: fundamentals
    """
    CREATE TABLE IF NOT EXISTS gold.fact_fundamentals (
        fundamental_key BIGSERIAL PRIMARY KEY,
        date_key        INTEGER       NOT NULL REFERENCES gold.dim_date(date_key),
        company_key     BIGINT        NOT NULL REFERENCES gold.dim_companies(company_key),
        metric_name     VARCHAR(100)  NOT NULL,
        metric_value    NUMERIC(20,4),
        unit            VARCHAR(20),
        CONSTRAINT uq_fact_fundamentals UNIQUE (date_key, company_key, metric_name)
    );
    """,

    # Fact: news sentiment
    """
    CREATE TABLE IF NOT EXISTS gold.fact_news_sentiment (
        sentiment_key   BIGSERIAL PRIMARY KEY,
        date_key        INTEGER       NOT NULL REFERENCES gold.dim_date(date_key),
        company_key     BIGINT        NOT NULL REFERENCES gold.dim_companies(company_key),
        content_hash    CHAR(64)      NOT NULL,
        headline        TEXT          NOT NULL,
        summary         TEXT,
        sentiment_score NUMERIC(5,4),
        embedding_id    UUID,
        CONSTRAINT uq_fact_news UNIQUE (content_hash)
    );
    """,

    # ── Gold: convenience materialised tables (readable by nse_reader) ─────
    # gold.stock_summary — latest snapshot per symbol (written by load.py)
    """
    CREATE TABLE IF NOT EXISTS gold.stock_summary (
        symbol              VARCHAR(20)   PRIMARY KEY,
        as_of_date          DATE          NOT NULL,
        -- Price & returns
        close_price         NUMERIC(14,4),
        daily_return        NUMERIC(10,6),
        normalised_close    NUMERIC(8,4),
        -- Technical indicators
        sma_20              NUMERIC(14,4),
        sma_50              NUMERIC(14,4),
        rsi_14              NUMERIC(6,4),
        -- Fundamentals (pivoted)
        market_cap          NUMERIC(20,4),
        trailing_pe         NUMERIC(14,4),
        price_to_book       NUMERIC(14,4),
        debt_to_equity      NUMERIC(14,4),
        return_on_equity    NUMERIC(14,4),
        dividend_yield      NUMERIC(10,6),
        fifty_two_week_high NUMERIC(14,4),
        fifty_two_week_low  NUMERIC(14,4),
        -- Audit
        updated_at          TIMESTAMPTZ   NOT NULL DEFAULT NOW()
    );
    """,

    # gold.news_feed — latest enriched news per symbol (written by load.py)
    """
    CREATE TABLE IF NOT EXISTS gold.news_feed (
        content_hash    CHAR(64)      PRIMARY KEY,
        symbol          VARCHAR(20)   NOT NULL,
        headline        TEXT          NOT NULL,
        source          VARCHAR(200),
        published_at    TIMESTAMPTZ,
        url             TEXT,
        summary         TEXT,
        sentiment_score NUMERIC(5,4),
        -- Audit
        updated_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW()
    );
    """,
    "CREATE INDEX IF NOT EXISTS ix_news_feed_symbol ON gold.news_feed (symbol, published_at DESC);",

    # ═══════════════════════════════════════════════════════
    # Pipeline audit / lineage
    # ═══════════════════════════════════════════════════════
    """
    CREATE TABLE IF NOT EXISTS public.pipeline_runs (
        run_id          UUID          PRIMARY KEY DEFAULT gen_random_uuid(),
        dag_id          VARCHAR(100)  NOT NULL,
        task_id         VARCHAR(100)  NOT NULL,
        run_date        DATE          NOT NULL,
        status          VARCHAR(20)   NOT NULL,   -- SUCCESS / FAILED / SKIPPED
        records_read    INTEGER,
        records_written INTEGER,
        error_message   TEXT,
        started_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
        finished_at     TIMESTAMPTZ
    );
    """,

    # ═══════════════════════════════════════════════════════
    # Earnings summaries (BART-generated from PDFs)
    # ═══════════════════════════════════════════════════════
    """
    CREATE TABLE IF NOT EXISTS public.earnings_summaries (
        id              BIGSERIAL PRIMARY KEY,
        ticker          VARCHAR(20)   NOT NULL,
        report_date     DATE,
        source_file     VARCHAR(255)  NOT NULL,
        summary         TEXT,
        created_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
        CONSTRAINT uq_earnings_summaries UNIQUE (ticker, report_date)
    );
    """,
    "CREATE INDEX IF NOT EXISTS ix_earnings_summaries_ticker ON public.earnings_summaries (ticker, report_date DESC);",
]

# ─────────────────────────────────────────────────
# Read-only role (Rule 8 — nse_reader for Streamlit)
# ─────────────────────────────────────────────────

def _ensure_readonly_role(conn: PgConnection) -> None:
    """
    Create the nse_reader role with SELECT-only privileges.
    Safe to call on every run; uses IF NOT EXISTS guards.
    Rule 8: Streamlit/Grafana MUST connect as nse_reader, never nse_admin.
    """
    readonly_user     = os.environ.get("POSTGRES_READONLY_USER", "nse_reader")
    readonly_password = os.environ["POSTGRES_READONLY_PASSWORD"]
    db_name           = os.environ["POSTGRES_DB"]

    with conn.cursor() as cur:
        # Create role if absent
        cur.execute(
            """
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_roles WHERE rolname = %s
                ) THEN
                    EXECUTE format('CREATE ROLE %%I LOGIN PASSWORD %%L',
                                   %s, %s);
                END IF;
            END
            $$;
            """,
            (readonly_user, readonly_user, readonly_password),
        )

        # Grant CONNECT
        cur.execute(
            sql.SQL("GRANT CONNECT ON DATABASE {} TO {};").format(
                sql.Identifier(db_name),
                sql.Identifier(readonly_user),
            )
        )

        # Grant USAGE on all schemas
        for schema in ("bronze", "silver", "gold", "public"):
            cur.execute(
                sql.SQL("GRANT USAGE ON SCHEMA {} TO {};").format(
                    sql.Identifier(schema),
                    sql.Identifier(readonly_user),
                )
            )
            # All existing tables
            cur.execute(
                sql.SQL(
                    "GRANT SELECT ON ALL TABLES IN SCHEMA {} TO {};"
                ).format(
                    sql.Identifier(schema),
                    sql.Identifier(readonly_user),
                )
            )
            # Future tables
            cur.execute(
                sql.SQL(
                    "ALTER DEFAULT PRIVILEGES IN SCHEMA {} "
                    "GRANT SELECT ON TABLES TO {};"
                ).format(
                    sql.Identifier(schema),
                    sql.Identifier(readonly_user),
                )
            )

    logger.info("Read-only role '%s' configured.", readonly_user)


# ─────────────────────────────────────────────────
# Populate dim_date (2020-01-01 → 2035-12-31)
# ─────────────────────────────────────────────────

_DIM_DATE_SQL = """
INSERT INTO gold.dim_date (
    date_key, full_date, year, quarter, month,
    week_of_year, day_of_week, is_weekend, is_nse_holiday
)
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INTEGER AS date_key,
    d                               AS full_date,
    EXTRACT(YEAR    FROM d)::SMALLINT,
    EXTRACT(QUARTER FROM d)::SMALLINT,
    EXTRACT(MONTH   FROM d)::SMALLINT,
    EXTRACT(WEEK    FROM d)::SMALLINT,
    EXTRACT(DOW     FROM d)::SMALLINT  - 1,  -- 0=Mon ... 6=Sun
    EXTRACT(DOW     FROM d) IN (0, 6),        -- Postgres: 0=Sun, 6=Sat
    FALSE                                      -- holidays updated separately
FROM generate_series(
    DATE '2020-01-01',
    DATE '2035-12-31',
    INTERVAL '1 day'
) AS g(d)
ON CONFLICT (date_key) DO NOTHING;   -- idempotent
"""


def _populate_dim_date(conn: PgConnection) -> None:
    with conn.cursor() as cur:
        cur.execute(_DIM_DATE_SQL)
    logger.info("dim_date populated (2020–2035).")


# ─────────────────────────────────────────────────
# Main entry-point
# ─────────────────────────────────────────────────

def run() -> None:
    """
    Idempotent initialisation: create schemas, tables, indices, roles, and
    seed dim_date.  Called by the Airflow db_init task AND can be run
    standalone: `python -m backend.pipeline.db_init`
    """
    logger.info("Starting db_init …")

    with get_connection() as conn:
        with conn.cursor() as cur:
            for stmt in DDL_STATEMENTS:
                trimmed = stmt.strip()
                if not trimmed:
                    continue
                try:
                    cur.execute(trimmed)
                    logger.debug("OK: %.80s …", trimmed.replace("\n", " "))
                except psycopg2.Error as exc:
                    logger.error("DDL error: %s\n%s", exc, trimmed[:200])
                    raise

        _ensure_readonly_role(conn)
        _populate_dim_date(conn)

    logger.info("db_init complete — all schemas, tables, and roles are ready.")


if __name__ == "__main__":
    missing = [
        v for v in (
            "POSTGRES_HOST", "POSTGRES_DB",
            "POSTGRES_USER", "POSTGRES_PASSWORD",
            "POSTGRES_READONLY_PASSWORD",
        )
        if not os.environ.get(v)
    ]
    if missing:
        logger.error("Missing env vars: %s", missing)
        sys.exit(1)
    run()
