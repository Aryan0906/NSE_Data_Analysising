-- ============================================================
-- NSE Data Platform — Database Schema
-- ============================================================
-- Layers:
--   bronze  : raw ingested data (immutable originals)
--   silver  : cleaned + enriched data
--   gold    : materialised analytics-ready views / tables
--
-- Run once at container start (idempotent via IF NOT EXISTS).
-- ============================================================

-- ─────────────────────────────────────────────────
-- Schemas
-- ─────────────────────────────────────────────────

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- ─────────────────────────────────────────────────
-- BRONZE: raw_prices
-- ─────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS bronze.raw_prices (
    id              BIGSERIAL PRIMARY KEY,
    symbol          TEXT        NOT NULL,
    trade_date      DATE        NOT NULL,
    open_price      NUMERIC(18, 4),
    high_price      NUMERIC(18, 4),
    low_price       NUMERIC(18, 4),
    close_price     NUMERIC(18, 4),
    adj_close_price NUMERIC(18, 4),
    volume          BIGINT,
    source          TEXT        DEFAULT 'yahoo_finance',
    ingested_at     TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (symbol, trade_date)
);

CREATE INDEX IF NOT EXISTS idx_bronze_prices_symbol_date
    ON bronze.raw_prices (symbol, trade_date DESC);

-- ─────────────────────────────────────────────────
-- BRONZE: raw_fundamentals
-- ─────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS bronze.raw_fundamentals (
    id            BIGSERIAL PRIMARY KEY,
    symbol        TEXT        NOT NULL,
    report_period DATE,              -- NULL = TTM snapshot
    metric_name   TEXT        NOT NULL,
    metric_value  NUMERIC(24, 6),
    unit          TEXT,
    source        TEXT        DEFAULT 'yahoo_finance',
    ingested_at   TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_bronze_fund_symbol_metric
    ON bronze.raw_fundamentals (symbol, metric_name, report_period DESC);

-- ─────────────────────────────────────────────────
-- BRONZE: raw_news
-- ─────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS bronze.raw_news (
    id           BIGSERIAL PRIMARY KEY,
    content_hash TEXT        NOT NULL UNIQUE,   -- MD5 of headline+body
    symbol       TEXT        NOT NULL,
    headline     TEXT        NOT NULL,
    source       TEXT,
    published_at TIMESTAMPTZ,
    url          TEXT,
    raw_body     TEXT,
    ingested_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_bronze_news_symbol_pub
    ON bronze.raw_news (symbol, published_at DESC);

-- ─────────────────────────────────────────────────
-- SILVER: prices
-- ─────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS silver.prices (
    symbol            TEXT        NOT NULL,
    trade_date        DATE        NOT NULL,
    open_price        NUMERIC(18, 4),
    high_price        NUMERIC(18, 4),
    low_price         NUMERIC(18, 4),
    close_price       NUMERIC(18, 4),
    adj_close_price   NUMERIC(18, 4),
    volume            BIGINT,
    -- Returns
    daily_return      NUMERIC(12, 6),
    log_return        NUMERIC(12, 6),
    is_outlier        BOOLEAN     DEFAULT FALSE,
    -- Indicators
    sma_20            NUMERIC(18, 4),
    sma_50            NUMERIC(18, 4),
    sma_200           NUMERIC(18, 4),
    ema_20            NUMERIC(18, 4),
    rsi_14            NUMERIC(8, 4),
    -- Rule 4 normalisation
    normalised_close  NUMERIC(8, 4),
    -- 52-week stats
    high_52w          NUMERIC(18, 4),
    low_52w           NUMERIC(18, 4),
    price_range_52w   NUMERIC(18, 4),
    updated_at        TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (symbol, trade_date)
);

CREATE INDEX IF NOT EXISTS idx_silver_prices_symbol_date
    ON silver.prices (symbol, trade_date DESC);

-- ─────────────────────────────────────────────────
-- SILVER: fundamentals
-- ─────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS silver.fundamentals (
    symbol        TEXT        NOT NULL,
    report_period DATE,
    metric_name   TEXT        NOT NULL,
    metric_value  NUMERIC(24, 6),
    unit          TEXT,
    updated_at    TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (symbol, report_period, metric_name)
);

-- ─────────────────────────────────────────────────
-- SILVER: news
-- ─────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS silver.news (
    content_hash          TEXT        NOT NULL PRIMARY KEY,
    symbol                TEXT        NOT NULL,
    headline              TEXT        NOT NULL,
    source                TEXT,
    published_at          TIMESTAMPTZ,
    url                   TEXT,
    summary               TEXT,           -- HF summarisation output
    sentiment_score       NUMERIC(6, 4),  -- [-1, 1] from HF sentiment
    embedding_updated_at  TIMESTAMPTZ,    -- set after ChromaDB upsert
    updated_at            TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_silver_news_symbol_pub
    ON silver.news (symbol, published_at DESC);

-- ─────────────────────────────────────────────────
-- GOLD: stock_summary
-- ─────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS gold.stock_summary (
    symbol              TEXT        NOT NULL PRIMARY KEY,
    as_of_date          DATE,
    close_price         NUMERIC(18, 4),
    daily_return        NUMERIC(12, 6),
    normalised_close    NUMERIC(8, 4),
    sma_20              NUMERIC(18, 4),
    sma_50              NUMERIC(18, 4),
    rsi_14              NUMERIC(8, 4),
    -- Fundamentals (pivoted)
    market_cap          NUMERIC(24, 2),
    trailing_pe         NUMERIC(12, 4),
    price_to_book       NUMERIC(12, 4),
    debt_to_equity      NUMERIC(12, 4),
    return_on_equity    NUMERIC(12, 4),
    dividend_yield      NUMERIC(12, 6),
    fifty_two_week_high NUMERIC(18, 4),
    fifty_two_week_low  NUMERIC(18, 4),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

-- ─────────────────────────────────────────────────
-- GOLD: news_feed
-- ─────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS gold.news_feed (
    content_hash    TEXT        NOT NULL PRIMARY KEY,
    symbol          TEXT        NOT NULL,
    headline        TEXT        NOT NULL,
    source          TEXT,
    published_at    TIMESTAMPTZ,
    url             TEXT,
    summary         TEXT,
    sentiment_score NUMERIC(6, 4),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_gold_news_feed_symbol_pub
    ON gold.news_feed (symbol, published_at DESC);

-- ─────────────────────────────────────────────────
-- Read-only role (Rule 8)
-- ─────────────────────────────────────────────────

DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'nse_reader') THEN
        CREATE ROLE nse_reader;
    END IF;
END
$$;

GRANT USAGE ON SCHEMA bronze, silver, gold TO nse_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA bronze TO nse_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA silver TO nse_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA gold   TO nse_reader;

-- Future tables also inherit
ALTER DEFAULT PRIVILEGES IN SCHEMA bronze GRANT SELECT ON TABLES TO nse_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA silver GRANT SELECT ON TABLES TO nse_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold   GRANT SELECT ON TABLES TO nse_reader;
