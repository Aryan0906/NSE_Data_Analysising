-- =============================================================
-- backend/sql/create_views.sql
-- =============================================================
-- Sprint 3: analytical views over the star-schema gold layer.
--
-- Design rules:
--   • SECURITY INVOKER  — nse_reader can SELECT only after GRANT
--   • CREATE OR REPLACE — idempotent, safe to re-run from Airflow
--   • All timestamps returned in TIMESTAMPTZ (UTC)
--   • Views live in the gold schema; source tables may be silver/*
--
-- Run order: must be executed AFTER db_init.py (schemas + tables exist).
-- =============================================================


-- ─────────────────────────────────────────────────────────────
-- 1. v_latest_prices
--    Latest silver.prices row per symbol using DISTINCT ON.
--    Used by Streamlit "current quotes" panel and Grafana alerts.
-- ─────────────────────────────────────────────────────────────

CREATE OR REPLACE VIEW gold.v_latest_prices
    WITH (security_invoker = true)
AS
SELECT DISTINCT ON (p.symbol)
    p.symbol,
    p.trade_date,
    p.close_price,
    p.adj_close_price,
    p.open_price,
    p.high_price,
    p.low_price,
    p.volume,
    p.daily_return,
    p.log_return,
    p.normalised_close,
    p.sma_20,
    p.sma_50,
    p.sma_200,
    p.ema_20,
    p.rsi_14,
    p.high_52w,
    p.low_52w,
    p.price_range_52w,
    p.is_outlier,
    p.updated_at
FROM silver.prices AS p
ORDER BY
    p.symbol,
    p.trade_date DESC;

COMMENT ON VIEW gold.v_latest_prices IS
    'Most-recent silver.prices row per symbol. Refreshed on every load.py run.';


-- ─────────────────────────────────────────────────────────────
-- 2. v_market_pulse
--    Daily market breadth: advance/decline counts, turnover,
--    and pct of symbols that are outliers or overbought (RSI>70).
--    Aggregated at trade_date level — useful for macro dashboards.
-- ─────────────────────────────────────────────────────────────

CREATE OR REPLACE VIEW gold.v_market_pulse
    WITH (security_invoker = true)
AS
SELECT
    p.trade_date,
    COUNT(*)                                              AS total_symbols,
    COUNT(*) FILTER (WHERE p.daily_return > 0)           AS advancing,
    COUNT(*) FILTER (WHERE p.daily_return < 0)           AS declining,
    COUNT(*) FILTER (WHERE p.daily_return = 0
                        OR p.daily_return IS NULL)        AS unchanged,
    ROUND(
        COUNT(*) FILTER (WHERE p.daily_return > 0)::NUMERIC
        / NULLIF(COUNT(*), 0) * 100, 2
    )                                                     AS advance_pct,
    SUM(p.volume)                                         AS total_volume,
    ROUND(AVG(p.daily_return) * 100, 4)                  AS avg_return_pct,
    COUNT(*) FILTER (WHERE p.is_outlier)                 AS outlier_count,
    COUNT(*) FILTER (WHERE p.rsi_14 > 70)                AS overbought_count,
    COUNT(*) FILTER (WHERE p.rsi_14 < 30)                AS oversold_count
FROM silver.prices AS p
GROUP BY p.trade_date;

COMMENT ON VIEW gold.v_market_pulse IS
    'Daily market breadth: advance/decline, volume, outlier & RSI counts.';


-- ─────────────────────────────────────────────────────────────
-- 3. v_sector_momentum
--    Sector-level aggregation of technical indicators.
--    Joins silver.prices → gold.dim_companies (current version only).
--    Restricted to the 20 most-recent trading dates via a lateral join.
-- ─────────────────────────────────────────────────────────────

CREATE OR REPLACE VIEW gold.v_sector_momentum
    WITH (security_invoker = true)
AS
WITH recent_dates AS (
    -- Last 20 distinct trading dates across all symbols
    SELECT DISTINCT trade_date
    FROM silver.prices
    ORDER BY trade_date DESC
    LIMIT 20
)
SELECT
    dc.sector,
    p.trade_date,
    COUNT(DISTINCT p.symbol)                              AS symbol_count,
    ROUND(AVG(p.close_price),    4)                      AS avg_close,
    ROUND(AVG(p.daily_return),   6)                      AS avg_daily_return,
    ROUND(AVG(p.sma_20),         4)                      AS avg_sma_20,
    ROUND(AVG(p.sma_50),         4)                      AS avg_sma_50,
    ROUND(AVG(p.rsi_14),         4)                      AS avg_rsi_14,
    ROUND(AVG(p.normalised_close), 4)                    AS avg_norm_close,
    COUNT(*) FILTER (WHERE p.daily_return > 0)           AS advancing,
    COUNT(*) FILTER (WHERE p.daily_return < 0)           AS declining,
    SUM(p.volume)                                         AS total_volume
FROM silver.prices AS p
JOIN recent_dates  AS rd ON rd.trade_date = p.trade_date
JOIN gold.dim_companies AS dc
    ON  dc.symbol     = p.symbol
    AND dc.is_current = TRUE
WHERE dc.sector IS NOT NULL
GROUP BY dc.sector, p.trade_date;

COMMENT ON VIEW gold.v_sector_momentum IS
    'Sector-level SMA/RSI aggregates over the 20 most-recent trading dates.';


-- ─────────────────────────────────────────────────────────────
-- 4. v_news_sentiment
--    7-day rolling average sentiment per symbol.
--    Also returns the latest headline and source for UI display.
-- ─────────────────────────────────────────────────────────────

CREATE OR REPLACE VIEW gold.v_news_sentiment
    WITH (security_invoker = true)
AS
WITH ranked AS (
    SELECT
        n.symbol,
        n.headline,
        n.source,
        n.published_at,
        n.sentiment_score,
        n.summary,
        ROW_NUMBER() OVER (
            PARTITION BY n.symbol
            ORDER BY n.published_at DESC NULLS LAST
        ) AS rn
    FROM silver.news AS n
    WHERE n.published_at >= NOW() - INTERVAL '7 days'
      AND n.sentiment_score IS NOT NULL
),
agg AS (
    SELECT
        symbol,
        COUNT(*)                          AS article_count_7d,
        ROUND(AVG(sentiment_score), 4)    AS avg_sentiment_7d,
        ROUND(MIN(sentiment_score), 4)    AS min_sentiment_7d,
        ROUND(MAX(sentiment_score), 4)    AS max_sentiment_7d,
        COUNT(*) FILTER (WHERE sentiment_score > 0.1)  AS positive_count,
        COUNT(*) FILTER (WHERE sentiment_score < -0.1) AS negative_count
    FROM silver.news
    WHERE published_at >= NOW() - INTERVAL '7 days'
      AND sentiment_score IS NOT NULL
    GROUP BY symbol
)
SELECT
    a.symbol,
    a.article_count_7d,
    a.avg_sentiment_7d,
    a.min_sentiment_7d,
    a.max_sentiment_7d,
    a.positive_count,
    a.negative_count,
    -- Latest article details (rn = 1)
    r.headline       AS latest_headline,
    r.source         AS latest_source,
    r.published_at   AS latest_published_at,
    r.summary        AS latest_summary,
    CASE
        WHEN a.avg_sentiment_7d >  0.1 THEN 'BULLISH'
        WHEN a.avg_sentiment_7d < -0.1 THEN 'BEARISH'
        ELSE 'NEUTRAL'
    END              AS sentiment_label
FROM agg AS a
LEFT JOIN ranked AS r ON r.symbol = a.symbol AND r.rn = 1;

COMMENT ON VIEW gold.v_news_sentiment IS
    '7-day rolling news sentiment per symbol with latest headline and label.';


-- ─────────────────────────────────────────────────────────────
-- 5. v_portfolio_risk
--    20-day annualised volatility (σ of log_return × √252) and a
--    beta stub (cov(stock, market) / var(market)) where the market
--    proxy is the daily avg log_return across all symbols.
--
--    NOTE: beta requires sufficient history; symbols with < 20 rows
--    in the last 20 trading dates will have NULL beta.
-- ─────────────────────────────────────────────────────────────

CREATE OR REPLACE VIEW gold.v_portfolio_risk
    WITH (security_invoker = true)
AS
WITH recent_20 AS (
    -- Last 20 distinct trading dates
    SELECT DISTINCT trade_date
    FROM silver.prices
    ORDER BY trade_date DESC
    LIMIT 20
),
market AS (
    -- Equal-weight market proxy: avg log_return per date
    SELECT
        p.trade_date,
        AVG(p.log_return) AS market_log_return
    FROM silver.prices AS p
    JOIN recent_20 AS r ON r.trade_date = p.trade_date
    WHERE p.log_return IS NOT NULL
    GROUP BY p.trade_date
),
stock_stats AS (
    SELECT
        p.symbol,
        COUNT(p.log_return)                                 AS obs_count,
        STDDEV_POP(p.log_return) * SQRT(252)               AS annualised_vol,
        AVG(p.log_return)                                   AS avg_log_return,
        -- Covariance(stock, market)
        COVAR_POP(p.log_return, m.market_log_return)        AS cov_with_market,
        -- Variance(market) — same for all rows in this stock
        VAR_POP(m.market_log_return)                        AS var_market
    FROM silver.prices AS p
    JOIN recent_20     AS r ON r.trade_date = p.trade_date
    JOIN market        AS m ON m.trade_date = p.trade_date
    WHERE p.log_return IS NOT NULL
    GROUP BY p.symbol
)
SELECT
    s.symbol,
    s.obs_count,
    ROUND(s.annualised_vol,    6)                           AS annualised_vol,
    ROUND(s.avg_log_return,    6)                           AS avg_log_return_20d,
    ROUND(
        CASE
            WHEN s.var_market > 0
            THEN s.cov_with_market / s.var_market
            ELSE NULL
        END,
        4
    )                                                       AS beta_20d,
    -- Sharpe-like ratio (no risk-free rate subtracted — use as relative rank)
    ROUND(
        CASE
            WHEN NULLIF(s.annualised_vol, 0) IS NOT NULL
            THEN (s.avg_log_return * 252) / s.annualised_vol
            ELSE NULL
        END,
        4
    )                                                       AS sharpe_proxy
FROM stock_stats AS s
WHERE s.obs_count >= 5;   -- require at least 5 data points

COMMENT ON VIEW gold.v_portfolio_risk IS
    '20-day annualised volatility, beta (market proxy), and Sharpe proxy per symbol.';


-- ─────────────────────────────────────────────────────────────
-- GRANT to nse_reader (Rule 8)
-- ─────────────────────────────────────────────────────────────

GRANT SELECT ON gold.v_latest_prices    TO nse_reader;
GRANT SELECT ON gold.v_market_pulse     TO nse_reader;
GRANT SELECT ON gold.v_sector_momentum  TO nse_reader;
GRANT SELECT ON gold.v_news_sentiment   TO nse_reader;
GRANT SELECT ON gold.v_portfolio_risk   TO nse_reader;
