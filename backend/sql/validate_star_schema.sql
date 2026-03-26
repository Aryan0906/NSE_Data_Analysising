-- =============================================================
-- backend/sql/validate_star_schema.sql
-- =============================================================
-- Sprint 3: star-schema integrity validation queries.
--
-- Each check is wrapped in a DO $$ block so it can be run as a
-- batch from psql or via psycopg2.  Problems are surfaced via
-- RAISE WARNING (non-fatal) so the full suite always completes.
--
-- Usage:
--   psql "$DATABASE_URL" -f backend/sql/validate_star_schema.sql
--   python -c "
--       import psycopg2, os
--       conn = psycopg2.connect(os.environ['DATABASE_URL'])
--       conn.autocommit = True
--       cur = conn.cursor()
--       cur.execute(open('backend/sql/validate_star_schema.sql').read())
--   "
-- =============================================================


-- ─────────────────────────────────────────────────────────────
-- CHECK 1 : fact_prices → dim_date referential integrity
--           Finds fact_prices rows whose date_key has no match
--           in gold.dim_date (dim_date covers 2020-01-01 → 2035-12-31).
-- ─────────────────────────────────────────────────────────────

DO $$
DECLARE
    v_count BIGINT;
BEGIN
    SELECT COUNT(*)
    INTO   v_count
    FROM   gold.fact_prices fp
    WHERE  NOT EXISTS (
        SELECT 1
        FROM   gold.dim_date dd
        WHERE  dd.date_key = fp.date_key
    );

    IF v_count > 0 THEN
        RAISE WARNING '[CHK-1] FAIL: % fact_prices row(s) have no matching dim_date.date_key.',
            v_count;
    ELSE
        RAISE NOTICE '[CHK-1] PASS: all fact_prices.date_key values exist in dim_date.';
    END IF;
END
$$;


-- ─────────────────────────────────────────────────────────────
-- CHECK 2 : fact_prices → dim_companies referential integrity
--           Finds fact_prices rows whose company_key has no
--           match in gold.dim_companies (any version, not just current).
-- ─────────────────────────────────────────────────────────────

DO $$
DECLARE
    v_count BIGINT;
BEGIN
    SELECT COUNT(*)
    INTO   v_count
    FROM   gold.fact_prices fp
    WHERE  NOT EXISTS (
        SELECT 1
        FROM   gold.dim_companies dc
        WHERE  dc.company_key = fp.company_key
    );

    IF v_count > 0 THEN
        RAISE WARNING '[CHK-2] FAIL: % fact_prices row(s) reference a company_key absent from dim_companies.',
            v_count;
    ELSE
        RAISE NOTICE '[CHK-2] PASS: all fact_prices.company_key values exist in dim_companies.';
    END IF;
END
$$;


-- ─────────────────────────────────────────────────────────────
-- CHECK 3 : orphan dim_companies
--           Current dim_companies rows with ZERO associated facts.
--           These are companies registered in the dimension but
--           whose price data has never been loaded.
-- ─────────────────────────────────────────────────────────────

DO $$
DECLARE
    v_count  BIGINT;
    v_sample TEXT;
BEGIN
    SELECT
        COUNT(*),
        STRING_AGG(dc.symbol, ', ' ORDER BY dc.symbol) FILTER (WHERE rn <= 5)
    INTO v_count, v_sample
    FROM (
        SELECT
            dc.symbol,
            ROW_NUMBER() OVER (ORDER BY dc.symbol) AS rn
        FROM gold.dim_companies dc
        WHERE dc.is_current = TRUE
          AND NOT EXISTS (
              SELECT 1
              FROM   gold.fact_prices fp
              WHERE  fp.company_key = dc.company_key
          )
    ) sub;

    IF v_count > 0 THEN
        RAISE WARNING '[CHK-3] WARN: % current dim_companies row(s) have no fact_prices. Sample: %.',
            v_count, v_sample;
    ELSE
        RAISE NOTICE '[CHK-3] PASS: all current dim_companies have at least one fact_prices row.';
    END IF;
END
$$;


-- ─────────────────────────────────────────────────────────────
-- CHECK 4 : silver → gold coverage gap
--           Symbols present in silver.prices but absent from
--           gold.dim_companies (is_current = TRUE).
--           Indicates an ETL failure in the dim load step.
-- ─────────────────────────────────────────────────────────────

DO $$
DECLARE
    v_count  BIGINT;
    v_sample TEXT;
BEGIN
    WITH silver_symbols AS (
        SELECT DISTINCT symbol FROM silver.prices
    ),
    gold_symbols AS (
        SELECT DISTINCT symbol FROM gold.dim_companies WHERE is_current = TRUE
    ),
    missing AS (
        SELECT ss.symbol
        FROM   silver_symbols ss
        LEFT JOIN gold_symbols gs USING (symbol)
        WHERE  gs.symbol IS NULL
    )
    SELECT
        COUNT(*),
        STRING_AGG(symbol, ', ' ORDER BY symbol) FILTER (WHERE rn <= 10)
    INTO v_count, v_sample
    FROM (
        SELECT symbol, ROW_NUMBER() OVER (ORDER BY symbol) AS rn
        FROM missing
    ) sub;

    IF v_count > 0 THEN
        RAISE WARNING '[CHK-4] FAIL: % silver symbol(s) missing from gold.dim_companies. Sample: %.',
            v_count, v_sample;
    ELSE
        RAISE NOTICE '[CHK-4] PASS: all silver.prices symbols are present in gold.dim_companies.';
    END IF;
END
$$;


-- ─────────────────────────────────────────────────────────────
-- CHECK 5 : dim_date calendar gap detector
--           Finds missing days inside the date range covered by
--           gold.dim_date.  A gap breaks date arithmetic in
--           rolling-window queries and Grafana time-series panels.
-- ─────────────────────────────────────────────────────────────

DO $$
DECLARE
    v_count    BIGINT;
    v_min_date DATE;
    v_max_date DATE;
    v_sample   TEXT;
BEGIN
    SELECT MIN(full_date), MAX(full_date)
    INTO   v_min_date, v_max_date
    FROM   gold.dim_date;

    WITH expected AS (
        SELECT generate_series(v_min_date, v_max_date, INTERVAL '1 day')::DATE AS dt
    ),
    missing AS (
        SELECT e.dt
        FROM   expected e
        LEFT JOIN gold.dim_date dd ON dd.full_date = e.dt
        WHERE  dd.full_date IS NULL
    )
    SELECT
        COUNT(*),
        STRING_AGG(dt::TEXT, ', ' ORDER BY dt) FILTER (WHERE rn <= 5)
    INTO v_count, v_sample
    FROM (
        SELECT dt, ROW_NUMBER() OVER (ORDER BY dt) AS rn
        FROM missing
    ) sub;

    IF v_count > 0 THEN
        RAISE WARNING '[CHK-5] FAIL: % date(s) missing from gold.dim_date between % and %. Sample: %.',
            v_count, v_min_date, v_max_date, v_sample;
    ELSE
        RAISE NOTICE '[CHK-5] PASS: gold.dim_date is contiguous from % to %.', v_min_date, v_max_date;
    END IF;
END
$$;


-- ─────────────────────────────────────────────────────────────
-- SUMMARY: return counts from all star-schema tables for a
--          quick at-a-glance health check (non-DO wrapper).
-- ─────────────────────────────────────────────────────────────

SELECT
    'gold.dim_date'         AS table_name, COUNT(*) AS row_count FROM gold.dim_date
UNION ALL
SELECT
    'gold.dim_companies',   COUNT(*) FROM gold.dim_companies
UNION ALL
SELECT
    'gold.fact_prices',     COUNT(*) FROM gold.fact_prices
UNION ALL
SELECT
    'gold.fact_fundamentals', COUNT(*) FROM gold.fact_fundamentals
UNION ALL
SELECT
    'gold.fact_news_sentiment', COUNT(*) FROM gold.fact_news_sentiment
UNION ALL
SELECT
    'silver.prices',        COUNT(*) FROM silver.prices
UNION ALL
SELECT
    'silver.news',          COUNT(*) FROM silver.news
ORDER BY table_name;
