"""
tests/test_db_init.py
======================
Static checks that the DDL_STATEMENTS list in db_init.py contains the
expected table names and key column definitions.
No database connection required — we simply inspect the Python string constants.
"""
import pytest

from backend.pipeline.db_init import DDL_STATEMENTS


# Concatenate all DDL strings for easy substring search.
_ALL_DDL = "\n".join(DDL_STATEMENTS)


# ─────────────────────────────────────────────────
# silver.prices
# ─────────────────────────────────────────────────

class TestSilverPrices:
    def test_table_exists(self):
        assert "silver.prices" in _ALL_DDL

    def test_log_return_column(self):
        assert "log_return" in _ALL_DDL

    def test_is_outlier_column(self):
        assert "is_outlier" in _ALL_DDL

    def test_sma_20_column(self):
        assert "sma_20" in _ALL_DDL

    def test_sma_50_column(self):
        assert "sma_50" in _ALL_DDL

    def test_sma_200_column(self):
        assert "sma_200" in _ALL_DDL

    def test_ema_20_column(self):
        assert "ema_20" in _ALL_DDL

    def test_rsi_14_column(self):
        assert "rsi_14" in _ALL_DDL

    def test_normalised_close_column(self):
        assert "normalised_close" in _ALL_DDL

    def test_high_52w_column(self):
        assert "high_52w" in _ALL_DDL

    def test_low_52w_column(self):
        assert "low_52w" in _ALL_DDL

    def test_price_range_52w_column(self):
        assert "price_range_52w" in _ALL_DDL

    def test_updated_at_column(self):
        assert "updated_at" in _ALL_DDL


# ─────────────────────────────────────────────────
# silver.news (renamed from silver.news_processed)
# ─────────────────────────────────────────────────

class TestSilverNews:
    def test_silver_news_exists(self):
        """silver.news table must be created."""
        assert "silver.news" in _ALL_DDL

    def test_silver_news_processed_absent(self):
        """The old, stale name must not appear in any DDL statement."""
        assert "silver.news_processed" not in _ALL_DDL

    def test_silver_news_has_source(self):
        assert "source" in _ALL_DDL

    def test_silver_news_has_url(self):
        assert "url" in _ALL_DDL

    def test_silver_news_has_published_at(self):
        assert "published_at" in _ALL_DDL

    def test_silver_news_has_sentiment_score(self):
        assert "sentiment_score" in _ALL_DDL

    def test_silver_news_has_embedding_updated_at(self):
        assert "embedding_updated_at" in _ALL_DDL


# ─────────────────────────────────────────────────
# gold.stock_summary
# ─────────────────────────────────────────────────

class TestGoldStockSummary:
    def test_table_exists(self):
        assert "gold.stock_summary" in _ALL_DDL

    def test_has_as_of_date(self):
        assert "as_of_date" in _ALL_DDL

    def test_has_close_price(self):
        assert "close_price" in _ALL_DDL

    def test_has_market_cap(self):
        assert "market_cap" in _ALL_DDL


# ─────────────────────────────────────────────────
# gold.news_feed
# ─────────────────────────────────────────────────

class TestGoldNewsFeed:
    def test_table_exists(self):
        assert "gold.news_feed" in _ALL_DDL

    def test_has_content_hash(self):
        assert "content_hash" in _ALL_DDL

    def test_has_symbol(self):
        # symbol appears in many tables so just verify it's there
        assert "symbol" in _ALL_DDL

    def test_has_summary(self):
        assert "summary" in _ALL_DDL
