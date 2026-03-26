"""
tests/test_load.py
==================
Static checks that the SQL string constants in load.py reference
the correct (post-fix) table names.
No database connection required.
"""
import pytest

from backend.pipeline.load import _GOLD_SUMMARY_UPSERT, _GOLD_NEWS_UPSERT


class TestGoldSummaryUpsert:
    def test_inserts_into_gold_stock_summary(self):
        """The upsert must target the gold schema table, not bronze/silver."""
        assert "gold.stock_summary" in _GOLD_SUMMARY_UPSERT

    def test_reads_from_silver_prices(self):
        """Source data must come from the silver layer."""
        assert "silver.prices" in _GOLD_SUMMARY_UPSERT

    def test_reads_from_silver_fundamentals(self):
        """Fundamental pivot uses silver.fundamentals."""
        assert "silver.fundamentals" in _GOLD_SUMMARY_UPSERT

    def test_contains_on_conflict(self):
        """Must be an upsert, not a plain INSERT."""
        assert "ON CONFLICT" in _GOLD_SUMMARY_UPSERT

    def test_has_updated_at_refresh(self):
        """updated_at should be refreshed on conflict."""
        assert "updated_at" in _GOLD_SUMMARY_UPSERT


class TestGoldNewsUpsert:
    def test_inserts_into_gold_news_feed(self):
        """Target table is gold.news_feed."""
        assert "gold.news_feed" in _GOLD_NEWS_UPSERT

    def test_reads_from_silver_news(self):
        """Source is silver.news (not the old silver.news_processed)."""
        assert "silver.news" in _GOLD_NEWS_UPSERT

    def test_silver_news_processed_absent(self):
        """The old, stale table name must not appear."""
        assert "silver.news_processed" not in _GOLD_NEWS_UPSERT

    def test_contains_on_conflict(self):
        """Must be an upsert."""
        assert "ON CONFLICT" in _GOLD_NEWS_UPSERT

    def test_has_sentiment_score(self):
        """sentiment_score column must be projected."""
        assert "sentiment_score" in _GOLD_NEWS_UPSERT
