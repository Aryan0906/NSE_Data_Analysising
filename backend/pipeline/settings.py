"""
backend/pipeline/settings.py
==============================
Single source of truth for all configurable values read from environment
variables.  Uses pydantic-settings so every field is validated on import.

Usage::

    from backend.pipeline.settings import settings

    conn_str = settings.postgres_dsn
"""

from __future__ import annotations

from functools import lru_cache
from typing import Optional

from pydantic import Field, PostgresDsn, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings — all values sourced from env / .env file."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",  # ignore unknown env vars
    )

    # ── Postgres ────────────────────────────────────────────────────────────
    postgres_host: str = Field(..., alias="POSTGRES_HOST")
    postgres_port: int = Field(5432, alias="POSTGRES_PORT")
    postgres_db: str = Field(..., alias="POSTGRES_DB")
    postgres_user: str = Field(..., alias="POSTGRES_USER")
    postgres_password: str = Field(..., alias="POSTGRES_PASSWORD")
    postgres_readonly_user: str = Field("nse_reader", alias="POSTGRES_READONLY_USER")
    postgres_readonly_password: str = Field(..., alias="POSTGRES_READONLY_PASSWORD")

    # ── ChromaDB (Rule 2: vector store for news embeddings) ─────────────────
    chromadb_host: str = Field("localhost", alias="CHROMADB_HOST")
    chromadb_port: int = Field(8000, alias="CHROMADB_PORT")

    # ── NewsAPI (Rule 6: news dedup via SHA-256) ─────────────────────────────
    news_api_key: Optional[str] = Field(None, alias="NEWS_API_KEY")
    news_page_size: int = Field(100, alias="NEWS_PAGE_SIZE")

    # ── HuggingFace (Rule 1: ≤80 calls/day) ──────────────────────────────────
    hf_api_token: Optional[str] = Field(None, alias="HF_API_TOKEN")
    hf_daily_call_limit: int = Field(80, alias="HF_DAILY_CALL_LIMIT")
    hf_summarisation_model: str = Field(
        "facebook/bart-large-cnn", alias="HF_SUMMARISATION_MODEL"
    )
    hf_sentiment_model: str = Field(
        "ProsusAI/finbert", alias="HF_SENTIMENT_MODEL"
    )

    # ── ETL ──────────────────────────────────────────────────────────────────
    nse_symbols: list[str] = Field(
        default_factory=list, alias="NSE_SYMBOLS"
    )
    etl_lookback_days: int = Field(365, alias="ETL_LOOKBACK_DAYS")
    log_level: str = Field("INFO", alias="LOG_LEVEL")

    # ── Derived DSN (built from individual fields) ───────────────────────────
    @property
    def postgres_dsn(self) -> str:
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @field_validator("nse_symbols", mode="before")
    @classmethod
    def _parse_symbols(cls, v: object) -> list[str]:
        """Accept comma-separated string or list."""
        if isinstance(v, str):
            return [s.strip() for s in v.split(",") if s.strip()]
        if isinstance(v, list):
            return v
        return []

    @field_validator("log_level")
    @classmethod
    def _upper_log(cls, v: str) -> str:
        return v.upper()


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return a cached Settings instance (once per process)."""
    return Settings()


# Module-level singleton for convenience
settings = get_settings()
