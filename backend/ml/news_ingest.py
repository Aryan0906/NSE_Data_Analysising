import hashlib
import logging
from datetime import datetime
from typing import List, Dict

import feedparser
from sqlalchemy import create_engine, text

from backend.pipeline.settings import settings

logger = logging.getLogger(__name__)

class NewsIngestor:
    """
    Fetches financial news via Yahoo Finance RSS, enforcing Rule 6 (News deduplication).
    """
    
    def __init__(self, db_engine=None):
        self.db_engine = db_engine or create_engine(settings.postgres_dsn, pool_pre_ping=True)
        self.tickers = settings.nse_symbols
        self._ensure_table()
        
    def _ensure_table(self):
        """Ensure raw_news table exists with url_hash UNIQUE constraint."""
        with self.db_engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS public.raw_news (
                    id SERIAL PRIMARY KEY,
                    ticker VARCHAR(20) NOT NULL,
                    title TEXT NOT NULL,
                    summary TEXT,
                    published_at TIMESTAMP,
                    url TEXT NOT NULL,
                    url_hash VARCHAR(64) UNIQUE NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    embedded BOOLEAN DEFAULT FALSE
                );
            """))
            
    def _generate_url_hash(self, url: str) -> str:
        """Rule 6: url_hash = SHA256(url)"""
        return hashlib.sha256(url.encode('utf-8')).hexdigest()

    def fetch_rss_for_ticker(self, ticker: str) -> List[Dict]:
        """Fetch RSS feed from Yahoo Finance for a specific ticker."""
        # Yahoo Finance RSS url format
        url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker}&region=IN&lang=en-IN"
        logger.info(f"Fetching RSS feed for {ticker}: {url}")
        feed = feedparser.parse(url)
        
        articles = []
        for entry in feed.entries:
            # Parse dates
            try:
                published_at = datetime(*entry.published_parsed[:6])
            except (AttributeError, TypeError):
                published_at = datetime.utcnow()
                
            articles.append({
                "ticker": ticker,
                "title": entry.title,
                "summary": getattr(entry, 'summary', ''),
                "url": entry.link,
                "published_at": published_at
            })
            
        return articles
        
    def ingest_all(self):
        """Fetch and store news for all configured tickers."""
        total_inserted = 0
        total_duplicates = 0
        
        with self.db_engine.begin() as conn:
            for ticker in self.tickers:
                articles = self.fetch_rss_for_ticker(ticker)
                
                for article in articles:
                    url_hash = self._generate_url_hash(article['url'])
                    
                    # Rule 6: ON CONFLICT DO NOTHING
                    result = conn.execute(text("""
                        INSERT INTO public.raw_news (ticker, title, summary, published_at, url, url_hash)
                        VALUES (:ticker, :title, :summary, :published_at, :url, :url_hash)
                        ON CONFLICT (url_hash) DO NOTHING;
                    """), {
                        "ticker": article['ticker'],
                        "title": article['title'],
                        "summary": article['summary'],
                        "published_at": article['published_at'],
                        "url": article['url'],
                        "url_hash": url_hash
                    })
                    
                    if result.rowcount > 0:
                        total_inserted += 1
                    else:
                        total_duplicates += 1
                        
        logger.info(f"News ingest complete. Inserted {total_inserted}, Discarded (duplicates) {total_duplicates}.")
        return total_inserted

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    ingestor = NewsIngestor()
    ingestor.ingest_all()
