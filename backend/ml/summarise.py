import logging
from math import ceil
from typing import List

from sqlalchemy import create_engine, text

from backend.pipeline.hf_client import HFClient
from backend.pipeline.settings import settings

logger = logging.getLogger(__name__)

class EarningsSummariser:
    """
    Chunks earnings reports and summarises them using HF BART API.
    Saves results to the `earnings_summaries` PostgreSQL table.
    """
    
    def __init__(self, db_engine=None):
        self.db_engine = db_engine or create_engine(settings.postgres_dsn, pool_pre_ping=True)
        self.hf_client = HFClient(self.db_engine)
        self.model = settings.hf_summarisation_model
        
        # BART max token limit roughly aligns with 4000 characters
        self.chunk_size = 4000 
        self._ensure_table()
        
    def _ensure_table(self):
        with self.db_engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS public.earnings_summaries (
                    id SERIAL PRIMARY KEY,
                    ticker VARCHAR(20) NOT NULL,
                    report_date DATE NOT NULL,
                    source_file VARCHAR(255),
                    raw_text_length INT,
                    summary TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(ticker, report_date)
                );
            """))
            
    def _chunk_text(self, text: str) -> List[str]:
        """Split text into manageable chunks for the BART model."""
        # Simple character-based chunking; robust enough for most text
        chunks = []
        for i in range(0, len(text), self.chunk_size):
            chunks.append(text[i:i+self.chunk_size])
        return chunks

    def fetch_summary(self, text_chunk: str) -> str:
        """Call HF inference API via refactored HFClient."""
        try:
            return self.hf_client.summarization(text=text_chunk, model=self.model)
        except Exception as e:
            logger.error(f"HF Inference API Error: {e}")
            return ""

    def summarise(self, ticker: str, report_date: str, source_file: str, raw_text: str) -> str:
        """
        Produce a chunked summary for an entire earnings report,
        then save the combined summary to PostgreSQL.
        """
        logger.info(f"Summarising {len(raw_text)} chars for {ticker} ({report_date})")
        
        chunks = self._chunk_text(raw_text)
        logger.info(f"Split into {len(chunks)} chunks.")
        
        chunk_summaries = []
        for i, chunk in enumerate(chunks, 1):
            logger.debug(f"Summarising chunk {i}/{len(chunks)}...")
            summary = self.fetch_summary(chunk)
            if summary:
                chunk_summaries.append(summary)
                
        final_summary = " ".join(chunk_summaries)
        
        if final_summary:
            with self.db_engine.begin() as conn:
                conn.execute(text("""
                    INSERT INTO public.earnings_summaries 
                    (ticker, report_date, source_file, raw_text_length, summary)
                    VALUES (:ticker, :report_date, :source_file, :raw_text_length, :summary)
                    ON CONFLICT (ticker, report_date) 
                    DO UPDATE SET summary = EXCLUDED.summary, 
                                  raw_text_length = EXCLUDED.raw_text_length,
                                  source_file = EXCLUDED.source_file,
                                  created_at = CURRENT_TIMESTAMP;
                """), {
                    "ticker": ticker,
                    "report_date": report_date,
                    "source_file": source_file,
                    "raw_text_length": len(raw_text),
                    "summary": final_summary
                })
            logger.info("Summary saved successfully.")
        else:
            logger.warning("Failed to generate summary.")
            
        return final_summary
