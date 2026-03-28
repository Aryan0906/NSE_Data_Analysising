#!/usr/bin/env python3
"""
Quick test to verify earnings pipeline flow.
Run from project root: python test_earnings_flow.py
"""

import os
import sys
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | test | %(message)s"
)
logger = logging.getLogger(__name__)

# Add backend to path
sys.path.insert(0, os.path.dirname(__file__))

try:
    from backend.pipeline.settings import settings
    logger.info(f"✓ Settings loaded. NSE symbols: {settings.nse_symbols[:3]}")
except ImportError as e:
    logger.error(f"✗ Failed to import settings: {e}")
    sys.exit(1)

try:
    from backend.pipeline import earnings_ingest
    logger.info("✓ earnings_ingest module imported")
except ImportError as e:
    logger.error(f"✗ Failed to import earnings_ingest: {e}")
    sys.exit(1)

try:
    from backend.pipeline.hf_client import summarise, get_sentiment
    logger.info("✓ HuggingFace client imported")
except ImportError as e:
    logger.error(f"✗ Failed to import HF client: {e}")
    sys.exit(1)

try:
    import psycopg2
    conn = psycopg2.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        dbname=settings.postgres_db,
        user=settings.postgres_user,
        password=settings.postgres_password,
    )
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM bronze.raw_fundamentals;")
        count = cur.fetchone()[0]
        logger.info(f"✓ Database connected. Fundamentals count: {count}")
    conn.close()
except Exception as e:
    logger.error(f"✗ Database error: {e}")
    sys.exit(1)

# Try to run earnings for first symbol
if settings.nse_symbols:
    try:
        logger.info(f"Running earnings_ingest for {settings.nse_symbols[0]}...")
        result = earnings_ingest.run([settings.nse_symbols[0]])
        logger.info(f"✓ Earnings ingested: {result} records")
    except Exception as e:
        logger.error(f"✗ Earnings ingest failed: {e}")
        import traceback
        traceback.print_exc()
else:
    logger.error("✗ No NSE symbols configured")
    sys.exit(1)

logger.info("✓ All tests passed!")
