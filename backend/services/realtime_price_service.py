#!/usr/bin/env python
"""
backend/services/realtime_price_service.py
============================================
Continuous background service that updates prices every 1 second.
Run as: python -m backend.services.realtime_price_service

Features:
- Updates every 1 second during market hours
- Lightweight caching to avoid API overload
- Only writes to DB when prices change
- Graceful shutdown on SIGINT
"""

from __future__ import annotations

import logging
import signal
import sys
import time
from datetime import datetime, time as time_obj

from backend.pipeline.realtime_prices import update_realtime_prices
from backend.pipeline.settings import settings

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=settings.log_level,
    format="%(asctime)s | %(levelname)s | realtime-svc | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)

RUNNING = True


def signal_handler(sig, frame):
    """Graceful shutdown."""
    global RUNNING
    logger.info("Shutdown signal received, cleaning up...")
    RUNNING = False
    sys.exit(0)


def is_market_hours() -> bool:
    """Check if current time is within NSE market hours (9:15 AM - 3:30 PM IST)."""
    from datetime import datetime, timedelta

    # Get current UTC time
    utc_now = datetime.utcnow()

    # Convert UTC to IST (UTC + 5:30)
    ist_now = utc_now + timedelta(hours=5, minutes=30)
    current_time = ist_now.time()

    # Market hours: 9:15 AM - 3:30 PM IST
    market_open = time_obj(9, 15)
    market_close = time_obj(15, 30)

    # Also check if it's a weekday (Monday=0, Sunday=6)
    is_weekday = ist_now.weekday() < 5

    return is_weekday and market_open <= current_time <= market_close


def main():
    """Main service loop."""
    global RUNNING

    # Register signal handler for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    logger.info("Starting real-time price service...")
    logger.info("Updates: Every 1 second during market hours (9:15 AM - 3:30 PM IST)")

    cycle = 0
    while RUNNING:
        try:
            cycle += 1

            # Check if market is open
            if not is_market_hours():
                logger.debug(f"[{cycle}] Market is closed, checking every 60s...")
                time.sleep(60)
                continue

            # Update prices
            logger.debug(f"[{cycle}] Fetching real-time prices...")
            results = update_realtime_prices()

            # Count updates
            total_updates = sum(v for v in results.values())
            if total_updates > 0:
                logger.info(f"[{cycle}] Updated {total_updates} prices")

            # Sleep 1 second
            time.sleep(1)

        except KeyboardInterrupt:
            logger.info("Received interrupt, shutting down...")
            RUNNING = False
            break
        except Exception as e:
            logger.error(f"Error in update cycle: {e}", exc_info=True)
            time.sleep(2)  # Wait 2s before retrying on error


if __name__ == "__main__":
    main()
