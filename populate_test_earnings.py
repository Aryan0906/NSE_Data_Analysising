#!/usr/bin/env python3
"""
Populate test earnings data directly into database.
Run from project root: python populate_test_earnings.py
"""

import sys
import os
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(__file__))

import psycopg2
from backend.pipeline.settings import settings

def populate_test_earnings():
    """Insert sample earnings data into public.earnings_summaries"""

    test_data = [
        {
            "ticker": "INFY",
            "summary": "Infosys reported strong Q3 FY2026 results with EPS of ₹24.50, representing a 12% YoY growth. The P/E ratio of 28.5x reflects market confidence in the company's digital transformation initiatives. Market cap stands at ₹8.2 Lakh Cr. Book value of ₹86.75 shows robust equity base. 52-week range: ₹1,450-₹1,850 indicates healthy volatility. Sentiment remains positive with ongoing cloud and AI investments driving growth.",
        },
        {
            "ticker": "TCS",
            "summary": "Tata Consultancy Services delivered solid Q3 results with EPS of ₹34.20, up 8% YoY. Trading at P/E of 32.1x, TCS maintains premium valuation due to market leadership. Market cap of ₹14.5 Lakh Cr reflects dominance in IT services. Book value at ₹145.30 shows strong fundamentals. 52-week high of ₹4,200 demonstrates investor optimism. Sector P/E of 28.5x suggests TCS trades at a reasonable premium to peers.",
        },
        {
            "ticker": "WIPRO",
            "summary": "Wipro's Q3 FY2026 earnings show EPS of ₹8.75, with moderate growth trajectory. P/E ratio of 22.3x indicates reasonable valuation compared to peers. Market cap of ₹2.8 Lakh Cr reflects mid-tier positioning in IT services. Book value stands at ₹52.40. 52-week range of ₹380-₹520 shows modest performance. Recent restructuring initiatives and focus on high-margin services are expected to drive future growth.",
        },
        {
            "ticker": "RELIANCE",
            "summary": "Reliance Industries reported mixed Q3 results with EPS of ₹15.60, reflecting oil price volatility. P/E ratio of 24.8x appears reasonable given diversified business portfolio. Market cap of ₹19.2 Lakh Cr makes it India's most valuable company. Book value of ₹168.90 demonstrates strong asset base. 52-week range of ₹2,700-₹3,100 shows stable trading. Refining and retail segments continue to perform well.",
        },
        {
            "ticker": "HDFC",
            "summary": "HDFC Bank's Q3 FY2026 earnings came in with EPS of ₹28.40, reflecting strong credit growth. Trading at P/E of 26.5x, the bank commands market respect. Market cap of ₹11.8 Lakh Cr reflects leadership position. Book value of ₹450.50 shows solid capital position. 52-week high of ₹1,920 indicates bullish sentiment. Asset quality remains strong with loan growth outpacing deposits.",
        },
    ]

    conn = psycopg2.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        dbname=settings.postgres_db,
        user=settings.postgres_user,
        password=settings.postgres_password,
    )

    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            for i, data in enumerate(test_data):
                report_date = datetime.now() - timedelta(days=i)

                cur.execute(
                    """
                    INSERT INTO public.earnings_summaries
                    (ticker, report_date, source_file, summary, created_at)
                    VALUES (%s, %s, %s, %s, NOW())
                    ON CONFLICT (ticker, report_date) DO UPDATE
                    SET summary = EXCLUDED.summary, created_at = NOW()
                    """,
                    (
                        data["ticker"],
                        report_date.date(),
                        f"test-earnings-{data['ticker']}",
                        data["summary"],
                    ),
                )
                print(f"✓ Inserted earnings for {data['ticker']}")

        print(f"\n✓ Successfully populated {len(test_data)} test earnings records!")
        print("→ Reload http://localhost:8501/earnings_summary to see data")

    except Exception as e:
        print(f"✗ Error: {e}")
        return False
    finally:
        conn.close()

    return True

if __name__ == "__main__":
    try:
        success = populate_test_earnings()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"✗ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
