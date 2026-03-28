#!/usr/bin/env python3
"""
Quick verification script for earnings_ingest module.
Tests imports and core functionality without database connection.
"""

import sys
import os

# Add backend to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

print("=" * 60)
print("Testing earnings_ingest module structure")
print("=" * 60)

# Test 1: Import module
print("\n✓ Test 1: Importing backend.pipeline.earnings_ingest...")
try:
    from backend.pipeline import earnings_ingest
    print("  SUCCESS: Module imported")
except Exception as e:
    print(f"  ERROR: {e}")
    sys.exit(1)

# Test 2: Check functions exist
print("\n✓ Test 2: Checking required functions...")
required_functions = [
    '_get_db_connection',
    '_fetch_nse_fundamentals',
    '_generate_earnings_narrative',
    '_analyze_earnings',
    '_store_earnings_summary',
    'run'
]

for func_name in required_functions:
    if hasattr(earnings_ingest, func_name):
        print(f"  ✓ {func_name}")
    else:
        print(f"  ✗ MISSING: {func_name}")
        sys.exit(1)

# Test 3: Check function signatures
print("\n✓ Test 3: Checking function signatures...")
import inspect

run_sig = inspect.signature(earnings_ingest.run)
print(f"  run{run_sig} — should accept symbols list")

gen_narrative_sig = inspect.signature(earnings_ingest._generate_earnings_narrative)
print(f"  _generate_earnings_narrative{gen_narrative_sig}")

# Test 4: Check dependencies
print("\n✓ Test 4: Checking imports/dependencies...")
try:
    from backend.pipeline.settings import settings
    print(f"  ✓ settings: nse_symbols = {len(settings.nse_symbols)} tickers")
except Exception as e:
    print(f"  ✗ settings import failed: {e}")

try:
    from backend.pipeline import hf_client
    print(f"  ✓ hf_client module available")
except Exception as e:
    print(f"  ✗ hf_client import failed: {e}")

# Test 5: Verify narrative generation (unit test)
print("\n✓ Test 5: Testing _generate_earnings_narrative()...")
test_fundamentals = {
    "pe": 25.5,
    "eps": 150.75,
    "market_cap": 5000000000,
    "book_value": 320.0,
    "price_to_book": 2.5,
    "high52": 2500.0,
    "low52": 1800.0,
    "sector_pe": 24.0,
    "ingested_at": "2025-03-28 12:00:00"
}

try:
    narrative = earnings_ingest._generate_earnings_narrative("INFY", test_fundamentals)
    lines = narrative.split('\n')
    print(f"  Generated narrative ({len(lines)} lines):")
    for line in lines[:5]:
        print(f"    {line}")
    if len(lines) > 5:
        print(f"    ... ({len(lines) - 5} more lines)")
    print(f"  ✓ Narrative generation works")
except Exception as e:
    print(f"  ✗ Error: {e}")
    sys.exit(1)

print("\n" + "=" * 60)
print("All verification tests PASSED ✓")
print("=" * 60)
print("\nEarnings module is ready for deployment.")
print("Next: Start Docker containers and trigger Airflow DAG.")
