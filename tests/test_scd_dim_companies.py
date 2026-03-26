"""
tests/test_scd_dim_companies.py
================================
Sprint 3 — Unit tests for SCD Type 2 (slowly-changing dimension) logic
applied to *gold.dim_companies*.

These tests are PURE PYTHON — no database connection is required.
The SCD-2 algorithm is expressed as a standalone helper function
``apply_scd2_upsert`` so it can be tested without a live DB.

SCD Type 2 invariants under test:
1. A brand-new symbol is inserted with  is_current=True, effective_to=None.
2. Changing a tracked attribute closes the old row (is_current=False,
   effective_to=<today>) and opens a new row (is_current=True).
3. At most ONE row per symbol may have is_current=True at any time.
4. Historical rows are preserved (not deleted) after a version bump.
5. Updating a tracked attribute twice creates exactly two closed rows
   plus one open row (total 3 versions).
"""

from __future__ import annotations

import copy
from datetime import date, timedelta
from typing import Any

import pytest

# ---------------------------------------------------------------------------
# Pure-Python SCD-2 helper (mirrors the logic in gold.dim_companies ETL)
# ---------------------------------------------------------------------------

TRACKED_COLUMNS = {"sector", "industry", "exchange", "company_name"}


def _company_key(symbol: str, version: int) -> int:
    """Deterministic surrogate key  — hash-based for test purposes."""
    return hash((symbol, version)) & 0x7FFFFFFF


def apply_scd2_upsert(
    rows: list[dict[str, Any]],
    new_record: dict[str, Any],
    as_of: date | None = None,
) -> list[dict[str, Any]]:
    """
    Apply a SCD Type 2 upsert to *rows* (list of dim_companies dicts).

    Rules
    -----
    * If no existing row exists for ``new_record["symbol"]``, INSERT with
      is_current=True, effective_from=as_of, effective_to=None.
    * If an existing CURRENT row exists AND a tracked attribute has changed,
      CLOSE the current row (is_current=False, effective_to=as_of - 1 day)
      and INSERT a new row.
    * If no tracked attribute has changed, the rows list is returned unchanged.

    Parameters
    ----------
    rows        : current in-memory dimension (mutated copy is returned)
    new_record  : the incoming record to upsert
    as_of       : effective date for the operation (defaults to today)

    Returns
    -------
    Updated rows list (original is NOT mutated).
    """
    if as_of is None:
        as_of = date.today()

    rows = copy.deepcopy(rows)  # never mutate the caller's list
    symbol = new_record["symbol"]

    # Find the CURRENT active row for this symbol
    current_rows = [r for r in rows if r["symbol"] == symbol and r["is_current"]]

    if not current_rows:
        # ── INSERT: brand-new symbol ─────────────────────────────────────────
        next_version = 1
        new_row = {
            "company_key":   _company_key(symbol, next_version),
            "symbol":        symbol,
            "company_name":  new_record.get("company_name"),
            "sector":        new_record.get("sector"),
            "industry":      new_record.get("industry"),
            "exchange":      new_record.get("exchange"),
            "is_current":    True,
            "effective_from": as_of,
            "effective_to":  None,
            "_version":       next_version,
        }
        rows.append(new_row)
        return rows

    # Exactly one current row should exist (invariant enforced separately)
    current = current_rows[0]

    # Detect changes in tracked columns
    changed = any(
        current.get(col) != new_record.get(col) for col in TRACKED_COLUMNS
    )

    if not changed:
        return rows  # no-op

    # ── UPDATE: close the old row, open a new version ────────────────────────
    current["is_current"] = False
    current["effective_to"] = as_of - timedelta(days=1)

    all_versions = [r for r in rows if r["symbol"] == symbol]
    next_version = max(r["_version"] for r in all_versions) + 1

    new_row = {
        "company_key":   _company_key(symbol, next_version),
        "symbol":        symbol,
        "company_name":  new_record.get("company_name"),
        "sector":        new_record.get("sector"),
        "industry":      new_record.get("industry"),
        "exchange":      new_record.get("exchange"),
        "is_current":    True,
        "effective_from": as_of,
        "effective_to":  None,
        "_version":       next_version,
    }
    rows.append(new_row)
    return rows


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def empty_dim() -> list[dict]:
    """An empty dim_companies table."""
    return []


@pytest.fixture()
def initial_record() -> dict:
    return {
        "symbol":       "RELIANCE",
        "company_name": "Reliance Industries Ltd",
        "sector":       "Energy",
        "industry":     "Integrated Oil & Gas",
        "exchange":     "NSE",
    }


@pytest.fixture()
def populated_dim(initial_record) -> list[dict]:
    """dim with one CURRENT row for RELIANCE."""
    return apply_scd2_upsert([], initial_record, as_of=date(2024, 1, 1))


# ---------------------------------------------------------------------------
# Test 1: Insert initial record — is_current=True, effective_to=None
# ---------------------------------------------------------------------------

class TestInsertInitialRecord:
    def test_single_row_created(self, empty_dim, initial_record):
        result = apply_scd2_upsert(empty_dim, initial_record, as_of=date(2024, 1, 1))
        assert len(result) == 1, "Should have exactly one row after first insert"

    def test_is_current_true(self, empty_dim, initial_record):
        result = apply_scd2_upsert(empty_dim, initial_record, as_of=date(2024, 1, 1))
        assert result[0]["is_current"] is True

    def test_effective_to_is_none(self, empty_dim, initial_record):
        result = apply_scd2_upsert(empty_dim, initial_record, as_of=date(2024, 1, 1))
        assert result[0]["effective_to"] is None, "Open row must have effective_to=NULL"

    def test_effective_from_matches_as_of(self, empty_dim, initial_record):
        as_of = date(2024, 1, 1)
        result = apply_scd2_upsert(empty_dim, initial_record, as_of=as_of)
        assert result[0]["effective_from"] == as_of

    def test_tracked_columns_copied_correctly(self, empty_dim, initial_record):
        result = apply_scd2_upsert(empty_dim, initial_record, as_of=date(2024, 1, 1))
        row = result[0]
        assert row["sector"]       == "Energy"
        assert row["industry"]     == "Integrated Oil & Gas"
        assert row["exchange"]     == "NSE"
        assert row["company_name"] == "Reliance Industries Ltd"

    def test_symbol_persisted(self, empty_dim, initial_record):
        result = apply_scd2_upsert(empty_dim, initial_record, as_of=date(2024, 1, 1))
        assert result[0]["symbol"] == "RELIANCE"


# ---------------------------------------------------------------------------
# Test 2: Updating a tracked attribute creates a new version
# ---------------------------------------------------------------------------

class TestUpdateCreatesNewVersion:
    def test_two_rows_after_sector_change(self, populated_dim):
        changed = {
            "symbol":       "RELIANCE",
            "company_name": "Reliance Industries Ltd",
            "sector":       "Conglomerates",   # changed
            "industry":     "Integrated Oil & Gas",
            "exchange":     "NSE",
        }
        result = apply_scd2_upsert(populated_dim, changed, as_of=date(2024, 6, 1))
        reliance_rows = [r for r in result if r["symbol"] == "RELIANCE"]
        assert len(reliance_rows) == 2, "Should have old + new row"

    def test_new_row_has_updated_sector(self, populated_dim):
        changed = {**populated_dim[0], "sector": "Conglomerates"}
        result = apply_scd2_upsert(populated_dim, changed, as_of=date(2024, 6, 1))
        current = next(r for r in result if r["is_current"])
        assert current["sector"] == "Conglomerates"

    def test_old_row_is_closed(self, populated_dim):
        changed = {**populated_dim[0], "sector": "Conglomerates"}
        result = apply_scd2_upsert(populated_dim, changed, as_of=date(2024, 6, 1))
        closed = [r for r in result if not r["is_current"]]
        assert len(closed) == 1
        assert closed[0]["effective_to"] == date(2024, 5, 31)  # day before as_of

    def test_no_change_returns_same_row_count(self, populated_dim):
        """Identical record → no new version, no row count change."""
        same = copy.deepcopy(populated_dim[0])
        result = apply_scd2_upsert(populated_dim, same, as_of=date(2024, 6, 1))
        assert len(result) == len(populated_dim)

    def test_non_tracked_column_change_is_noop(self, populated_dim):
        """Changing a non-tracked attribute must NOT trigger a new version."""
        tweaked = dict(populated_dim[0])
        tweaked["_version"] = 99          # not a tracked column
        result = apply_scd2_upsert(populated_dim, tweaked, as_of=date(2024, 6, 1))
        assert len(result) == len(populated_dim), "Non-tracked change should be no-op"


# ---------------------------------------------------------------------------
# Test 3: Exactly one is_current=True row per symbol at any time
# ---------------------------------------------------------------------------

class TestOnlyOneCurrentPerSymbol:
    def test_single_current_after_insert(self, empty_dim, initial_record):
        result = apply_scd2_upsert(empty_dim, initial_record, as_of=date(2024, 1, 1))
        current = [r for r in result if r["symbol"] == "RELIANCE" and r["is_current"]]
        assert len(current) == 1

    def test_single_current_after_update(self, populated_dim):
        changed = {**populated_dim[0], "sector": "Conglomerates"}
        result = apply_scd2_upsert(populated_dim, changed, as_of=date(2024, 6, 1))
        current = [r for r in result if r["symbol"] == "RELIANCE" and r["is_current"]]
        assert len(current) == 1

    def test_single_current_after_two_updates(self, populated_dim):
        step1 = apply_scd2_upsert(
            populated_dim,
            {**populated_dim[0], "sector": "Conglomerates"},
            as_of=date(2024, 6, 1),
        )
        current1 = next(r for r in step1 if r["is_current"])
        step2 = apply_scd2_upsert(
            step1,
            {**current1, "sector": "Technology"},
            as_of=date(2024, 12, 1),
        )
        current = [r for r in step2 if r["symbol"] == "RELIANCE" and r["is_current"]]
        assert len(current) == 1

    def test_multiple_symbols_each_have_one_current(self, empty_dim):
        rec_a = {"symbol": "TCS",      "sector": "IT", "industry": "IT Services",
                 "company_name": "Tata Consultancy", "exchange": "NSE"}
        rec_b = {"symbol": "INFY",     "sector": "IT", "industry": "IT Services",
                 "company_name": "Infosys Ltd",       "exchange": "NSE"}
        result = apply_scd2_upsert(empty_dim, rec_a, as_of=date(2024, 1, 1))
        result = apply_scd2_upsert(result,    rec_b, as_of=date(2024, 1, 1))
        for sym in ("TCS", "INFY"):
            current = [r for r in result if r["symbol"] == sym and r["is_current"]]
            assert len(current) == 1, f"{sym} should have exactly 1 current row"


# ---------------------------------------------------------------------------
# Test 4: Historical rows are preserved (not deleted) after a version bump
# ---------------------------------------------------------------------------

class TestHistoryPreserved:
    def test_old_row_still_exists_after_update(self, populated_dim):
        old_key = populated_dim[0]["company_key"]
        changed = {**populated_dim[0], "sector": "Conglomerates"}
        result = apply_scd2_upsert(populated_dim, changed, as_of=date(2024, 6, 1))
        keys = [r["company_key"] for r in result]
        assert old_key in keys, "Old company_key must remain in the dimension"

    def test_old_row_sector_unchanged_after_update(self, populated_dim):
        old_sector = populated_dim[0]["sector"]
        changed = {**populated_dim[0], "sector": "Conglomerates"}
        result = apply_scd2_upsert(populated_dim, changed, as_of=date(2024, 6, 1))
        closed_rows = [r for r in result if not r["is_current"]]
        assert len(closed_rows) == 1
        assert closed_rows[0]["sector"] == old_sector, "Historical row must retain original values"

    def test_three_versions_after_two_updates(self, populated_dim):
        step1 = apply_scd2_upsert(
            populated_dim,
            {**populated_dim[0], "sector": "Conglomerates"},
            as_of=date(2024, 6, 1),
        )
        current1 = next(r for r in step1 if r["is_current"])
        step2 = apply_scd2_upsert(
            step1,
            {**current1, "sector": "Technology"},
            as_of=date(2024, 12, 1),
        )
        reliance = [r for r in step2 if r["symbol"] == "RELIANCE"]
        assert len(reliance) == 3, "Three updates ⟹ three versions (v1 closed, v2 closed, v3 open)"

    def test_effective_to_chain_is_contiguous(self, populated_dim):
        """v1.effective_to + 1 day == v2.effective_from."""
        step1 = apply_scd2_upsert(
            populated_dim,
            {**populated_dim[0], "sector": "Conglomerates"},
            as_of=date(2024, 6, 1),
        )
        v1 = next(r for r in step1 if r["_version"] == 1)
        v2 = next(r for r in step1 if r["_version"] == 2)
        assert v1["effective_to"] + timedelta(days=1) == v2["effective_from"]


# ---------------------------------------------------------------------------
# Test 5: No duplicate open (is_current=True) rows for the same symbol
# ---------------------------------------------------------------------------

class TestNoDuplicateOpenRecords:
    def _count_current(self, rows: list[dict], symbol: str) -> int:
        return sum(1 for r in rows if r["symbol"] == symbol and r["is_current"])

    def test_insert_is_idempotent_for_unchanged_record(self, populated_dim, initial_record):
        """Inserting the same record twice should not double the open rows."""
        result = apply_scd2_upsert(populated_dim, initial_record, as_of=date(2024, 3, 1))
        assert self._count_current(result, "RELIANCE") == 1

    def test_sequential_updates_never_produce_two_open_rows(self, populated_dim):
        """Two sequential sector changes: at every step only one current row."""
        changes = [
            ("Conglomerates", date(2024, 6, 1)),
            ("Technology",    date(2024, 9, 1)),
            ("Healthcare",    date(2024, 12, 1)),
        ]
        rows = populated_dim
        for sector, as_of in changes:
            current = next(r for r in rows if r["is_current"])
            rows = apply_scd2_upsert(rows, {**current, "sector": sector}, as_of=as_of)
            assert self._count_current(rows, "RELIANCE") == 1, \
                f"Duplicate open rows found after update to sector='{sector}'"

    def test_independent_symbols_dont_interfere(self, empty_dim):
        """Updating RELIANCE must not affect TCS's current status."""
        rec_r = {"symbol": "RELIANCE", "sector": "Energy", "industry": "Oil",
                 "company_name": "Reliance", "exchange": "NSE"}
        rec_t = {"symbol": "TCS",      "sector": "IT",     "industry": "Services",
                 "company_name": "TCS",      "exchange": "NSE"}

        rows = apply_scd2_upsert(empty_dim, rec_r, as_of=date(2024, 1, 1))
        rows = apply_scd2_upsert(rows,      rec_t, as_of=date(2024, 1, 1))
        # Update only RELIANCE
        rows = apply_scd2_upsert(rows, {**rec_r, "sector": "Conglomerates"},
                                 as_of=date(2024, 6, 1))

        assert self._count_current(rows, "TCS")      == 1, "TCS should still have 1 current row"
        assert self._count_current(rows, "RELIANCE") == 1, "RELIANCE should have 1 current row"
