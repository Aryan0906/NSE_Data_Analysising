"""
tests/test_transform.py
========================
Unit tests for the four pure-function price helpers in transform.py.
No database connection or HuggingFace API calls are made.
"""
import math
import pytest

# Import the helpers directly — they have no side-effects at module level.
from backend.pipeline.transform import (
    _calc_sma,
    _calc_ema,
    _calc_rsi,
    _normalise_52w,
)


# ─────────────────────────────────────────────────
# _calc_sma
# ─────────────────────────────────────────────────

class TestCalcSma:
    def test_happy_path(self):
        """Last `window` elements are averaged."""
        closes = [1.0, 2.0, 3.0, 4.0, 5.0]
        result = _calc_sma(closes, 3)
        assert result == pytest.approx(4.0)

    def test_exact_window_size(self):
        """Exactly `window` elements → same as mean."""
        closes = [10.0, 20.0, 30.0]
        assert _calc_sma(closes, 3) == pytest.approx(20.0)

    def test_insufficient_data_returns_none(self):
        """Fewer elements than window → None."""
        assert _calc_sma([1.0, 2.0], 5) is None

    def test_empty_returns_none(self):
        assert _calc_sma([], 5) is None

    def test_window_one(self):
        """Window of 1 → last value."""
        assert _calc_sma([7.0, 8.0, 9.0], 1) == pytest.approx(9.0)


# ─────────────────────────────────────────────────
# _calc_ema
# ─────────────────────────────────────────────────

class TestCalcEma:
    def test_constant_series_equals_value(self):
        """EMA of identical values must equal that value."""
        closes = [5.0] * 20
        result = _calc_ema(closes, 10)
        assert result == pytest.approx(5.0, rel=1e-4)

    def test_insufficient_data_returns_none(self):
        assert _calc_ema([1.0, 2.0], 5) is None

    def test_empty_returns_none(self):
        assert _calc_ema([], 10) is None

    def test_exact_window_size(self):
        """With exactly `window` elements the EMA equals the SMA seed."""
        closes = [2.0, 4.0, 6.0]
        result = _calc_ema(closes, 3)
        # No extra elements to compound, so result == (2+4+6)/3 == 4.0
        assert result == pytest.approx(4.0)

    def test_uptrend_ema_approaches_close(self):
        """EMA should be below the last close for a monotone rising series."""
        closes = list(range(1, 31))          # 1..30
        ema = _calc_ema(closes, 10)
        assert ema is not None
        assert ema < closes[-1]              # smoothed < last bar
        assert ema > closes[0]              # but above the start


# ─────────────────────────────────────────────────
# _calc_rsi
# ─────────────────────────────────────────────────

class TestCalcRsi:
    def test_all_gains_returns_100(self):
        """Monotone rising series → RSI == 100."""
        closes = list(range(1, 17))          # 16 values → 15 deltas, period 14
        result = _calc_rsi(closes, period=14)
        assert result == pytest.approx(100.0)

    def test_all_losses_returns_zero(self):
        """Monotone falling series → RSI ≈ 0."""
        closes = list(range(16, 0, -1))      # 16..1
        result = _calc_rsi(closes, period=14)
        assert result is not None
        assert result == pytest.approx(0.0, abs=1e-3)

    def test_insufficient_data_returns_none(self):
        """Need at least period + 1 values."""
        assert _calc_rsi([1.0, 2.0, 3.0], period=14) is None

    def test_exact_minimum_data(self):
        """period + 1 values → result returned, not None."""
        closes = [float(i) for i in range(1, 16)]   # 15 values, period=14
        result = _calc_rsi(closes, period=14)
        assert result is not None
        assert 0.0 <= result <= 100.0

    def test_alternating_gains_losses(self):
        """Balanced gains/losses → RSI near 50."""
        closes = []
        val = 100.0
        for i in range(30):
            val = val + 1.0 if i % 2 == 0 else val - 1.0
            closes.append(val)
        result = _calc_rsi(closes, period=14)
        assert result is not None
        assert 40 < result < 60


# ─────────────────────────────────────────────────
# _normalise_52w
# ─────────────────────────────────────────────────

class TestNormalise52w:
    def test_midpoint(self):
        """Value at midpoint of range → 0.5."""
        assert _normalise_52w(50.0, 0.0, 100.0) == pytest.approx(0.5)

    def test_at_high(self):
        """Value == high → 1.0."""
        assert _normalise_52w(100.0, 0.0, 100.0) == pytest.approx(1.0)

    def test_at_low(self):
        """Value == low → 0.0."""
        assert _normalise_52w(0.0, 0.0, 100.0) == pytest.approx(0.0)

    def test_clamp_below_low(self):
        """Value below low is clamped to 0.0 (Rule 4)."""
        assert _normalise_52w(-10.0, 0.0, 100.0) == pytest.approx(0.0)

    def test_clamp_above_high(self):
        """Value above high is clamped to 1.0 (Rule 4)."""
        assert _normalise_52w(150.0, 0.0, 100.0) == pytest.approx(1.0)

    def test_same_high_and_low_returns_none(self):
        """Zero range → division by zero risk → returns None."""
        assert _normalise_52w(50.0, 50.0, 50.0) is None

    def test_result_is_rounded_to_4dp(self):
        """Output has at most 4 decimal places."""
        result = _normalise_52w(1.0, 0.0, 3.0)
        assert result is not None
        assert result == round(result, 4)
