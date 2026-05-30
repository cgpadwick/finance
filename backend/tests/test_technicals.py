"""Tests for the deterministic technicals engine (synthetic data)."""

import numpy as np
import pandas as pd

from finance.technicals import analyze_stock, rsi, technicals_summary_text


def _ohlcv(prices):
    n = len(prices)
    idx = pd.bdate_range("2023-01-02", periods=n)
    c = pd.Series(prices, dtype=float)
    return pd.DataFrame(
        {
            "date": idx,
            "open": c.values,
            "high": (c * 1.01).values,
            "low": (c * 0.99).values,
            "close": c.values,
            "volume": np.full(n, 1_000_000.0),
        }
    )


def test_uptrend_is_bullish():
    prices = list(100 * np.cumprod(1 + np.full(300, 0.003)))  # steady uptrend
    t = analyze_stock(_ohlcv(prices))
    assert t["trend"]["above_ma200"] is True
    assert t["trend"]["golden_cross"] is True
    assert t["verdict"]["bullish"] > t["verdict"]["bearish"]
    assert t["returns"]["r_6m"] > 0


def test_downtrend_is_bearish():
    prices = list(100 * np.cumprod(1 + np.full(300, -0.003)))  # steady downtrend
    t = analyze_stock(_ohlcv(prices))
    assert t["trend"]["above_ma200"] is False
    assert t["verdict"]["bearish"] >= t["verdict"]["bullish"]
    assert t["returns"]["r_6m"] < 0


def test_rsi_bounds():
    up = pd.Series(100 * np.cumprod(1 + np.full(100, 0.01)))
    assert rsi(up).iloc[-1] > 70  # persistent gains -> high RSI
    down = pd.Series(100 * np.cumprod(1 + np.full(100, -0.01)))
    assert rsi(down).iloc[-1] < 30


def test_insufficient_data():
    t = analyze_stock(_ohlcv([100, 101, 102]))
    assert t.get("insufficient_data") is True


def test_summary_text_mentions_computed_flag():
    prices = list(100 * np.cumprod(1 + np.full(300, 0.002)))
    t = analyze_stock(_ohlcv(prices))
    text = technicals_summary_text(t)
    assert "do not invent numbers" in text
    assert "RSI" in text
