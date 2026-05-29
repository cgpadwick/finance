"""Unit tests for the momentum engine and store, using synthetic data.

These run without network or the real DuckDB file — they build a tiny in-memory
universe with known properties and assert the math/ranking behaves correctly.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from finance.config import AppConfig
from finance.momentum import apply_universe_filters, compute_signals, score
from finance.store import BarStore


def _make_close(n_days=300, start=100.0, daily_drift=0.0, noise=0.0, seed=0):
    rng = np.random.default_rng(seed)
    rets = daily_drift + noise * rng.standard_normal(n_days)
    prices = start * np.cumprod(1 + rets)
    idx = pd.bdate_range("2023-01-02", periods=n_days)
    return pd.Series(prices, index=idx)


def _wide_from(series_map):
    close = pd.DataFrame(series_map)
    volume = pd.DataFrame(
        {k: np.full(len(v), 1_000_000.0) for k, v in series_map.items()},
        index=close.index,
    )
    return close, volume


def test_signals_basic_shapes_and_values():
    cfg = AppConfig()
    # UP trends steadily; DOWN declines; FLAT is flat.
    close, volume = _wide_from(
        {
            "UP": _make_close(daily_drift=0.001, noise=0.005, seed=1),
            "DOWN": _make_close(daily_drift=-0.001, noise=0.005, seed=2),
            "FLAT": _make_close(daily_drift=0.0, noise=0.005, seed=3),
        }
    )
    sig = compute_signals(close, volume, cfg, as_of_pos=-1)
    # Uptrend should have positive 12-1 momentum, downtrend negative.
    assert sig.loc["UP", "mom_12_1"] > 0
    assert sig.loc["DOWN", "mom_12_1"] < 0
    # Uptrend should sit near its 52w high; downtrend far below.
    assert sig.loc["UP", "high_52w_proximity"] > sig.loc["DOWN", "high_52w_proximity"]
    # Risk-adjusted momentum ordering matches raw direction here.
    assert sig.loc["UP", "risk_adjusted_mom"] > sig.loc["DOWN", "risk_adjusted_mom"]


def test_penny_and_illiquid_filtered_out():
    cfg = AppConfig()
    close = pd.DataFrame(
        {
            "GOOD": _make_close(start=100.0, daily_drift=0.001, noise=0.005, seed=4),
            "PENNY": _make_close(start=1.0, daily_drift=0.002, noise=0.02, seed=5),
        }
    )
    volume = pd.DataFrame(
        {
            "GOOD": np.full(len(close), 1_000_000.0),
            "PENNY": np.full(len(close), 1_000_000.0),  # but price < $5
        },
        index=close.index,
    )
    sig = compute_signals(close, volume, cfg, as_of_pos=-1)
    elig = apply_universe_filters(sig, cfg)
    assert "GOOD" in elig.index
    assert "PENNY" not in elig.index  # price floor removes it


def test_illiquid_filtered_by_dollar_volume():
    cfg = AppConfig()
    close = pd.DataFrame(
        {
            "LIQUID": _make_close(start=50.0, daily_drift=0.001, seed=6),
            "THIN": _make_close(start=50.0, daily_drift=0.001, seed=7),
        }
    )
    volume = pd.DataFrame(
        {
            "LIQUID": np.full(len(close), 1_000_000.0),  # $50 * 1M = $50M/day
            "THIN": np.full(len(close), 100.0),  # $50 * 100 = $5k/day
        },
        index=close.index,
    )
    sig = compute_signals(close, volume, cfg, as_of_pos=-1)
    elig = apply_universe_filters(sig, cfg)
    assert "LIQUID" in elig.index
    assert "THIN" not in elig.index


def test_score_ranks_winner_above_loser():
    cfg = AppConfig()
    series = {
        f"W{i}": _make_close(daily_drift=0.0015, noise=0.004, seed=10 + i)
        for i in range(8)
    }
    series.update(
        {f"L{i}": _make_close(daily_drift=-0.0015, noise=0.004, seed=50 + i) for i in range(8)}
    )
    close, volume = _wide_from(series)
    sig = compute_signals(close, volume, cfg, as_of_pos=-1)
    elig = apply_universe_filters(sig, cfg)
    ranked = score(elig, cfg)
    top = ranked.iloc[0].name
    bottom = ranked.iloc[-1].name
    assert top.startswith("W")
    assert bottom.startswith("L")
    # composite score is monotonic with rank
    assert ranked["score"].is_monotonic_decreasing


def test_disabled_signal_weight_excluded():
    cfg = AppConfig()
    cfg.momentum.signals.path_quality = 0.0  # disable
    active = cfg.momentum.signals.active()
    assert "path_quality" not in active
    assert pytest.approx(sum(active.values()), abs=1e-9) == 1.0


def test_store_roundtrip(tmp_path):
    store = BarStore(tmp_path / "t.duckdb")
    df = pd.DataFrame(
        {
            "open": [1.0, 2.0],
            "high": [1.0, 2.0],
            "low": [1.0, 2.0],
            "close": [1.5, 2.5],
            "volume": [100.0, 200.0],
        },
        index=pd.to_datetime(["2020-01-02", "2020-01-03"]).date,
    )
    df.index.name = "date"
    assert store.upsert_bars("X", df) == 2
    # idempotent re-pull
    assert store.upsert_bars("X", df) == 2
    assert store.list_symbols() == ["X"]
    assert len(store.read_bars(["X"])) == 2
    store.close()
