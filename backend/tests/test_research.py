"""Tests for research prompt building and hand-off behavior (no network)."""

from finance.config import Settings
from finance.research import build_prompt, get_or_generate
from finance.store import BarStore


def test_build_prompt_includes_context():
    ctx = {
        "symbol": "MU",
        "name": "MICRON TECHNOLOGY INC",
        "sector": "Technology",
        "industry": "Semiconductors & Related Devices",
        "as_of": "2026-05-29",
        "last_close": 924.0,
        "mom_12_1": 4.24,
        "risk_adjusted_mom": 7.0,
        "high_52w_proximity": 0.99,
        "volatility": 0.5,
    }
    p = build_prompt(ctx)
    assert "MU" in p and "MICRON TECHNOLOGY INC" in p
    assert "Technology" in p
    assert "99%" in p  # 52-week-high proximity formatted as percent


def test_handoff_when_no_key(tmp_path):
    store = BarStore(tmp_path / "t.duckdb")
    ctx = {"symbol": "MU", "name": "Micron", "as_of": "2026-05-29"}
    # generate=True but no key configured -> hand-off, never calls the network
    res = get_or_generate(
        ctx, store, generate=True, settings=Settings(anthropic_api_key="")
    )
    assert res["mode"] == "handoff"
    assert res["research_enabled"] is False
    assert "MU" in res["prompt"]
    store.close()


def test_returns_cached_report(tmp_path):
    store = BarStore(tmp_path / "t.duckdb")
    store.save_research("MU", "2026-05-29", "# cached report", "2026-05-29T00:00:00")
    ctx = {"symbol": "MU", "as_of": "2026-05-29"}
    res = get_or_generate(ctx, store, generate=True, settings=Settings(anthropic_api_key="x"))
    assert res["mode"] == "cached"
    assert res["report"] == "# cached report"
    store.close()
