"""FastAPI backend for the momentum screener.

Endpoints (all under /api):
  GET /api/health                  -> liveness + data coverage summary
  GET /api/config                  -> current tunable config (filters + weights)
  GET /api/leaderboard             -> ranked universe; optional ?overrides for live tuning
  GET /api/stock/{symbol}          -> adjusted OHLCV + moving averages + this stock's signals

In production the built React app is served from the same origin (see
serve_frontend), so no CORS is needed; in dev we allow the Vite origin.

Leaderboard computation is cached per config so repeated requests are instant;
overriding filters/weights computes (and caches) a variant.
"""

from __future__ import annotations

import json
from functools import lru_cache

import numpy as np
import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from finance.config import REPO_ROOT, AppConfig, load_config
from finance.momentum import apply_universe_filters, compute_leaderboard, compute_signals
from finance.store import BarStore

app = FastAPI(title="Momentum Screener API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_CONFIG = load_config()
FRONTEND_DIST = REPO_ROOT / "frontend" / "dist"


def _read_store() -> BarStore:
    return BarStore(read_only=True)


def _clean_records(df: pd.DataFrame) -> list[dict]:
    """JSON-safe records: NaN/inf -> None."""
    df = df.replace([np.inf, -np.inf], np.nan)
    return json.loads(df.to_json(orient="records"))


# ----- leaderboard (cached per override signature) -----


@lru_cache(maxsize=64)
def _cached_leaderboard(override_key: str) -> str:
    """Compute leaderboard for a config-override signature; cache the JSON."""
    overrides = json.loads(override_key)
    as_of = overrides.get("as_of")  # not a config field; passed through
    cfg = _config_with_overrides(overrides)
    store = _read_store()
    try:
        lb = compute_leaderboard(store, cfg, as_of_date=as_of)
    finally:
        store.close()
    if lb.empty:
        return json.dumps({"as_of": None, "count": 0, "rows": []})
    payload = {
        "as_of": lb["as_of"].iloc[0],
        "count": int(len(lb)),
        "rows": _clean_records(lb),
    }
    return json.dumps(payload)


def _config_with_overrides(overrides: dict) -> AppConfig:
    cfg = BASE_CONFIG.model_copy(deep=True)
    u, m = cfg.universe, cfg.momentum
    if "min_price" in overrides:
        u.min_price = float(overrides["min_price"])
    if "min_dollar_volume" in overrides:
        u.min_dollar_volume = float(overrides["min_dollar_volume"])
    if "min_market_cap" in overrides:
        u.min_market_cap = float(overrides["min_market_cap"])
    if "lookback_days" in overrides:
        lb = int(overrides["lookback_days"])
        m.lookback_days = lb
        # Keep the secondary (shorter) signal coherent with the main window.
        m.short_lookback_days = max(lb // 2, m.skip_days + 5)
    for sig in ("risk_adjusted_mom", "high_52w_proximity", "path_quality", "mom_3_6"):
        key = f"w_{sig}"
        if key in overrides:
            setattr(m.signals, sig, float(overrides[key]))
    return cfg


@app.get("/api/health")
def health():
    store = _read_store()
    try:
        cov = store.coverage()
        return {
            "status": "ok",
            "symbols": int(len(cov)),
            "total_bars": int(cov["n_bars"].sum()) if len(cov) else 0,
            "date_range": (
                [str(cov["first_date"].min()), str(cov["last_date"].max())]
                if len(cov)
                else None
            ),
        }
    finally:
        store.close()


@app.get("/api/config")
def get_config():
    return BASE_CONFIG.model_dump()


@app.get("/api/leaderboard")
def leaderboard(
    min_price: float | None = Query(None),
    min_dollar_volume: float | None = Query(None),
    min_market_cap: float | None = Query(None),
    w_risk_adjusted_mom: float | None = Query(None),
    w_high_52w_proximity: float | None = Query(None),
    w_path_quality: float | None = Query(None),
    w_mom_3_6: float | None = Query(None),
    lookback_days: int | None = Query(None, description="Momentum window (trading days)."),
    as_of: str | None = Query(None, description="Snapshot date YYYY-MM-DD (default latest)."),
):
    overrides = {
        k: v
        for k, v in {
            "min_price": min_price,
            "min_dollar_volume": min_dollar_volume,
            "min_market_cap": min_market_cap,
            "w_risk_adjusted_mom": w_risk_adjusted_mom,
            "w_high_52w_proximity": w_high_52w_proximity,
            "w_path_quality": w_path_quality,
            "w_mom_3_6": w_mom_3_6,
            "lookback_days": lookback_days,
            "as_of": as_of,
        }.items()
        if v is not None
    }
    key = json.dumps(overrides, sort_keys=True)
    return JSONResponse(content=json.loads(_cached_leaderboard(key)))


@app.get("/api/stock/{symbol}")
def stock_detail(
    symbol: str,
    ma: str = Query("20,50,200", description="Comma-separated MA windows."),
):
    symbol = symbol.upper()
    store = _read_store()
    try:
        bars = store.read_bars([symbol])
        if bars.empty:
            raise HTTPException(404, f"No data for {symbol}")
        bars = bars.sort_values("date")
        bars["date"] = pd.to_datetime(bars["date"]).dt.strftime("%Y-%m-%d")

        ma_windows = [int(w) for w in ma.split(",") if w.strip()]
        mas = {}
        close = bars["close"]
        for w in ma_windows:
            mas[f"ma_{w}"] = close.rolling(w, min_periods=1).mean().round(4).tolist()

        # This stock's current signals (even if it fails universe filters).
        close_w, vol_w = _stock_wide(store)
        signals = None
        if symbol in close_w.columns:
            sig = compute_signals(close_w, vol_w, BASE_CONFIG, as_of_pos=-1)
            if symbol in sig.index:
                row = sig.loc[symbol].replace([np.inf, -np.inf], np.nan)
                eligible = symbol in apply_universe_filters(sig, BASE_CONFIG).index
                signals = {**json.loads(row.to_json()), "eligible": eligible}

        ohlc = bars[["date", "open", "high", "low", "close", "volume"]]
        return {
            "symbol": symbol,
            "bars": _clean_records(ohlc),
            "moving_averages": mas,
            "signals": signals,
        }
    finally:
        store.close()


def _stock_wide(store: BarStore):
    bars = store.read_bars()
    bars["date"] = pd.to_datetime(bars["date"])
    close = bars.pivot(index="date", columns="symbol", values="close").sort_index()
    volume = bars.pivot(index="date", columns="symbol", values="volume").sort_index()
    return close, volume


# ----- serve the built React SPA (prod) from the same origin -----
# Hashed assets are served from /assets; every other non-/api path falls back to
# index.html so client-side routes (e.g. /stock/AAPL) deep-link correctly.
if FRONTEND_DIST.exists():
    from fastapi.responses import FileResponse

    assets_dir = FRONTEND_DIST / "assets"
    if assets_dir.exists():
        app.mount("/assets", StaticFiles(directory=str(assets_dir)), name="assets")

    @app.get("/{full_path:path}")
    def spa(full_path: str):
        candidate = FRONTEND_DIST / full_path
        if full_path and candidate.is_file():
            return FileResponse(str(candidate))
        return FileResponse(str(FRONTEND_DIST / "index.html"))
