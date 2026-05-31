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


@lru_cache(maxsize=16)
def _cached_embedding(key: str) -> str:
    params = json.loads(key)
    from finance.embedding import compute_embedding

    cfg = _config_with_overrides(params)
    # Reuse the cached (latest) leaderboard to skip the full-universe recompute.
    # For a historical as_of we must recompute, so leave lb=None.
    lb_df = None
    if not params.get("as_of"):
        lb_payload = json.loads(_cached_leaderboard(json.dumps({}, sort_keys=True)))
        if lb_payload.get("rows"):
            lb_df = pd.DataFrame(lb_payload["rows"])
    store = _read_store()
    try:
        emb = compute_embedding(
            store,
            cfg,
            method=params.get("method", "umap"),
            as_of_date=params.get("as_of"),
            dims=int(params.get("dims", 2)),
            sector=params.get("sector"),
            lb=lb_df,
        )
    finally:
        store.close()
    if emb.empty:
        return json.dumps({"count": 0, "points": []})
    return json.dumps({"count": int(len(emb)), "points": _clean_records(emb)})


@app.get("/api/embedding")
def embedding(
    method: str = Query("umap", description="umap | tsne | pca"),
    dims: int = Query(2, description="2 or 3"),
    sector: str | None = Query(None, description="restrict to one sector (faster)"),
    lookback_days: int | None = Query(None),
    as_of: str | None = Query(None),
):
    params = {"method": method, "dims": 3 if dims == 3 else 2}
    if sector is not None:
        params["sector"] = sector
    if lookback_days is not None:
        params["lookback_days"] = lookback_days
    if as_of is not None:
        params["as_of"] = as_of
    key = json.dumps(params, sort_keys=True)
    return JSONResponse(content=json.loads(_cached_embedding(key)))


def _relative_strength(symbol: str) -> dict | None:
    """Relative strength from the cached leaderboard: rank/percentile vs the
    whole market, and score/momentum vs the stock's own sector."""
    payload = json.loads(_cached_leaderboard(json.dumps({}, sort_keys=True)))
    rows = payload.get("rows", [])
    if not rows:
        return None
    df = pd.DataFrame(rows)
    hit = df[df["symbol"] == symbol]
    if hit.empty:
        return {"in_universe": False}
    row = hit.iloc[0]
    sector = row.get("sector")
    sect = df[df["sector"] == sector]
    return {
        "in_universe": True,
        "rank": int(row["rank"]),
        "universe_size": int(len(df)),
        "percentile": round(float(row["percentile"]), 4),
        "decile": int(row["decile"]),
        "score": round(float(row["score"]), 4),
        "sector": sector,
        "sector_size": int(len(sect)),
        "score_vs_sector": round(float(row["score"] - sect["score"].mean()), 4),
        "mom_vs_sector": round(float(row["mom_12_1"] - sect["mom_12_1"].mean()), 4),
        "z_breakdown": {
            k: (None if pd.isna(row.get(k)) else round(float(row.get(k)), 3))
            for k in ("z_risk_adjusted_mom", "z_high_52w_proximity",
                      "z_path_quality", "z_mom_3_6")
            if k in row
        },
    }


@app.get("/api/stock/{symbol}/technicals")
def stock_technicals(symbol: str):
    """Deterministic technical analysis + relative strength for one stock."""
    from finance.technicals import analyze_stock

    symbol = symbol.upper()
    store = _read_store()
    try:
        bars = store.read_bars([symbol])
        if bars.empty:
            raise HTTPException(404, f"No data for {symbol}")
        tech = analyze_stock(bars)
    finally:
        store.close()
    return {
        "symbol": symbol,
        "technicals": tech,
        "relative_strength": _relative_strength(symbol),
    }


@app.get("/api/sectors")
def sectors():
    """Sector leaderboard: average momentum score per sector (leading→lagging)."""
    payload = json.loads(_cached_leaderboard(json.dumps({}, sort_keys=True)))
    rows = payload.get("rows", [])
    if not rows:
        return {"as_of": payload.get("as_of"), "sectors": []}
    df = pd.DataFrame(rows)
    df["sector"] = df["sector"].fillna("Unknown")
    g = df.groupby("sector")
    agg = g.agg(
        count=("symbol", "size"),
        avg_score=("score", "mean"),
        median_score=("score", "median"),
        avg_mom_12_1=("mom_12_1", "mean"),
        avg_high_52w=("high_52w_proximity", "mean"),
        winners=("is_winner", "sum"),
        losers=("is_loser", "sum"),
    ).reset_index()
    agg = agg.sort_values("avg_score", ascending=False)
    return {"as_of": payload.get("as_of"), "sectors": _clean_records(agg)}


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
            "company": store.get_company(symbol),
            "bars": _clean_records(ohlc),
            "moving_averages": mas,
            "signals": signals,
        }
    finally:
        store.close()


def _stock_context(store: BarStore, symbol: str) -> dict:
    """Build the research context (company info + current signals) for a stock."""
    company = store.get_company(symbol) or {}
    ctx = {
        "symbol": symbol,
        "name": company.get("name"),
        "sector": company.get("sector"),
        "industry": company.get("industry"),
    }
    close_w, vol_w = _stock_wide(store)
    if symbol in close_w.columns:
        sig = compute_signals(close_w, vol_w, BASE_CONFIG, as_of_pos=-1)
        if symbol in sig.index:
            row = sig.loc[symbol].replace([np.inf, -np.inf], np.nan)
            for k in ("last_close", "mom_12_1", "risk_adjusted_mom",
                      "high_52w_proximity", "volatility"):
                v = row.get(k)
                ctx[k] = None if pd.isna(v) else float(v)
        ctx["as_of"] = pd.to_datetime(close_w.index[-1]).date().isoformat()
    return ctx


# A small curated model menu for the UI picker (all OpenRouter slugs).
RESEARCH_MODELS = [
    "deepseek/deepseek-v4-flash",
    "anthropic/claude-sonnet-4-6",
    "google/gemini-3-flash-preview",
    "openai/gpt-5-mini",
]


def research_config():
    from finance.config import get_settings

    s = get_settings()
    return {
        "enabled": s.research_enabled,
        "model": s.research_model,
        "models": RESEARCH_MODELS,
        "web_search": s.research_web_search,
    }


# register without decorator stacking issues
app.add_api_route("/api/research/config", research_config, methods=["GET"])


def _now() -> str:
    from datetime import datetime

    return datetime.now().isoformat(timespec="seconds")


@app.get("/api/stock/{symbol}/research")
def get_research(symbol: str):
    """Return a cached report if one exists (no generation / no cost)."""
    from finance.research import get_or_generate

    symbol = symbol.upper()
    store = _read_store()
    try:
        ctx = _stock_context(store, symbol)
        return get_or_generate(ctx, store, generate=False)
    finally:
        store.close()


@app.post("/api/stock/{symbol}/research")
def make_research(
    symbol: str,
    model: str | None = Query(None),
    force: bool = Query(False),
):
    """Generate (and cache) a report if a key is configured; else return the
    hand-off prompt. Generation needs a read-write store."""
    from finance.config import get_settings
    from finance.research import get_or_generate

    from finance.technicals import analyze_stock, technicals_summary_text

    symbol = symbol.upper()
    read_store = _read_store()
    try:
        ctx = _stock_context(read_store, symbol)
        # Ground the narrative in COMPUTED technicals (no hallucinated numbers).
        bars = read_store.read_bars([symbol])
        if not bars.empty:
            ctx["technicals_text"] = technicals_summary_text(analyze_stock(bars))
        if not get_settings().research_enabled:
            return get_or_generate(ctx, read_store, generate=False)
    finally:
        read_store.close()
    store = BarStore()
    try:
        return get_or_generate(
            ctx, store, generate=True, now=_now(), force=force, model=model
        )
    finally:
        store.close()


@app.get("/api/stock/{symbol}/panel")
def get_panel(symbol: str):
    """Cached bull/bear/facts analyst panel (no generation)."""
    from finance.research import get_or_generate_panel

    symbol = symbol.upper()
    store = _read_store()
    try:
        ctx = _stock_context(store, symbol)
        return get_or_generate_panel(ctx, store, generate=False)
    finally:
        store.close()


@app.post("/api/stock/{symbol}/panel")
def make_panel(
    symbol: str,
    model: str | None = Query(None),
    force: bool = Query(False),
):
    """Generate the 3-analyst panel (facts/bull/bear), grounded in technicals."""
    from finance.config import get_settings
    from finance.research import get_or_generate_panel
    from finance.technicals import analyze_stock, technicals_summary_text

    symbol = symbol.upper()
    read_store = _read_store()
    try:
        ctx = _stock_context(read_store, symbol)
        bars = read_store.read_bars([symbol])
        if not bars.empty:
            ctx["technicals_text"] = technicals_summary_text(analyze_stock(bars))
        if not get_settings().research_enabled:
            return get_or_generate_panel(ctx, read_store, generate=False)
    finally:
        read_store.close()
    store = BarStore()
    try:
        return get_or_generate_panel(
            ctx, store, generate=True, now=_now(), force=force, model=model
        )
    finally:
        store.close()


def _cohort_rows(
    view: str, n: int, sector: str | None = None
) -> tuple[list[dict], str, str, str]:
    """Rows + as_of + (label, cache_key) for a cohort.

    view=winners|losers, or sector=<name> for a single-sector cohort."""
    store = _read_store()
    try:
        lb = compute_leaderboard(store, BASE_CONFIG)
    finally:
        store.close()
    if lb.empty:
        return [], "", "", ""
    as_of = lb["as_of"].iloc[0]
    if sector:
        sel = lb[lb["sector"] == sector].sort_values("score", ascending=False).head(n)
        label = f"the top momentum stocks in the {sector} sector"
        key = f"__sector_{sector}"
    else:
        sel = lb[lb["is_winner"]] if view == "winners" else lb[lb["is_loser"]]
        sel = sel.sort_values("score", ascending=(view == "losers")).head(n)
        label = f"the current top momentum {view}"
        key = f"__cohort_{view}"
    cols = ["symbol", "sector", "score", "mom_12_1"]
    rows = json.loads(sel[[c for c in cols if c in sel.columns]].to_json(orient="records"))
    return rows, as_of, label, key


def _cohort_aggregate(rows: list[dict], store: BarStore) -> tuple[dict, str]:
    """Aggregate computed technicals across the cohort's symbols."""
    from finance.technicals import aggregate_cohort, analyze_stock, cohort_summary_text

    syms = [r["symbol"] for r in rows]
    bars = store.read_bars(syms)
    analyses = []
    if not bars.empty:
        for _, g in bars.groupby("symbol"):
            analyses.append(analyze_stock(g))
    agg = aggregate_cohort(analyses)
    return agg, cohort_summary_text(agg)


@app.get("/api/research/cohort")
def get_cohort(view: str = Query("winners"), sector: str | None = Query(None)):
    """Cached cohort/sector briefing + computed technicals (no generation)."""
    from finance.research import get_or_generate_cohort

    store = _read_store()
    try:
        rows, as_of, label, key = _cohort_rows(view, 10, sector)
        agg, _ = _cohort_aggregate(rows, store) if rows else ({}, "")
        res = get_or_generate_cohort(label, key, rows, as_of, store, generate=False)
        return {**res, "aggregate": agg}
    finally:
        store.close()


@app.post("/api/research/cohort")
def make_cohort(
    view: str = Query("winners"),
    sector: str | None = Query(None),
    n: int = Query(10),
    model: str | None = Query(None),
    force: bool = Query(False),
):
    from finance.config import get_settings
    from finance.research import get_or_generate_cohort

    rows, as_of, label, key = _cohort_rows(view, n, sector)
    if not rows:
        return {"mode": "handoff", "report": None, "prompt": "", "research_enabled": False}
    read_store = _read_store()
    try:
        agg, tech_text = _cohort_aggregate(rows, read_store)
        if not get_settings().research_enabled:
            res = get_or_generate_cohort(label, key, rows, as_of, read_store, generate=False)
            return {**res, "aggregate": agg}
    finally:
        read_store.close()
    store = BarStore()
    try:
        res = get_or_generate_cohort(
            label, key, rows, as_of, store, generate=True, now=_now(), force=force,
            model=model, tech_text=tech_text,
        )
        return {**res, "aggregate": agg}
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
