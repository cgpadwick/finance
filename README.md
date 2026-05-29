# Momentum Screener

A personal momentum-investing stock screener: it pulls **split/dividend-adjusted**
daily prices from Alpaca, ranks a screened universe of stocks by a composite of
academically-grounded momentum signals, and serves the results through a React
dashboard (sortable leaderboard + per-stock candlestick charts).

> Replaces the old yfinance batch pipeline (still in `src/`, kept for reference).

## Why it's built this way

- **Splits "just work."** All prices come from Alpaca with `Adjustment.ALL`
  (split + dividend adjusted), so returns are computed from a clean adjusted
  series. `finance/validate.py` proves it: AAPL's 4:1 split shows a normal
  daily return, not a phantom −75% crash.
- **No penny-stock junk.** The classic mistake is ranking by raw % return, which
  rewards a $0.50→$1.00 noise spike. We fix it the right way: hard universe
  filters (price, dollar-volume, history) **plus** risk-adjusting momentum by
  volatility — not by abandoning percentage returns.
- **Quality momentum.** The composite score blends four signals (weights in
  `backend/config.yaml`, tunable live in the UI):
  | Signal | What it captures |
  |---|---|
  | Risk-adjusted momentum | 12-1 return ÷ volatility (Sharpe-style; demotes junk) |
  | 52-week-high proximity | how close to the trailing-year high (George–Hwang) |
  | Path quality | steady trends vs. spiky ones (frog-in-the-pan) |
  | 3–6 month momentum | shorter, confirming signal |

  Each is z-scored cross-sectionally, combined, and ranked into deciles
  (top decile = winners, bottom = losers).

## Architecture

```
Alpaca (adjusted bars) ─► DuckDB store ─► momentum engine ─► FastAPI ─► React SPA
   datasources/            store.py        momentum.py       api/       frontend/
```

- **backend/** — Python (`uv`-managed). Ingest, storage, momentum engine, API.
- **frontend/** — React + Vite + TS, Tailwind, TanStack Query/Table, TradingView
  Lightweight Charts.

## Setup

1. **Alpaca keys** (free): create paper-trading API keys at app.alpaca.markets,
   then put them in a repo-root `.env` (gitignored — see `.env.example`):
   ```
   ALPACA_API_KEY=...
   ALPACA_SECRET_KEY=...
   ```
2. **Install everything:**
   ```
   make setup            # backend venv + deps, frontend npm install
   ```

## Usage

```
make validate           # prove split adjustment is correct
make ingest             # backfill ~600 liquid symbols into DuckDB (LIMIT=N to change; 0=all)
make test               # backend unit tests

# Dev (two servers, hot reload):
make api                # FastAPI on :8077
make web                # Vite dev server on :5173  (proxies /api -> :8077)
#   open http://localhost:5173

# Production-style (one server serves SPA + API):
make serve              # builds frontend, serves everything on :8077
#   open http://localhost:8077
```

> The API runs on **:8077** (port 8000 was occupied on the dev machine).

## Tuning

Edit `backend/config.yaml` for defaults (universe filters, signal weights,
lookback/skip windows, rebalance), or adjust them live in the dashboard's
"Tune the screen" panel — the API applies overrides per-request.
