# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

A finance analysis repo being rebuilt into a **momentum-investing stock screener with a React UI**. The new code lives in `backend/` (Python package `finance`); the original scripts in `src/` are **legacy** and being phased out.

Architecture target: `ingest (Alpaca, adjusted bars) → DuckDB store → momentum engine → FastAPI → React SPA`. Data source is swappable behind a `DataSource` interface; storage is DuckDB (not pickle). All momentum filters and signal weights are tunable via `backend/config.yaml` (and will be UI-editable).

### New backend (`backend/`)

- Managed with `uv`; deps in `backend/pyproject.toml`. Setup: `cd backend && uv venv && uv pip install -e ".[dev]"`. Run things with `.venv/bin/python` or `uv run`.
- `finance/config.py` — `load_config()` reads `backend/config.yaml` (tunable knobs: universe filters, momentum signal weights — set a weight to 0 to disable a signal; `active()` renormalizes). `get_settings()` reads Alpaca keys from the **repo-root `.env`** (gitignored; vars `ALPACA_API_KEY`/`ALPACA_SECRET_KEY`). Never commit `.env` or print key values.
- `finance/datasources/` — `DataSource` ABC + `get_data_source(name)` factory. Adapters MUST return split+dividend-**adjusted** daily OHLCV (this is what makes splits "just work"). Alpaca is primary (use `Adjustment.ALL`); Stooq/yfinance are fallbacks.
- `finance/store.py` — `BarStore` (DuckDB at `data/finance.duckdb`). `upsert_bars` is DELETE-then-INSERT so re-pulling full history after a corporate action is idempotent and correctly re-adjusted.
- `finance/universe.py` — fetches NASDAQ symbol directory over FTP (migrated from `src/get_nasdaq_symbols.py`).

- `finance/momentum.py` — the engine: `compute_leaderboard()` reads the store, computes signals (12-1 mom, risk-adjusted mom, 52w-high proximity, frog-in-pan path quality), applies universe filters, cross-sectional z-scores → composite score → deciles, and left-joins company sector/industry. All driven by `AppConfig`.
- `finance/sectors.py` — `python -m finance.sectors` fetches SIC code/industry from **SEC EDGAR** (free, no key, no daily cap) and maps SIC→broad sector; cached in the DuckDB `companies` table (resumable). `sic_to_sector()` is the pure mapping (specific overrides before broad ranges; e.g. SIC 3559/3827 → Technology for semi-equipment).
- `finance/embedding.py` — `compute_embedding()` builds a 2D "map of the market": feature = each stock's trailing-1yr daily returns, distance = 1−correlation, reduced via **UMAP** (default; t-SNE/PCA optional), merged with the leaderboard for score/sector coloring. Lazy-imports umap (heavy/numba).
- `api/main.py` — FastAPI. `/api/leaderboard` (accepts `min_price`/`min_dollar_volume`/`w_*` weights plus `lookback_days` (momentum window) and `as_of` (historical snapshot date) query overrides, cached per signature), `/api/stock/{symbol}`, `/api/config`, `/api/health`. In prod it also serves the built SPA from `frontend/dist` with an `index.html` catch-all for client-side routes.
- `finance/ingest.py` — `python -m finance.ingest` backfills adjusted bars (batched via Alpaca multi-symbol). `--update` does an incremental ~400-day re-pull (re-adjusts recent corporate actions). `--limit N` bounds the universe (0 = all).
- `finance/validate.py` — `python -m finance.validate` asserts split adjustment is correct against known splits. Run this whenever changing the data source.
- Tests: `cd backend && .venv/bin/python -m pytest`.

### Frontend (`frontend/`)

- React 19 + Vite + TS, Tailwind v4 (`@tailwindcss/vite`), TanStack Query + Table, TradingView **Lightweight Charts v5** (note v5 API: `chart.addSeries(CandlestickSeries, {...})`, not `addCandlestickSeries`).
- Dev: `npm run dev` (Vite on :5173) proxies `/api` → `:8077` (see `vite.config.ts`), so the SPA uses same-origin relative URLs in both dev and prod. Build: `npm run build` → `frontend/dist`.
- Pages: `Leaderboard.tsx` (TanStack Table, winners/losers/all toggle, sector column + filter, lookback/as-of `AnalysisControls`, live `FilterPanel` tuning), `StockDetail.tsx` (candlestick + MA overlays + signal stats), and `MarketMap.tsx` (2D/3D ECharts scatter of the UMAP embedding — 3D via `echarts-gl` scatter3D/grid3D, drag to rotate — color-by score/sector, click→detail; **lazy-loaded** so ECharts/echarts-gl aren't in the main bundle). `/api/embedding?dims=3` returns x/y/z.
- DuckDB is single-writer: don't run `make ingest`/`make sectors` (writers) while `make api` (reader) is up, or the reader hits a lock error. Run writers offline, then serve.

### Commands

Use the root `Makefile`: `make setup`, `make ingest [LIMIT=N]`, `make validate`, `make test`, `make api` (:8077), `make web` (:5173), `make serve` (build + single-server prod on :8077). **The API uses port 8077** — 8000 was occupied on the dev machine; `vite.config.ts` and the Makefile both assume 8077.

### Legacy pipeline (`src/`)

Legacy three-stage pipeline (being replaced; the harsh date filter below is intentionally NOT carried into the new backend):

## Pipeline (run in order, from `src/`)

1. **Fetch symbol list** — `get_nasdaq_symbols.py` pulls `nasdaqlisted.txt` over anonymous FTP from `ftp.nasdaqtrader.com` and writes `nasdaq_symbols.csv` (the committed `data/nasdaq_symbols.csv` was produced this way). The downstream step requires a `Symbol` column.

   ```
   python get_nasdaq_symbols.py
   ```

2. **Download per-symbol data** — `download_ticker_data.py` reads the symbol CSV and writes one JSON file per symbol into `--output` using yfinance (`auto_adjust=True`). Downloads run across a `ThreadPoolExecutor`. Existing JSON files are skipped, so re-runs are incremental/resumable.

   ```
   python download_ticker_data.py --csv ../data/nasdaq_symbols.csv --output ../data/stock_data_json/ --start 2020-01-01 --end 2025-01-03 --subsample 0.1
   ```

   - Use `--subsample <fraction>` (with `--seed`) for quick tests over a random subset of symbols.
   - **All warnings/errors go to `download.log`, not the console** — check that file when downloads look incomplete.

3. **Build combined DataFrame** — `create_dataframe.py` loads every JSON in `--directory`, adds moving-average columns, concatenates, and pickles the result to `--outputpkl`.

   ```
   python create_dataframe.py --directory ../data/stock_data_json/ --outputpkl dataframe.pkl
   ```

4. **Explore** — `plot_stock_data.ipynb` loads the pickle and plots.

## Key behaviors to know

- **Harsh date filter in `create_dataframe.py`.** yfinance often returns partial date ranges. `process_all_stocks` computes the global min and max date across *all* loaded files, then **drops any symbol whose own min/max doesn't exactly match** the global range. This guarantees every retained series spans the identical date window, but can discard a large fraction of symbols — the script prints the kept count/percentage and asserts that at least one survives.
- **Moving averages** default to windows `[5, 10, 30, 90]`, computed on `Close` as `MA_<window>` with `min_periods=1`.
- **Dropped columns** default to `Dividends`, `Stock Splits`, `Capital Gains`.
- **Date handling**: JSON `Date` strings are parsed UTC then reduced to a plain `date` (no time component) before sorting.

## Environment

No `requirements.txt` / pyproject is present. Scripts depend on: `pandas`, `yfinance`, `tqdm` (and stdlib `ftplib`, `concurrent.futures`). Install these into your environment before running.
