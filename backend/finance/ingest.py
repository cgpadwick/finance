"""Backfill / update adjusted daily bars into the DuckDB store.

Usage:
    python -m finance.ingest --source alpaca --limit 500
    python -m finance.ingest --symbols AAPL MSFT NVDA
    python -m finance.ingest --update            # incremental top-up

Symbols come from the cached NASDAQ listing (fetched if missing). For a working
demo we default to a bounded, liquid subset (--limit) rather than the full
~4000-symbol universe, which is slow on the free tier; pass --limit 0 for all.
"""

from __future__ import annotations

import argparse

import pandas as pd
from tqdm import tqdm

from finance.config import load_config
from finance.datasources import get_data_source
from finance.store import BarStore
from finance.universe import fetch_symbols, load_cached_symbols

# A liquid, well-known default set so the screener is meaningful out of the box
# even before a full universe backfill. Used when --limit can't be met from cache.
SEED_SYMBOLS = [
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "AVGO", "COST",
    "NFLX", "AMD", "PEP", "ADBE", "CSCO", "INTC", "QCOM", "TXN", "AMAT", "MU",
    "INTU", "BKNG", "ISRG", "AMGN", "GILD", "REGN", "VRTX", "PANW", "CRWD",
    "SNPS", "CDNS", "MRVL", "ORLY", "FTNT", "ADSK", "MELI", "ABNB", "WDAY",
    "DDOG", "TEAM", "ZS", "DXCM", "ON", "LRCX", "KLAC", "ASML", "PYPL", "SBUX",
]


def get_symbol_list(args, cfg) -> list[str]:
    if args.symbols:
        return [s.upper() for s in args.symbols]
    try:
        symbols = load_cached_symbols()
    except FileNotFoundError:
        print("No cached symbol list; fetching NASDAQ listing...")
        symbols = fetch_symbols(cfg.ingest.exchanges)
    if not symbols:
        symbols = SEED_SYMBOLS
    if args.limit and args.limit > 0:
        # Keep the liquid seed names first, then fill from the listing.
        seed = [s for s in SEED_SYMBOLS if s in set(symbols)]
        rest = [s for s in symbols if s not in set(seed)]
        symbols = (seed + rest)[: args.limit]
    return symbols


def run_ingest(
    symbols: list[str], source: str, start: str, batch_log: bool = True
) -> dict[str, int]:
    src = get_data_source(source)
    store = BarStore()
    written: dict[str, int] = {}
    try:
        # Use the source's batched get_many where available (Alpaca).
        if hasattr(src, "get_many") and source == "alpaca":
            # Chunk so progress is visible and memory stays bounded.
            chunk = 100
            for i in tqdm(range(0, len(symbols), chunk), desc="Ingesting (batched)"):
                part = symbols[i : i + chunk]
                frames = src.get_many(part, start)
                for sym, df in frames.items():
                    written[sym] = store.upsert_bars(sym, df)
        else:
            for sym in tqdm(symbols, desc=f"Ingesting via {source}"):
                df = src.get_daily_adjusted(sym, start)
                if not df.empty:
                    written[sym] = store.upsert_bars(sym, df)
        cov = store.coverage()
        print(
            f"\nIngest complete: {len(written)} symbols with data, "
            f"{int(cov['n_bars'].sum()) if len(cov) else 0} total bars."
        )
        if len(cov):
            print(f"Date range: {cov['first_date'].min()} -> {cov['last_date'].max()}")
    finally:
        store.close()
    return written


def main():
    cfg = load_config()
    parser = argparse.ArgumentParser(description="Ingest adjusted daily bars into DuckDB.")
    parser.add_argument("--source", default="alpaca", help="DataSource name.")
    parser.add_argument("--symbols", nargs="*", help="Explicit symbols (overrides listing).")
    parser.add_argument(
        "--limit",
        type=int,
        default=500,
        help="Max symbols from the listing (0 = all). Default 500.",
    )
    parser.add_argument(
        "--start", default=cfg.ingest.start_date, help="Backfill start date YYYY-MM-DD."
    )
    parser.add_argument(
        "--update",
        action="store_true",
        help="Incremental top-up of symbols already in the store.",
    )
    args = parser.parse_args()

    if args.update:
        store = BarStore()
        symbols = store.list_symbols()
        store.close()
        if not symbols:
            print("Store empty; nothing to update. Run a backfill first.")
            return
        # Re-pull last ~1y so recent corporate actions re-adjust correctly.
        start = (pd.Timestamp.today() - pd.Timedelta(days=400)).strftime("%Y-%m-%d")
        run_ingest(symbols, args.source, start)
        return

    symbols = get_symbol_list(args, cfg)
    print(f"Ingesting {len(symbols)} symbols from {args.start} via {args.source}...")
    run_ingest(symbols, args.source, args.start)


if __name__ == "__main__":
    main()
