"""Symbol universe — fetch and cache the list of tradable tickers.

Migrated from the old ``get_nasdaq_symbols.py``: pulls NASDAQ's official
symbol directory over anonymous FTP. The actual liquidity/price/market-cap
*screening* happens later in the momentum engine; this is just the raw list.
"""

from __future__ import annotations

import ftplib
from io import BytesIO, StringIO
from pathlib import Path

import pandas as pd

from finance.config import DATA_DIR

FTP_HOST = "ftp.nasdaqtrader.com"
FTP_DIR = "Symboldirectory"
EXCHANGE_FILES = {
    "NASDAQ": "nasdaqlisted.txt",
    "OTHER": "otherlisted.txt",  # NYSE, AMEX, etc.
}
SYMBOLS_CSV = DATA_DIR / "nasdaq_symbols.csv"


def _download_listing(filename: str) -> pd.DataFrame:
    """Download one pipe-delimited NASDAQ symbol-directory file."""
    ftp = ftplib.FTP(FTP_HOST)
    ftp.login()  # anonymous
    ftp.cwd(FTP_DIR)
    buffer = BytesIO()
    ftp.retrbinary(f"RETR {filename}", buffer.write)
    ftp.quit()

    data = buffer.getvalue().decode("utf-8")
    df = pd.read_csv(StringIO(data), sep="|")
    # Last row is a "File Creation Time" footer — drop it.
    if len(df) and "File Creation Time" in str(df.iloc[-1, 0]):
        df = df[:-1]
    return df


def fetch_symbols(exchanges: list[str] | None = None, save: bool = True) -> list[str]:
    """Fetch ticker symbols for the given exchanges (default NASDAQ).

    Filters out test issues and (for NASDAQ) common non-common-stock noise.
    Optionally caches the raw listing to data/nasdaq_symbols.csv.
    """
    exchanges = exchanges or ["NASDAQ"]
    frames = []
    for exch in exchanges:
        fname = EXCHANGE_FILES.get(exch.upper())
        if not fname:
            raise ValueError(f"Unknown exchange {exch!r}; known: {list(EXCHANGE_FILES)}")
        frames.append(_download_listing(fname))

    df = pd.concat(frames, ignore_index=True)

    # Symbol column differs across files (Symbol vs ACT Symbol); normalize.
    sym_col = "Symbol" if "Symbol" in df.columns else "ACT Symbol"
    if "Test Issue" in df.columns:
        df = df[df["Test Issue"] != "Y"]

    symbols = df[sym_col].dropna().astype(str).str.strip()
    # Drop obvious non-equity tickers (warrants/units/preferreds carry $ or .)
    symbols = symbols[~symbols.str.contains(r"[\$\.]", regex=True)]
    symbols = sorted(symbols.unique().tolist())

    if save:
        SYMBOLS_CSV.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(SYMBOLS_CSV, index=False)

    return symbols


def load_etf_symbols(path: Path | str = SYMBOLS_CSV) -> set[str]:
    """Symbols flagged as ETFs in the NASDAQ listing (column ETF == 'Y')."""
    path = Path(path)
    if not path.exists():
        return set()
    df = pd.read_csv(path)
    if "ETF" not in df.columns:
        return set()
    sym_col = "Symbol" if "Symbol" in df.columns else "ACT Symbol"
    etfs = df.loc[df["ETF"] == "Y", sym_col].dropna().astype(str).str.strip()
    return set(etfs.tolist())


def load_excluded_symbols(path: Path | str = SYMBOLS_CSV) -> set[str]:
    """Non-stock symbols to drop from the screener: ETFs + NASDAQ test issues."""
    path = Path(path)
    excluded = set(load_etf_symbols(path))
    if path.exists():
        df = pd.read_csv(path)
        sym_col = "Symbol" if "Symbol" in df.columns else "ACT Symbol"
        if "Test Issue" in df.columns:
            tests = df.loc[df["Test Issue"] == "Y", sym_col].dropna().astype(str).str.strip()
            excluded |= set(tests.tolist())
    return excluded


def load_cached_symbols(path: Path | str = SYMBOLS_CSV) -> list[str]:
    """Load symbols from the cached CSV (raises if not yet fetched)."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(
            f"{path} not found — run fetch_symbols() first to download the listing."
        )
    df = pd.read_csv(path)
    sym_col = "Symbol" if "Symbol" in df.columns else "ACT Symbol"
    return sorted(df[sym_col].dropna().astype(str).str.strip().unique().tolist())
