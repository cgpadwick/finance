"""yfinance adapter — legacy fallback only.

Kept for parity with the original pipeline and as an emergency cross-check.
yfinance is an unofficial Yahoo scraper (fragile); prefer Alpaca. Requires the
optional ``yfinance`` package (not in core deps).
"""

from __future__ import annotations

import pandas as pd

from finance.datasources.base import BAR_COLUMNS, DataSource


class YFinanceSource(DataSource):
    name = "yfinance"

    def __init__(self):
        try:
            import yfinance  # noqa: F401
        except ImportError as e:
            raise RuntimeError(
                "yfinance not installed. `uv pip install yfinance` to use this fallback."
            ) from e

    def get_daily_adjusted(
        self, symbol: str, start: str, end: str | None = None
    ) -> pd.DataFrame:
        import yfinance as yf

        df = yf.Ticker(symbol).history(
            start=start, end=end, auto_adjust=True
        )  # auto_adjust => split+dividend adjusted
        if df.empty:
            return pd.DataFrame(columns=BAR_COLUMNS)
        df = df.rename(columns=str.lower)
        df.index = pd.to_datetime(df.index).date
        df.index.name = "date"
        return df[BAR_COLUMNS]
