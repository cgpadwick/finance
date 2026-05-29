"""DataSource interface — swappable market-data providers.

The whole point of this abstraction: the rest of the codebase only ever sees a
clean, split+dividend-ADJUSTED daily bar frame, regardless of provider. That is
what makes splits "just work" — returns computed from an adjusted series are
inherently split/dividend correct.

Concrete adapters (Alpaca primary, Stooq/yfinance fallback) land in Phase 1.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd

# Canonical schema every adapter must return.
BAR_COLUMNS = ["open", "high", "low", "close", "volume"]


class DataSource(ABC):
    """A source of daily adjusted OHLCV bars."""

    name: str = "base"

    @abstractmethod
    def get_daily_adjusted(
        self, symbol: str, start: str, end: str | None = None
    ) -> pd.DataFrame:
        """Return daily bars for one symbol over [start, end].

        Returns a DataFrame indexed by a tz-naive ``date`` (datetime.date) with
        columns ``BAR_COLUMNS``. Prices MUST be split- and dividend-adjusted.
        Returns an empty frame (correct columns) if no data is available.
        """

    def get_many(
        self, symbols: list[str], start: str, end: str | None = None
    ) -> dict[str, pd.DataFrame]:
        """Default: fetch symbols one at a time. Adapters with a bulk endpoint
        should override this for speed."""
        out: dict[str, pd.DataFrame] = {}
        for sym in symbols:
            df = self.get_daily_adjusted(sym, start, end)
            if not df.empty:
                out[sym] = df
        return out


def get_data_source(name: str = "alpaca") -> DataSource:
    """Factory. Adapters are imported lazily so missing optional deps for one
    provider don't break the others."""
    name = name.lower()
    if name == "alpaca":
        from finance.datasources.alpaca import AlpacaSource

        return AlpacaSource()
    if name == "stooq":
        from finance.datasources.stooq import StooqSource

        return StooqSource()
    if name == "yfinance":
        from finance.datasources.yfinance_source import YFinanceSource

        return YFinanceSource()
    raise ValueError(f"Unknown data source: {name!r}")
