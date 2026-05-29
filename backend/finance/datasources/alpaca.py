"""Alpaca market-data adapter — primary source.

Returns split+dividend-ADJUSTED daily bars via Alpaca's ``Adjustment.ALL``.
This is what makes momentum returns split/dividend-correct without any manual
adjustment code on our side.

Alpaca's bars endpoint accepts a *list* of symbols and returns them all in one
(paginated) request, so ``get_many`` batches for efficiency on the free tier.
"""

from __future__ import annotations

import pandas as pd

from finance.config import get_settings
from finance.datasources.base import BAR_COLUMNS, DataSource

# Alpaca recommends <= a few hundred symbols per multi-symbol bars request.
_BATCH_SIZE = 100


class AlpacaSource(DataSource):
    name = "alpaca"

    def __init__(self, adjustment: str = "all"):
        # Imported here so the module loads even if alpaca-py is absent.
        from alpaca.data.enums import Adjustment
        from alpaca.data.historical import StockHistoricalDataClient

        settings = get_settings()
        if not settings.alpaca_configured:
            raise RuntimeError(
                "Alpaca keys missing. Set ALPACA_API_KEY/ALPACA_SECRET_KEY in the "
                "repo-root .env."
            )
        self._client = StockHistoricalDataClient(
            settings.alpaca_api_key, settings.alpaca_secret_key
        )
        self._adjustment = Adjustment(adjustment)

    def _request(self, symbols: list[str], start: str, end: str | None):
        from alpaca.data.requests import StockBarsRequest
        from alpaca.data.timeframe import TimeFrame

        req = StockBarsRequest(
            symbol_or_symbols=symbols,
            timeframe=TimeFrame.Day,
            start=pd.Timestamp(start).to_pydatetime(),
            end=pd.Timestamp(end).to_pydatetime() if end else None,
            adjustment=self._adjustment,
        )
        return self._client.get_stock_bars(req)

    @staticmethod
    def _to_frame(rows) -> pd.DataFrame:
        """Map Alpaca bar rows (for one symbol) to our canonical schema."""
        if not rows:
            return pd.DataFrame(columns=BAR_COLUMNS)
        recs = [
            {
                "date": b.timestamp.date(),
                "open": b.open,
                "high": b.high,
                "low": b.low,
                "close": b.close,
                "volume": b.volume,
            }
            for b in rows
        ]
        df = pd.DataFrame(recs).set_index("date").sort_index()
        return df[BAR_COLUMNS]

    def get_daily_adjusted(
        self, symbol: str, start: str, end: str | None = None
    ) -> pd.DataFrame:
        bars = self._request([symbol], start, end)
        return self._to_frame(bars.data.get(symbol, []))

    def get_many(
        self, symbols: list[str], start: str, end: str | None = None
    ) -> dict[str, pd.DataFrame]:
        out: dict[str, pd.DataFrame] = {}
        for i in range(0, len(symbols), _BATCH_SIZE):
            batch = symbols[i : i + _BATCH_SIZE]
            bars = self._request(batch, start, end)
            for sym in batch:
                frame = self._to_frame(bars.data.get(sym, []))
                if not frame.empty:
                    out[sym] = frame
        return out
