"""Stooq adapter — free, no API key. Used as a cross-check / fallback source.

Stooq serves daily CSV directly over HTTP. Close is split/dividend-adjusted by
default. No SLA, so this is a backup and a way to sanity-check the primary
source's split adjustment, not the production backbone.
"""

from __future__ import annotations

from io import StringIO

import pandas as pd
import requests

from finance.datasources.base import BAR_COLUMNS, DataSource

_URL = "https://stooq.com/q/d/l/"


class StooqSource(DataSource):
    name = "stooq"

    def __init__(self, suffix: str = ".us", timeout: int = 30):
        self.suffix = suffix
        self.timeout = timeout

    def get_daily_adjusted(
        self, symbol: str, start: str, end: str | None = None
    ) -> pd.DataFrame:
        params = {"s": f"{symbol.lower()}{self.suffix}", "i": "d"}
        resp = requests.get(_URL, params=params, timeout=self.timeout)
        resp.raise_for_status()
        text = resp.text.strip()
        if not text or text.startswith("<") or "Date" not in text.split("\n")[0]:
            return pd.DataFrame(columns=BAR_COLUMNS)

        df = pd.read_csv(StringIO(text))
        df.columns = [c.lower() for c in df.columns]
        if "date" not in df.columns:
            return pd.DataFrame(columns=BAR_COLUMNS)
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df = df.set_index("date").sort_index()

        if start:
            df = df[df.index >= pd.Timestamp(start).date()]
        if end:
            df = df[df.index <= pd.Timestamp(end).date()]
        for col in BAR_COLUMNS:
            if col not in df.columns:
                df[col] = pd.NA
        return df[BAR_COLUMNS]
