"""DuckDB-backed storage for adjusted daily bars.

Replaces the old pickle + per-symbol-JSON approach. DuckDB gives us a single
queryable file that scales to thousands of symbols x years of daily bars, with
fast columnar reads for the momentum engine and the API.

The ``bars`` table is the source of truth. Prices are always ADJUSTED
(split+dividend) as guaranteed by the DataSource layer.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

from finance.config import DATA_DIR

DEFAULT_DB_PATH = DATA_DIR / "finance.duckdb"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS bars (
    symbol  VARCHAR NOT NULL,
    date    DATE    NOT NULL,
    open    DOUBLE,
    high    DOUBLE,
    low     DOUBLE,
    close   DOUBLE,
    volume  DOUBLE,
    PRIMARY KEY (symbol, date)
);
"""


class BarStore:
    """Thin DuckDB wrapper for reading/writing adjusted daily bars."""

    def __init__(self, db_path: Path | str = DEFAULT_DB_PATH, read_only: bool = False):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        # read_only allows many concurrent API readers without lock contention.
        self.con = duckdb.connect(str(self.db_path), read_only=read_only)
        if not read_only:
            self.con.execute(_SCHEMA)

    # ----- writes -----

    def upsert_bars(self, symbol: str, df: pd.DataFrame) -> int:
        """Insert/replace bars for one symbol. ``df`` is indexed by date with
        columns open/high/low/close/volume. Returns rows written."""
        if df.empty:
            return 0
        frame = df.reset_index()
        frame.columns = [str(c).lower() for c in frame.columns]
        if "index" in frame.columns and "date" not in frame.columns:
            frame = frame.rename(columns={"index": "date"})
        frame["symbol"] = symbol
        frame = frame[["symbol", "date", "open", "high", "low", "close", "volume"]]
        # DELETE-then-INSERT keeps re-pulls idempotent (important: re-pulling
        # full history after a corporate action is the correct adjustment model).
        self.con.execute("DELETE FROM bars WHERE symbol = ?", [symbol])
        self.con.register("incoming", frame)
        self.con.execute("INSERT INTO bars SELECT * FROM incoming")
        self.con.unregister("incoming")
        return len(frame)

    # ----- reads -----

    def read_bars(
        self,
        symbols: list[str] | None = None,
        start: str | None = None,
        end: str | None = None,
    ) -> pd.DataFrame:
        """Return a long DataFrame (symbol, date, OHLCV) sorted by symbol, date."""
        clauses, params = [], []
        if symbols:
            placeholders = ",".join("?" for _ in symbols)
            clauses.append(f"symbol IN ({placeholders})")
            params.extend(symbols)
        if start:
            clauses.append("date >= ?")
            params.append(start)
        if end:
            clauses.append("date <= ?")
            params.append(end)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        sql = f"SELECT * FROM bars {where} ORDER BY symbol, date"
        return self.con.execute(sql, params).df()

    def close_series(self, symbol: str) -> pd.Series:
        """Adjusted close indexed by date for one symbol."""
        df = self.con.execute(
            "SELECT date, close FROM bars WHERE symbol = ? ORDER BY date", [symbol]
        ).df()
        return df.set_index("date")["close"]

    def list_symbols(self) -> list[str]:
        rows = self.con.execute(
            "SELECT DISTINCT symbol FROM bars ORDER BY symbol"
        ).fetchall()
        return [r[0] for r in rows]

    def coverage(self) -> pd.DataFrame:
        """Per-symbol row count and date range — handy for sanity checks."""
        return self.con.execute(
            """
            SELECT symbol, COUNT(*) AS n_bars,
                   MIN(date) AS first_date, MAX(date) AS last_date
            FROM bars GROUP BY symbol ORDER BY symbol
            """
        ).df()

    def close(self) -> None:
        self.con.close()

    def __enter__(self) -> "BarStore":
        return self

    def __exit__(self, *exc) -> None:
        self.close()
