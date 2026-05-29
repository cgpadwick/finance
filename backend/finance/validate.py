"""Split-adjustment validation — the confidence check the old pipeline lacked.

A correctly split-ADJUSTED price series shows NO phantom crash on a split date.
E.g. AAPL's 4-for-1 split on 2020-08-28: raw price ~halved-and-then-some
overnight, but the adjusted series should show a normal daily return. We assert
that the daily return on each known split date is within a sane band.

Run:  python -m finance.validate
"""

from __future__ import annotations

import pandas as pd

from finance.datasources import get_data_source

# (symbol, split_date, ratio) — well-known recent splits.
KNOWN_SPLITS = [
    ("AAPL", "2020-08-31", 4),   # 4:1, effective 2020-08-31 (first trading day)
    ("NVDA", "2024-06-10", 10),  # 10:1
    ("TSLA", "2022-08-25", 3),   # 3:1
    ("AMZN", "2022-06-06", 20),  # 20:1
]

# If splits were NOT handled, the day-of return would be roughly -(1 - 1/ratio),
# e.g. -75% for a 4:1. A correctly adjusted series stays well inside this band.
MAX_ABS_DAILY_RETURN = 0.25


def _daily_return_on(df: pd.DataFrame, date: str) -> float | None:
    d = pd.Timestamp(date).date()
    idx = list(df.index)
    if d not in idx:
        # Use the first trading day on/after the split date.
        later = [x for x in idx if x >= d]
        if not later:
            return None
        d = later[0]
    pos = idx.index(d)
    if pos == 0:
        return None
    prev_close = df["close"].iloc[pos - 1]
    close = df["close"].iloc[pos]
    if prev_close in (0, None):
        return None
    return close / prev_close - 1


def validate_source(source: str = "alpaca", verbose: bool = True) -> bool:
    """Return True iff every known split shows an adjusted (sane) day-of return."""
    src = get_data_source(source)
    all_ok = True
    for symbol, date, ratio in KNOWN_SPLITS:
        start = (pd.Timestamp(date) - pd.Timedelta(days=20)).strftime("%Y-%m-%d")
        end = (pd.Timestamp(date) + pd.Timedelta(days=20)).strftime("%Y-%m-%d")
        df = src.get_daily_adjusted(symbol, start, end)
        ret = _daily_return_on(df, date) if not df.empty else None
        if ret is None:
            all_ok = False
            if verbose:
                print(f"  ?? {symbol} {date}: no data to check (source may lack history)")
            continue
        naive = -(1 - 1 / ratio)  # what we'd see if UNadjusted
        ok = abs(ret) <= MAX_ABS_DAILY_RETURN
        all_ok &= ok
        if verbose:
            mark = "OK " if ok else "FAIL"
            print(
                f"  {mark} {symbol} {ratio}:1 on {date}: adjusted day return "
                f"{ret:+.2%} (unadjusted would be ~{naive:+.0%})"
            )
    return all_ok


def main():
    print("Validating split adjustment (adjusted series should NOT crash on split dates):")
    ok = validate_source("alpaca")
    print("\nRESULT:", "PASS - splits handled correctly ✅" if ok else "FAIL - check adjustment ❌")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
