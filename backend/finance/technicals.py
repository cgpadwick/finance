"""Deterministic technical analysis — computed from price data, never guessed.

These functions turn a stock's adjusted OHLCV into a battery of standard
indicators plus a bullish/bearish scorecard. The whole point: the numbers are
COMPUTED here so the LLM report explains real figures instead of hallucinating
them. Everything is point-in-time as of the last bar.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

TRADING_DAYS_YEAR = 252


def _safe(x) -> float | None:
    try:
        f = float(x)
        return None if (np.isnan(f) or np.isinf(f)) else round(f, 4)
    except (TypeError, ValueError):
        return None


def rsi(close: pd.Series, period: int = 14) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0).ewm(alpha=1 / period, adjust=False).mean()
    loss = (-delta.clip(upper=0)).ewm(alpha=1 / period, adjust=False).mean()
    # loss==0 (all gains) -> rs=inf -> RSI=100; handled naturally, no NaN replace.
    with np.errstate(divide="ignore", invalid="ignore"):
        rs = gain / loss
    return 100 - 100 / (1 + rs)


def macd(close: pd.Series, fast=12, slow=26, signal=9):
    ema_fast = close.ewm(span=fast, adjust=False).mean()
    ema_slow = close.ewm(span=slow, adjust=False).mean()
    line = ema_fast - ema_slow
    sig = line.ewm(span=signal, adjust=False).mean()
    return line, sig, line - sig


def atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    high, low, close = df["high"], df["low"], df["close"]
    prev = close.shift(1)
    tr = pd.concat([(high - low), (high - prev).abs(), (low - prev).abs()], axis=1).max(axis=1)
    return tr.ewm(alpha=1 / period, adjust=False).mean()


def _ret(close: pd.Series, days: int) -> float | None:
    if len(close) <= days:
        return None
    return _safe(close.iloc[-1] / close.iloc[-1 - days] - 1)


def _signal(value: float | None, bull: bool, bear: bool) -> str:
    if value is None:
        return "neutral"
    if bull:
        return "bullish"
    if bear:
        return "bearish"
    return "neutral"


def analyze_stock(df: pd.DataFrame) -> dict:
    """Full technical readout from an OHLCV frame (indexed/sorted by date)."""
    df = df.sort_values("date") if "date" in df.columns else df.sort_index()
    close = df["close"].astype(float).reset_index(drop=True)
    vol = df["volume"].astype(float).reset_index(drop=True)
    n = len(close)
    if n < 30:
        return {"insufficient_data": True, "bars": n}

    price = close.iloc[-1]
    ma50 = close.rolling(50, min_periods=1).mean().iloc[-1]
    ma200 = close.rolling(200, min_periods=1).mean().iloc[-1] if n >= 60 else None
    rsi_v = rsi(close).iloc[-1]
    macd_line, macd_sig, macd_hist = macd(close)
    ml, ms, mh = macd_line.iloc[-1], macd_sig.iloc[-1], macd_hist.iloc[-1]

    ma20 = close.rolling(20).mean()
    sd20 = close.rolling(20).std()
    bb_upper = (ma20 + 2 * sd20).iloc[-1]
    bb_lower = (ma20 - 2 * sd20).iloc[-1]
    bb_pctb = (
        (price - bb_lower) / (bb_upper - bb_lower)
        if bb_upper and not np.isnan(bb_upper) and bb_upper != bb_lower
        else None
    )

    atr_v = atr(df.reset_index(drop=True)).iloc[-1] if n >= 15 else None
    daily_ret = close.pct_change(fill_method=None)
    ann_vol = daily_ret.tail(TRADING_DAYS_YEAR).std() * np.sqrt(TRADING_DAYS_YEAR)

    cummax = close.cummax()
    dd = close / cummax - 1
    max_dd = dd.min()
    cur_dd = dd.iloc[-1]

    win = close.tail(TRADING_DAYS_YEAR)
    hi52, lo52 = win.max(), win.min()
    vol_avg50 = vol.rolling(50, min_periods=1).mean().iloc[-1]
    vol_ratio = vol.iloc[-1] / vol_avg50 if vol_avg50 else None
    up_day_pct = (daily_ret.tail(TRADING_DAYS_YEAR) > 0).mean()

    golden = bool(ma200 and ma50 > ma200)

    # ---- scorecard ----
    card = []

    def add(label, signal, detail):
        card.append({"label": label, "signal": signal, "detail": detail})

    add("Price vs 50-day MA", _signal(ma50, price > ma50, price < ma50),
        f"{'above' if price > ma50 else 'below'} (${_safe(ma50)})")
    if ma200:
        add("Price vs 200-day MA", _signal(ma200, price > ma200, price < ma200),
            f"{'above' if price > ma200 else 'below'} (${_safe(ma200)})")
        add("50/200 MA (trend)", "bullish" if golden else "bearish",
            "golden cross" if golden else "death cross")
    add("RSI(14)", _signal(rsi_v, 50 <= rsi_v < 70, rsi_v < 45) if not np.isnan(rsi_v) else "neutral",
        f"{_safe(rsi_v)}{' — overbought' if rsi_v >= 70 else ' — oversold' if rsi_v < 30 else ''}")
    add("MACD", _signal(mh, ml > ms, ml < ms), "above signal" if ml > ms else "below signal")
    if bb_pctb is not None:
        add("Bollinger position",
            "bearish" if bb_pctb > 1 else "bullish" if bb_pctb > 0.5 else "bearish" if bb_pctb < 0 else "neutral",
            f"{round(bb_pctb * 100)}% of band")
    add("Drawdown from high", _signal(cur_dd, cur_dd > -0.1, cur_dd < -0.25),
        f"{_safe(cur_dd * 100)}% off peak")
    if vol_ratio is not None:
        add("Volume vs 50d avg", "bullish" if vol_ratio > 1.2 else "neutral",
            f"{round(vol_ratio, 2)}x")

    bull = sum(1 for c in card if c["signal"] == "bullish")
    bear = sum(1 for c in card if c["signal"] == "bearish")
    neutral = sum(1 for c in card if c["signal"] == "neutral")
    verdict = "Bullish" if bull > bear + 1 else "Bearish" if bear > bull + 1 else "Mixed"

    return {
        "bars": n,
        "price": _safe(price),
        "trend": {
            "ma50": _safe(ma50),
            "ma200": _safe(ma200),
            "above_ma50": bool(price > ma50),
            "above_ma200": bool(ma200 and price > ma200),
            "golden_cross": golden,
        },
        "momentum": {
            "rsi": _safe(rsi_v),
            "macd": _safe(ml),
            "macd_signal": _safe(ms),
            "macd_hist": _safe(mh),
        },
        "volatility": {
            "ann_vol": _safe(ann_vol),
            "atr_pct": _safe(atr_v / price) if atr_v else None,
            "bollinger_pctb": _safe(bb_pctb),
            "max_drawdown": _safe(max_dd),
            "current_drawdown": _safe(cur_dd),
        },
        "returns": {
            "r_1m": _ret(close, 21),
            "r_3m": _ret(close, 63),
            "r_6m": _ret(close, 126),
            "r_12m": _ret(close, 252),
            "up_day_pct": _safe(up_day_pct),
        },
        "range": {
            "high_52w": _safe(hi52),
            "low_52w": _safe(lo52),
            "pct_from_high": _safe(price / hi52 - 1) if hi52 else None,
            "pct_from_low": _safe(price / lo52 - 1) if lo52 else None,
        },
        "volume_ratio": _safe(vol_ratio),
        "scorecard": card,
        "verdict": {"label": verdict, "bullish": bull, "bearish": bear, "neutral": neutral},
    }


def aggregate_cohort(analyses: list[dict]) -> dict:
    """Roll up per-stock technicals across a cohort into shared stats."""
    valid = [a for a in analyses if not a.get("insufficient_data")]
    if not valid:
        return {}

    def mean(vals):
        vals = [v for v in vals if v is not None]
        return _safe(sum(vals) / len(vals)) if vals else None

    n = len(valid)
    above200 = sum(1 for a in valid if a["trend"]["above_ma200"])
    golden = sum(1 for a in valid if a["trend"]["golden_cross"])
    overbought = sum(1 for a in valid if (a["momentum"]["rsi"] or 0) >= 70)
    bullish = sum(1 for a in valid if a["verdict"]["label"] == "Bullish")
    return {
        "n": n,
        "pct_above_200ma": _safe(above200 / n),
        "pct_golden_cross": _safe(golden / n),
        "n_overbought": overbought,
        "pct_bullish_verdict": _safe(bullish / n),
        "avg_rsi": mean([a["momentum"]["rsi"] for a in valid]),
        "avg_6m_return": mean([a["returns"]["r_6m"] for a in valid]),
        "avg_12m_return": mean([a["returns"]["r_12m"] for a in valid]),
        "avg_max_drawdown": mean([a["volatility"]["max_drawdown"] for a in valid]),
        "avg_ann_vol": mean([a["volatility"]["ann_vol"] for a in valid]),
    }


def cohort_summary_text(agg: dict) -> str:
    if not agg:
        return ""

    def p(x):
        return "n/a" if x is None else f"{x * 100:.0f}%"

    return (
        f"COMPUTED COHORT TECHNICALS across {agg['n']} names (use these real figures):\n"
        f"- {p(agg['pct_above_200ma'])} trade above their 200-day MA; "
        f"{p(agg['pct_golden_cross'])} in a golden-cross regime\n"
        f"- {p(agg['pct_bullish_verdict'])} have a bullish technical verdict; "
        f"{agg['n_overbought']} are overbought (RSI>=70)\n"
        f"- Avg RSI {agg['avg_rsi']}; avg 6m return {p(agg['avg_6m_return'])}, "
        f"12m {p(agg['avg_12m_return'])}; avg max drawdown {p(agg['avg_max_drawdown'])}; "
        f"avg annualized vol {p(agg['avg_ann_vol'])}"
    )


def technicals_summary_text(t: dict) -> str:
    """Compact, LLM-ready text of the COMPUTED metrics (so the model cites real
    numbers instead of inventing them)."""
    if t.get("insufficient_data"):
        return "Insufficient price history for technical analysis."
    tr, m, v, r, rg = t["trend"], t["momentum"], t["volatility"], t["returns"], t["range"]

    def p(x):
        return "n/a" if x is None else f"{x * 100:.0f}%"

    return (
        f"COMPUTED TECHNICALS (use these exact figures, do not invent numbers):\n"
        f"- Verdict: {t['verdict']['label']} ({t['verdict']['bullish']} bullish / "
        f"{t['verdict']['bearish']} bearish signals)\n"
        f"- Price ${t['price']}; MA50 ${tr['ma50']}, MA200 ${tr['ma200']}; "
        f"{'above' if tr['above_ma200'] else 'below'} 200-day; "
        f"{'golden' if tr['golden_cross'] else 'death'} cross\n"
        f"- RSI(14) {m['rsi']}; MACD {'above' if (m['macd'] or 0) > (m['macd_signal'] or 0) else 'below'} signal\n"
        f"- Annualized vol {p(v['ann_vol'])}; max drawdown {p(v['max_drawdown'])}; "
        f"now {p(v['current_drawdown'])} off peak\n"
        f"- Returns: 1m {p(r['r_1m'])}, 3m {p(r['r_3m'])}, 6m {p(r['r_6m'])}, 12m {p(r['r_12m'])}; "
        f"up-days {p(r['up_day_pct'])}\n"
        f"- {p(rg['pct_from_high'])} from 52-week high"
    )
