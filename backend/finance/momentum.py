"""Momentum engine — universe screening + signals + cross-sectional ranking.

Produces the leaderboard: every eligible stock scored by a composite of
academically-grounded momentum signals, ranked into deciles (winners/losers).

All signals are computed on the ADJUSTED close (splits/dividends already handled
by the DataSource layer), so returns are correct by construction. The
penny-stock problem is handled the right way: hard universe filters (price,
liquidity, history) + risk-adjustment, not by abandoning percentage returns.

Signals (see config.yaml for weights; set a weight to 0 to disable):
  - risk_adjusted_mom : 12-1 momentum / annualized formation-window volatility
  - high_52w_proximity: close / trailing 252d high  (George & Hwang)
  - path_quality      : frog-in-the-pan information discreteness (steady > spiky)
  - mom_3_6           : ~3-6 month momentum (shorter, confirming signal)
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from finance.config import AppConfig, load_config
from finance.store import BarStore

TRADING_DAYS_YEAR = 252


def _wide(store: BarStore, symbols: list[str] | None = None):
    """Return (close, volume) wide matrices indexed by date, columns = symbols."""
    bars = store.read_bars(symbols)
    if bars.empty:
        return pd.DataFrame(), pd.DataFrame()
    bars["date"] = pd.to_datetime(bars["date"])
    close = bars.pivot(index="date", columns="symbol", values="close").sort_index()
    volume = bars.pivot(index="date", columns="symbol", values="volume").sort_index()
    return close, volume


def _zscore(s: pd.Series, clip: float) -> pd.Series:
    std = s.std(ddof=0)
    if std == 0 or np.isnan(std):
        return pd.Series(0.0, index=s.index)
    z = (s - s.mean()) / std
    return z.clip(-clip, clip)


def compute_signals(
    close: pd.DataFrame, volume: pd.DataFrame, cfg: AppConfig, as_of_pos: int = -1
) -> pd.DataFrame:
    """Compute raw signals + the filter inputs for every symbol at one date."""
    m, u = cfg.momentum, cfg.universe
    t = as_of_pos if as_of_pos >= 0 else len(close) + as_of_pos
    skip, look, short = m.skip_days, m.lookback_days, m.short_lookback_days

    last = close.iloc[t]
    p_skip = close.iloc[t - skip]
    p_look = close.iloc[t - look]
    p_short = close.iloc[t - short]

    mom_12_1 = p_skip / p_look - 1.0
    mom_3_6 = p_skip / p_short - 1.0

    window = close.iloc[t - look : t + 1]
    gh52 = last / window.max()

    # Formation window daily returns: [t-look, t-skip] (the 12-1 span).
    formation = close.iloc[t - look : t - skip + 1]
    rets = formation.pct_change(fill_method=None)
    vol = rets.std() * np.sqrt(TRADING_DAYS_YEAR)
    risk_adjusted_mom = mom_12_1 / vol.replace(0, np.nan)

    pos = (rets > 0).sum()
    neg = (rets < 0).sum()
    total = (pos + neg).replace(0, np.nan)
    pct_pos, pct_neg = pos / total, neg / total
    path_quality = np.sign(mom_12_1) * (pct_neg - pct_pos)  # lower = steadier

    # Liquidity: avg dollar volume over trailing window.
    dv_win = close.iloc[t - u.dollar_volume_window + 1 : t + 1] * volume.iloc[
        t - u.dollar_volume_window + 1 : t + 1
    ]
    avg_dollar_volume = dv_win.mean()

    df = pd.DataFrame(
        {
            "last_close": last,
            "mom_12_1": mom_12_1,
            "mom_3_6": mom_3_6,
            "risk_adjusted_mom": risk_adjusted_mom,
            "high_52w_proximity": gh52,
            "path_quality": path_quality,
            "volatility": vol,
            "avg_dollar_volume": avg_dollar_volume,
            "history_ok": p_look.notna(),
        }
    )
    df.index.name = "symbol"
    return df


def apply_universe_filters(df: pd.DataFrame, cfg: AppConfig) -> pd.DataFrame:
    """Keep only liquid, sufficiently-priced names with enough history."""
    u = cfg.universe
    mask = (
        df["history_ok"]
        & (df["last_close"] >= u.min_price)
        & (df["avg_dollar_volume"] >= u.min_dollar_volume)
        & df["risk_adjusted_mom"].notna()
    )
    # market cap filter only if configured (>0) and the column exists
    if u.min_market_cap > 0 and "market_cap" in df.columns:
        mask &= df["market_cap"] >= u.min_market_cap
    return df[mask].copy()


def score(df: pd.DataFrame, cfg: AppConfig) -> pd.DataFrame:
    """Cross-sectionally z-score active signals and form the composite score."""
    m = cfg.momentum
    weights = m.signals.active()  # renormalized, non-zero only

    z_cols = {}
    composite = pd.Series(0.0, index=df.index)
    for signal, w in weights.items():
        raw = df[signal]
        # path_quality is "lower is better" -> flip so higher z = better
        if signal == "path_quality":
            raw = -raw
        z = _zscore(raw, m.clip_z)
        z_cols[f"z_{signal}"] = z
        composite = composite + w * z

    out = df.copy()
    for col, z in z_cols.items():
        out[col] = z
    out["score"] = composite
    out = out.sort_values("score", ascending=False)
    out["rank"] = np.arange(1, len(out) + 1)
    out["percentile"] = out["score"].rank(pct=True)
    if len(out) >= 10:
        out["decile"] = pd.qcut(out["score"].rank(method="first"), 10, labels=False) + 1
    else:
        out["decile"] = 1
    q = m.winners_quantile
    out["is_winner"] = out["percentile"] >= (1 - q)
    out["is_loser"] = out["percentile"] <= q
    return out


def compute_leaderboard(
    store: BarStore | None = None,
    cfg: AppConfig | None = None,
    as_of_pos: int = -1,
    as_of_date: str | None = None,
) -> pd.DataFrame:
    """End-to-end: read store -> signals -> filters -> ranked leaderboard.

    as_of_date (YYYY-MM-DD) selects a historical snapshot: the leaderboard is
    computed as of the last trading day on/before that date. Returns an empty
    frame if there isn't enough history before that date for the lookback.
    """
    cfg = cfg or load_config()
    own_store = store is None
    store = store or BarStore()
    try:
        close, volume = _wide(store)
        if close.empty:
            return pd.DataFrame()

        # Resolve the as-of position.
        if as_of_date:
            target = pd.Timestamp(as_of_date)
            valid = close.index[close.index <= target]
            if len(valid) == 0:
                return pd.DataFrame()
            t = close.index.get_loc(valid[-1])
        else:
            t = as_of_pos if as_of_pos >= 0 else len(close) + as_of_pos

        # Need a full lookback of history before t, else iloc would wrap.
        if t < cfg.momentum.lookback_days:
            return pd.DataFrame()

        raw = compute_signals(close, volume, cfg, t)
        eligible = apply_universe_filters(raw, cfg)
        if cfg.universe.exclude_etfs:
            from finance.universe import load_etf_symbols

            etfs = load_etf_symbols()
            if etfs:
                eligible = eligible[~eligible.index.isin(etfs)]
        if eligible.empty:
            return eligible
        ranked = score(eligible, cfg)
        ranked["as_of"] = pd.to_datetime(close.index[t]).date().isoformat()
        out = ranked.reset_index()

        # Attach sector/industry/company name if available (best-effort join).
        companies = store.read_companies()
        if not companies.empty:
            out = out.merge(
                companies[["symbol", "name", "industry", "sector"]],
                on="symbol",
                how="left",
            )
        out["sector"] = out.get("sector", pd.Series(index=out.index)).fillna("Unknown")
        return out
    finally:
        if own_store:
            store.close()
