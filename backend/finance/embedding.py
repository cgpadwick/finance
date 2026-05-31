"""2D embedding of stocks for the "map of the market" scatter view.

Analogy to computer vision: instead of image -> CNN embedding -> 2D, we do
stock -> return-series feature vector -> 2D. The distance between two stocks is
based on how correlated their daily returns are, so co-moving stocks land near
each other -- and those clusters tend to recover sectors organically.

Default reducer is UMAP with metric='correlation' (1 - corr distance). t-SNE and
PCA are available as alternatives. Coordinates are merged with the leaderboard so
each point carries its momentum score, sector, and winner/loser flag for coloring.

Caveat (surfaced in the UI): in UMAP/t-SNE, cluster *membership* is meaningful
but absolute positions, axis directions, and inter-cluster distances are not.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from finance.config import AppConfig, load_config
from finance.momentum import _wide, compute_leaderboard
from finance.store import BarStore

RANDOM_STATE = 42


def _reduce(X: np.ndarray, method: str, n_components: int = 2) -> np.ndarray:
    """Reduce an (n_symbols, n_days) return matrix to n_components dims (2 or 3)."""
    method = method.lower()
    n = X.shape[0]
    if method == "umap":
        import umap  # lazy: heavy import (numba)

        reducer = umap.UMAP(
            n_components=n_components,
            n_neighbors=min(15, max(2, n - 1)),
            min_dist=0.1,
            metric="correlation",
            random_state=RANDOM_STATE,
        )
        return reducer.fit_transform(X)
    if method == "tsne":
        from sklearn.manifold import TSNE

        reducer = TSNE(
            n_components=n_components,
            metric="correlation",
            init="pca",
            perplexity=min(30, max(5, n // 4)),
            random_state=RANDOM_STATE,
        )
        return reducer.fit_transform(X)
    if method == "pca":
        from sklearn.decomposition import PCA

        return PCA(n_components=n_components, random_state=RANDOM_STATE).fit_transform(X)
    raise ValueError(f"Unknown embedding method: {method!r}")


def compute_embedding(
    store: BarStore | None = None,
    cfg: AppConfig | None = None,
    method: str = "umap",
    as_of_date: str | None = None,
    dims: int = 2,
    sector: str | None = None,
    lb: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Embed the eligible (leaderboard) universe into 2D or 3D, with score/sector.

    sector restricts to one sector's stocks — far fewer points AND far fewer
    bars to read, so much faster. Pass `lb` (a precomputed leaderboard) to skip
    the expensive full-universe recompute — the API hands in the cached one.

    Returns columns: symbol, x, y, (z if dims==3), score, sector, industry,
    name, is_winner, is_loser, last_close. Empty frame if not enough data.
    """
    cfg = cfg or load_config()
    own_store = store is None
    store = store or BarStore()
    try:
        if lb is None:
            lb = compute_leaderboard(store, cfg, as_of_date=as_of_date)
        if sector:
            lb = lb[lb["sector"] == sector]
        if lb.empty or len(lb) < 5:
            return pd.DataFrame()
        symbols = lb["symbol"].tolist()

        close, _ = _wide(store, symbols)
        # Trailing one-year window of daily returns = the feature vector.
        rets = close.pct_change(fill_method=None).iloc[-cfg.momentum.lookback_days :]
        # Keep symbols with mostly-complete history; fill the rest with 0.
        thresh = int(0.8 * len(rets))
        rets = rets.dropna(axis=1, thresh=thresh).fillna(0.0)
        if rets.shape[1] < 5:
            return pd.DataFrame()

        dims = 3 if dims == 3 else 2
        X = rets.T.values  # rows = symbols, cols = days
        coords = _reduce(X, method, n_components=dims)

        emb = pd.DataFrame(
            {"symbol": rets.columns, "x": coords[:, 0], "y": coords[:, 1]}
        )
        if dims == 3:
            emb["z"] = coords[:, 2]
        keep = [
            "symbol", "score", "sector", "industry", "name",
            "is_winner", "is_loser", "last_close",
        ]
        cols = [c for c in keep if c in lb.columns]
        out = emb.merge(lb[cols], on="symbol", how="left")
        out["sector"] = out.get("sector", pd.Series(index=out.index)).fillna("Unknown")
        # JSON-safe
        return out.replace([np.inf, -np.inf], np.nan)
    finally:
        if own_store:
            store.close()
