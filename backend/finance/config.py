"""Configuration: secrets from the repo-root .env, tunable knobs from config.yaml.

Secrets (Alpaca keys) and tunables are deliberately separate:
  - Settings  -> environment / .env  (never committed)
  - AppConfig -> config.yaml          (committed, safe to share, UI-editable)
"""

from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

# Repo root is two levels up from this file: backend/finance/config.py -> repo/
REPO_ROOT = Path(__file__).resolve().parents[2]
BACKEND_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG_PATH = BACKEND_ROOT / "config.yaml"
DATA_DIR = REPO_ROOT / "data"


class Settings(BaseSettings):
    """Secrets loaded from the repo-root .env (gitignored)."""

    alpaca_api_key: str = ""
    alpaca_secret_key: str = ""
    anthropic_api_key: str = ""
    research_model: str = "claude-sonnet-4-6"

    model_config = SettingsConfigDict(
        env_file=str(REPO_ROOT / ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    @property
    def alpaca_configured(self) -> bool:
        return bool(self.alpaca_api_key and self.alpaca_secret_key)

    @property
    def research_enabled(self) -> bool:
        return bool(self.anthropic_api_key)


# ----- tunable knobs (config.yaml) -----


class IngestConfig(BaseModel):
    start_date: str = "2018-01-01"
    exchanges: list[str] = Field(default_factory=lambda: ["NASDAQ"])


class UniverseConfig(BaseModel):
    min_price: float = 5.0
    min_market_cap: float = 0.0
    min_dollar_volume: float = 1_000_000.0
    dollar_volume_window: int = 20
    min_history_days: int = 252
    exclude_etfs: bool = True  # drop ETFs/funds — momentum screener is for stocks


class SignalWeights(BaseModel):
    risk_adjusted_mom: float = 0.45
    high_52w_proximity: float = 0.25
    path_quality: float = 0.20
    mom_3_6: float = 0.10

    def active(self) -> dict[str, float]:
        """Non-zero weights, renormalized to sum to 1.0."""
        weights = {k: v for k, v in self.model_dump().items() if v > 0}
        total = sum(weights.values())
        if total == 0:
            raise ValueError("At least one signal weight must be > 0.")
        return {k: v / total for k, v in weights.items()}


class MomentumConfig(BaseModel):
    skip_days: int = 21
    lookback_days: int = 252
    short_lookback_days: int = 126
    winners_quantile: float = 0.10
    clip_z: float = 3.0
    signals: SignalWeights = Field(default_factory=SignalWeights)


class AppConfig(BaseModel):
    ingest: IngestConfig = Field(default_factory=IngestConfig)
    universe: UniverseConfig = Field(default_factory=UniverseConfig)
    momentum: MomentumConfig = Field(default_factory=MomentumConfig)


def load_config(path: Path | str = DEFAULT_CONFIG_PATH) -> AppConfig:
    """Load config.yaml into a validated AppConfig (defaults if file absent)."""
    path = Path(path)
    if not path.exists():
        return AppConfig()
    with open(path) as f:
        raw = yaml.safe_load(f) or {}
    return AppConfig.model_validate(raw)


def get_settings() -> Settings:
    return Settings()
