"""Settings, read from environment variables prefixed ``FEEDWATCH_`` (see .env.example)."""

from __future__ import annotations

from functools import lru_cache

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Thresholds(BaseModel):
    """Detection thresholds. Each is documented in docs/detection-rules.md."""

    silent_after_s: float = 10.0  # no message of any kind from a source for this long
    divergence_bps_major: float = 50.0  # BTC/ETH/SOL: 0.5 % between exchanges
    divergence_bps_stable: float = 30.0  # stablecoins: 0.3 % between exchanges
    divergence_hold_s: float = 10.0  # must persist this long before it's an incident
    price_fresh_s: float = 20.0  # compare exchanges only if both traded this recently
    depeg_warn_pct: float = 0.5  # stablecoin 0.5 % away from $1
    depeg_critical_pct: float = 2.0
    depeg_resolve_pct: float = 0.3  # back within 0.3 % ...
    depeg_resolve_hold_s: float = 60.0  # ... for a full minute
    depeg_window_s: float = 60.0  # price = volume-weighted average over this window
    depeg_min_trades: int = 3  # need at least this many trades in the window
    outlier_window: int = 50  # compare a trade with the median of the last N trades
    outlier_min_bps: float = 50.0  # and flag it if it's this far away ...
    outlier_mad_k: float = 10.0  # ... and more than k median absolute deviations
    latency_p95_ms: float = 3000.0  # exchange-to-us delay, 95th percentile over 60 s
    latency_window_s: float = 60.0


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="FEEDWATCH_", env_nested_delimiter="__", env_file=".env", extra="ignore"
    )

    kafka_bootstrap: str = "localhost:29092"  # the port docker compose exposes
    topic_events: str = "market.events"
    topic_dlq: str = "market.events.dlq"
    topic_incidents: str = "feed.incidents"
    topic_health: str = "feed.health"
    topic_metrics: str = "market.metrics.1m"

    database_url: str = "postgresql://feedwatch:feedwatch@localhost:55432/feedwatch"

    coinbase_url: str = "wss://ws-feed.exchange.coinbase.com"
    coinbase_products: list[str] = Field(
        default=["BTC-USD", "ETH-USD", "SOL-USD", "USDT-USD", "USDS-USD"]
    )
    kraken_url: str = "wss://ws.kraken.com/v2"
    kraken_symbols: list[str] = Field(
        default=[
            "BTC/USD",
            "ETH/USD",
            "SOL/USD",
            "USDT/USD",
            "USDC/USD",
            "DAI/USD",
            "PYUSD/USD",
            "USDS/USD",
        ]
    )

    raw_dir: str = "data/raw"  # where the ingest service records raw messages (optional)
    record_raw: bool = False

    thresholds: Thresholds = Field(default_factory=Thresholds)


@lru_cache
def get_settings() -> Settings:
    return Settings()
