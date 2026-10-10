"""The events that flow through the pipeline, and the incidents it raises.

Everything on the `market.events` topic is one of four kinds:

- ``Trade``       a normalised trade from an exchange
- ``Heartbeat``   an exchange's own "still alive" message (Coinbase includes the last trade id)
- ``Connection``  our ingest service connected, subscribed or lost a connection
- ``Tick``        a once-a-second clock from the ingest service, so time-based rules
                  (a feed going silent) fire even when no exchange is sending anything

Prices and sizes are ``Decimal`` in memory and strings on the wire, so a price like
0.99897 never turns into 0.9989699999999999.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from decimal import Decimal
from enum import StrEnum
from typing import Any

STABLECOINS = frozenset({"USDT", "USDC", "DAI", "PYUSD", "USDS"})


def base_asset(symbol: str) -> str:
    """'USDT-USD' -> 'USDT'."""
    return symbol.split("-", 1)[0]


def is_stablecoin(symbol: str) -> bool:
    return base_asset(symbol) in STABLECOINS and symbol.endswith("-USD")


class Kind(StrEnum):
    TRADE = "trade"
    HEARTBEAT = "heartbeat"
    CONNECTION = "connection"
    TICK = "tick"


@dataclass(frozen=True, slots=True)
class Trade:
    source: str
    symbol: str  # canonical, e.g. "BTC-USD"
    trade_id: int | None
    price: Decimal
    size: Decimal
    side: str  # taker side: "buy" or "sell"
    event_ms: int  # when the exchange says the trade happened
    ingest_ms: int  # when we received it
    snapshot: bool = False  # sent on subscribe as "the last trade", not as it happened
    kind: Kind = field(default=Kind.TRADE, init=False)


@dataclass(frozen=True, slots=True)
class Heartbeat:
    source: str
    symbol: str | None  # Coinbase sends one per product; Kraken one per connection
    last_trade_id: int | None
    event_ms: int | None
    ingest_ms: int
    kind: Kind = field(default=Kind.HEARTBEAT, init=False)


@dataclass(frozen=True, slots=True)
class Connection:
    source: str
    state: str  # "connected" | "subscribed" | "disconnected"
    ingest_ms: int
    detail: str = ""
    kind: Kind = field(default=Kind.CONNECTION, init=False)


@dataclass(frozen=True, slots=True)
class Tick:
    ingest_ms: int
    kind: Kind = field(default=Kind.TICK, init=False)


Event = Trade | Heartbeat | Connection | Tick


def to_message(event: Event) -> dict[str, Any]:
    """Serialise an event for Kafka (JSON)."""
    out: dict[str, Any] = {}
    for key, value in asdict(event).items():
        out[key] = str(value) if isinstance(value, Decimal) else value
    out["kind"] = event.kind.value
    return out


def from_message(msg: dict[str, Any]) -> Event:
    """Parse a Kafka message back into an event. Raises ``ValueError`` on bad input."""
    try:
        kind = Kind(msg["kind"])
        if kind is Kind.TRADE:
            return Trade(
                source=msg["source"],
                symbol=msg["symbol"],
                trade_id=None if msg.get("trade_id") is None else int(msg["trade_id"]),
                price=Decimal(msg["price"]),
                size=Decimal(msg["size"]),
                side=msg["side"],
                event_ms=int(msg["event_ms"]),
                ingest_ms=int(msg["ingest_ms"]),
                snapshot=bool(msg.get("snapshot", False)),
            )
        if kind is Kind.HEARTBEAT:
            return Heartbeat(
                source=msg["source"],
                symbol=msg.get("symbol"),
                last_trade_id=None
                if msg.get("last_trade_id") is None
                else int(msg["last_trade_id"]),
                event_ms=None if msg.get("event_ms") is None else int(msg["event_ms"]),
                ingest_ms=int(msg["ingest_ms"]),
            )
        if kind is Kind.CONNECTION:
            return Connection(
                source=msg["source"],
                state=msg["state"],
                ingest_ms=int(msg["ingest_ms"]),
                detail=msg.get("detail", ""),
            )
        return Tick(ingest_ms=int(msg["ingest_ms"]))
    except (KeyError, TypeError, ArithmeticError) as exc:
        raise ValueError(f"not a valid event: {exc!r}") from exc


def message_key(event: Event) -> str:
    """Kafka key. Trades and per-symbol heartbeats are keyed by symbol so every exchange's
    data for one symbol lands in the same partition, in order."""
    symbol = getattr(event, "symbol", None)
    return symbol or f"_{getattr(event, 'source', 'clock')}"


class IncidentType(StrEnum):
    FEED_SILENT = "feed_silent"
    TRADE_GAP = "trade_gap"
    PRICE_DIVERGENCE = "price_divergence"
    DEPEG = "depeg"
    OUTLIER_PRINT = "outlier_print"
    HIGH_LATENCY = "high_latency"


class Severity(StrEnum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


@dataclass(slots=True)
class Incident:
    """Something wrong with the data. Incidents open, may change severity, and resolve.

    Every state change is published, so consumers always see the latest version of an
    incident by its ``id`` (the topic is compacted on ``id``)."""

    id: str
    type: IncidentType
    severity: Severity
    source: str | None
    symbol: str | None
    opened_ms: int
    updated_ms: int
    summary: str
    details: dict[str, Any] = field(default_factory=dict)
    resolved_ms: int | None = None

    @property
    def is_open(self) -> bool:
        return self.resolved_ms is None

    def to_message(self) -> dict[str, Any]:
        data = asdict(self)
        data["type"] = self.type.value
        data["severity"] = self.severity.value
        data["status"] = "open" if self.is_open else "resolved"
        return data
