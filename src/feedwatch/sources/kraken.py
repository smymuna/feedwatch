"""Kraken WebSocket API v2, public ``trade`` channel (no authentication).

Docs: https://docs.kraken.com/api/docs/websocket-v2/trade

Kraken symbols look like ``BTC/USD``; we map them to the canonical ``BTC-USD``. Kraken
sends prices as JSON numbers, which we parse as exact decimals (see ``loads``). Its
heartbeat is per connection and carries no timestamp or trade id, so it only tells us the
connection is alive.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from feedwatch.models import Connection, Event, Heartbeat, Trade
from feedwatch.sources.base import iso_to_ms, loads


def canonical(symbol: str) -> str:
    """'BTC/USD' -> 'BTC-USD'."""
    return symbol.replace("/", "-")


@dataclass
class Kraken:
    symbols: list[str]
    url: str = "wss://ws.kraken.com/v2"
    name: str = field(default="kraken", init=False)

    def subscribe_messages(self) -> list[dict[str, Any]]:
        return [
            {
                "method": "subscribe",
                "params": {"channel": "trade", "symbol": self.symbols, "snapshot": False},
            }
        ]

    def parse(self, raw: str | bytes, ingest_ms: int) -> list[Event]:
        msg = loads(raw)
        channel = msg.get("channel")
        if channel == "trade" and msg.get("type") in ("update", "snapshot"):
            return [
                Trade(
                    source=self.name,
                    symbol=canonical(t["symbol"]),
                    trade_id=int(t["trade_id"]),
                    price=Decimal(t["price"]),
                    size=Decimal(t["qty"]),
                    side=t["side"],
                    event_ms=iso_to_ms(t["timestamp"]),
                    ingest_ms=ingest_ms,
                    snapshot=msg.get("type") == "snapshot",
                )
                for t in msg.get("data", [])
            ]
        if channel == "heartbeat":
            return [
                Heartbeat(
                    source=self.name,
                    symbol=None,
                    last_trade_id=None,
                    event_ms=None,
                    ingest_ms=ingest_ms,
                )
            ]
        if channel == "status":
            data = (msg.get("data") or [{}])[0]
            return [
                Connection(
                    source=self.name,
                    state="connected",
                    ingest_ms=ingest_ms,
                    detail=f"system={data.get('system', '?')}",
                )
            ]
        if msg.get("method") == "subscribe":
            result = msg.get("result") or {}
            ok = bool(msg.get("success"))
            return [
                Connection(
                    source=self.name,
                    state="subscribed" if ok else "error",
                    ingest_ms=ingest_ms,
                    detail=str(result.get("symbol") or msg.get("error", "")),
                )
            ]
        return []
