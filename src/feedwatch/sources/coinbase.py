"""Coinbase Exchange public WebSocket feed (no authentication).

Docs: https://docs.cdp.coinbase.com/exchange/websocket-feed/overview

We use two channels:

- ``matches``    one message per executed trade. ``trade_id`` increases by exactly 1 per
                 product, which is what makes gap detection possible.
- ``heartbeat``  one message per product per second with ``last_trade_id``. If that id is
                 ahead of the last trade we received, we missed trades.

Note on ``side``: Coinbase reports the *maker* order's side. We store the *taker* side
(the side that initiated the trade), which is what Kraken reports, so both sources mean
the same thing.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from feedwatch.models import Connection, Event, Heartbeat, Trade
from feedwatch.sources.base import iso_to_ms, loads

_TAKER = {"buy": "sell", "sell": "buy"}


@dataclass
class Coinbase:
    products: list[str]
    url: str = "wss://ws-feed.exchange.coinbase.com"
    name: str = field(default="coinbase", init=False)

    def subscribe_messages(self) -> list[dict[str, Any]]:
        return [
            {
                "type": "subscribe",
                "product_ids": self.products,
                "channels": ["matches", "heartbeat"],
            }
        ]

    def parse(self, raw: str | bytes, ingest_ms: int) -> list[Event]:
        msg = loads(raw)
        kind = msg.get("type")
        if kind in ("match", "last_match"):
            return [
                Trade(
                    source=self.name,
                    symbol=msg["product_id"],
                    trade_id=int(msg["trade_id"]),
                    price=Decimal(msg["price"]),
                    size=Decimal(msg["size"]),
                    side=_TAKER.get(msg["side"], msg["side"]),
                    event_ms=iso_to_ms(msg["time"]),
                    ingest_ms=ingest_ms,
                    snapshot=kind == "last_match",
                )
            ]
        if kind == "heartbeat":
            return [
                Heartbeat(
                    source=self.name,
                    symbol=msg["product_id"],
                    last_trade_id=int(msg["last_trade_id"]),
                    event_ms=iso_to_ms(msg["time"]),
                    ingest_ms=ingest_ms,
                )
            ]
        if kind == "subscriptions":
            channels = ",".join(c["name"] for c in msg.get("channels", []))
            return [
                Connection(
                    source=self.name, state="subscribed", ingest_ms=ingest_ms, detail=channels
                )
            ]
        if kind == "error":
            return [
                Connection(
                    source=self.name,
                    state="error",
                    ingest_ms=ingest_ms,
                    detail=str(msg.get("message", "")),
                )
            ]
        return []
