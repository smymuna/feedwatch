"""Live market view for the coin pages: the latest trades and the candles still forming.

Finished one-minute candles come from the database (written by the metrics job about a
minute after each minute ends). For the *current* minute, and for trade-by-trade updates,
the API follows ``market.events`` itself: a background thread consumes the topic from a
few minutes back and keeps a small in-memory buffer per symbol. Each API instance uses its
own consumer group, so every instance sees every trade.

``LiveMarket`` holds the buffer and is pure (no Kafka), so it's tested directly.
"""

from __future__ import annotations

import json
import logging
import threading
import time
import uuid
from collections import deque
from decimal import Decimal
from typing import Any

from confluent_kafka import Consumer, KafkaException, TopicPartition

from feedwatch.config import Settings

log = logging.getLogger(__name__)

MINUTE_MS = 60_000


class LiveMarket:
    def __init__(self, max_trades: int = 300, keep_minutes: int = 3) -> None:
        self.max_trades = max_trades
        self.keep_minutes = keep_minutes
        self._lock = threading.Lock()
        self._seq = 0
        self._trades: dict[str, deque[dict[str, Any]]] = {}
        self._candles: dict[tuple[str, str], dict[int, dict[str, Any]]] = {}
        self.last_message_ms: int | None = None

    def add(self, msg: dict[str, Any]) -> None:
        """Take one message from ``market.events``. Only live trades matter here."""
        if msg.get("kind") != "trade" or msg.get("snapshot"):
            return
        symbol, source = msg["symbol"], msg["source"]
        price, size, t = Decimal(msg["price"]), Decimal(msg["size"]), int(msg["event_ms"])
        with self._lock:
            self._seq += 1
            self.last_message_ms = int(msg["ingest_ms"])
            trades = self._trades.setdefault(symbol, deque(maxlen=self.max_trades))
            trades.append(
                {
                    "seq": self._seq,
                    "source": source,
                    "symbol": symbol,
                    "price": msg["price"],
                    "size": msg["size"],
                    "side": msg["side"],
                    "time_ms": t,
                    "trade_id": msg.get("trade_id"),
                }
            )
            minute = t - t % MINUTE_MS
            candles = self._candles.setdefault((symbol, source), {})
            c = candles.get(minute)
            if c is None:
                candles[minute] = {
                    "minute_ms": minute,
                    "source": source,
                    "open": price,
                    "high": price,
                    "low": price,
                    "close": price,
                    "volume": size,
                    "trades": 1,
                    "first_ms": t,
                    "last_ms": t,
                }
            else:
                c["high"], c["low"] = max(c["high"], price), min(c["low"], price)
                c["volume"] += size
                c["trades"] += 1
                if t < c["first_ms"]:
                    c["first_ms"], c["open"] = t, price
                if t >= c["last_ms"]:
                    c["last_ms"], c["close"] = t, price
            for old in [m for m in candles if m < minute - self.keep_minutes * MINUTE_MS]:
                del candles[old]

    def trades(self, symbol: str, since_seq: int = 0, limit: int = 100) -> list[dict[str, Any]]:
        with self._lock:
            items = [t for t in self._trades.get(symbol, ()) if t["seq"] > since_seq]
        return items[-limit:]

    def candles(self, symbol: str) -> list[dict[str, Any]]:
        """Candles of the last few minutes (including the one still forming), per source."""
        with self._lock:
            out = [
                {k: (format(v, "f") if isinstance(v, Decimal) else v) for k, v in c.items()}
                for (sym, _src), by_minute in self._candles.items()
                if sym == symbol
                for c in by_minute.values()
            ]
        return sorted(out, key=lambda c: (c["minute_ms"], c["source"]))

    def last_prices(self, symbol: str) -> dict[str, dict[str, Any]]:
        out: dict[str, dict[str, Any]] = {}
        with self._lock:
            for t in self._trades.get(symbol, ()):
                out[t["source"]] = {"price": t["price"], "time_ms": t["time_ms"]}
        return out

    @property
    def seq(self) -> int:
        return self._seq


class LiveFeeder(threading.Thread):
    """Follows ``market.events`` into a ``LiveMarket``. Starts a few minutes back so the
    forming candles are complete straight away. Retries quietly if Kafka is down."""

    def __init__(self, settings: Settings, market: LiveMarket, rewind_ms: int = 3 * MINUTE_MS):
        super().__init__(daemon=True, name="live-feeder")
        self.s = settings
        self.market = market
        self.rewind_ms = rewind_ms
        self.connected = False
        self._stop = threading.Event()

    def _assign(self, consumer: Consumer) -> None:
        meta = consumer.list_topics(self.s.topic_events, timeout=10).topics[self.s.topic_events]
        since = int(time.time() * 1000) - self.rewind_ms
        wanted = [TopicPartition(self.s.topic_events, p, since) for p in meta.partitions]
        consumer.assign(consumer.offsets_for_times(wanted, timeout=10))

    def run(self) -> None:
        while not self._stop.is_set():
            consumer = Consumer(
                {
                    "bootstrap.servers": self.s.kafka_bootstrap,
                    "group.id": f"feedwatch-api-{uuid.uuid4().hex[:8]}",
                    "enable.auto.commit": False,
                    "auto.offset.reset": "latest",
                }
            )
            try:
                self._assign(consumer)
                self.connected = True
                while not self._stop.is_set():
                    for msg in consumer.consume(500, timeout=0.5):
                        if not msg.error() and msg.value():
                            self.market.add(json.loads(msg.value()))
            except (KafkaException, KeyError, ValueError) as exc:
                self.connected = False
                log.warning("live feed unavailable (%s), retrying in 5 s", exc)
                self._stop.wait(5)
            finally:
                consumer.close()

    def stop(self) -> None:
        self._stop.set()
