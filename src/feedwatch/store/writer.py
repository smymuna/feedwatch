"""The store service: incidents, metrics and health from Kafka into PostgreSQL/TimescaleDB.

Every write is an upsert on a natural key (incident id; symbol+source+minute; source), and
offsets are committed only after the database transaction commits. A crash between the
two simply replays a few messages, which overwrite the same rows: at-least-once delivery
with idempotent writes gives exactly-once *results*.
"""

from __future__ import annotations

import json
import logging
import signal
import threading
from datetime import UTC, datetime
from importlib import resources
from typing import Any

import psycopg
from confluent_kafka import Consumer
from psycopg.types.json import Jsonb

from feedwatch.config import Settings

log = logging.getLogger(__name__)


def ts(ms: int | None) -> datetime | None:
    return None if ms is None else datetime.fromtimestamp(ms / 1000, UTC)


def apply_schema(conn: psycopg.Connection[Any]) -> None:
    sql = resources.files("feedwatch.store").joinpath("schema.sql").read_text()
    conn.execute(sql.encode())
    conn.commit()


UPSERT_INCIDENT = """
INSERT INTO incidents (id, type, severity, status, source, symbol, opened_at, updated_at,
                       resolved_at, summary, details)
VALUES (%(id)s, %(type)s, %(severity)s, %(status)s, %(source)s, %(symbol)s, %(opened_at)s,
        %(updated_at)s, %(resolved_at)s, %(summary)s, %(details)s)
ON CONFLICT (id) DO UPDATE SET
    severity = EXCLUDED.severity, status = EXCLUDED.status, updated_at = EXCLUDED.updated_at,
    resolved_at = EXCLUDED.resolved_at, summary = EXCLUDED.summary, details = EXCLUDED.details
WHERE incidents.updated_at <= EXCLUDED.updated_at
"""

UPSERT_METRIC = """
INSERT INTO metrics_1m (bucket, source, symbol, trades, volume, vwap, open, high, low, close,
                        latency_p50_ms, latency_p95_ms)
VALUES (%(bucket)s, %(source)s, %(symbol)s, %(trades)s, %(volume)s, %(vwap)s, %(open)s,
        %(high)s, %(low)s, %(close)s, %(latency_p50_ms)s, %(latency_p95_ms)s)
ON CONFLICT (symbol, source, bucket) DO UPDATE SET
    trades = EXCLUDED.trades, volume = EXCLUDED.volume, vwap = EXCLUDED.vwap,
    open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low, close = EXCLUDED.close,
    latency_p50_ms = EXCLUDED.latency_p50_ms, latency_p95_ms = EXCLUDED.latency_p95_ms
"""

UPSERT_SOURCE = """
INSERT INTO feed_health (source, connected, last_message_at, last_trade_at, messages_per_s,
                         trades_per_s, latency_p95_ms, gaps_total, missing_trades_total, updated_at)
VALUES (%(source)s, %(connected)s, %(last_message_at)s, %(last_trade_at)s, %(messages_per_s)s,
        %(trades_per_s)s, %(latency_p95_ms)s, %(gaps_total)s, %(missing_trades_total)s,
        %(updated_at)s)
ON CONFLICT (source) DO UPDATE SET
    connected = EXCLUDED.connected, last_message_at = EXCLUDED.last_message_at,
    last_trade_at = EXCLUDED.last_trade_at, messages_per_s = EXCLUDED.messages_per_s,
    trades_per_s = EXCLUDED.trades_per_s, latency_p95_ms = EXCLUDED.latency_p95_ms,
    gaps_total = EXCLUDED.gaps_total, missing_trades_total = EXCLUDED.missing_trades_total,
    updated_at = EXCLUDED.updated_at
"""

UPSERT_SYMBOL = """
INSERT INTO symbol_health (symbol, source, last_price, vwap_60s, trades_60s, peg_dev_pct,
                           divergence_bps, last_trade_at, updated_at)
VALUES (%(symbol)s, %(source)s, %(last_price)s, %(vwap_60s)s, %(trades_60s)s, %(peg_dev_pct)s,
        %(divergence_bps)s, %(last_trade_at)s, %(updated_at)s)
ON CONFLICT (symbol, source) DO UPDATE SET
    last_price = EXCLUDED.last_price, vwap_60s = EXCLUDED.vwap_60s,
    trades_60s = EXCLUDED.trades_60s, peg_dev_pct = EXCLUDED.peg_dev_pct,
    divergence_bps = EXCLUDED.divergence_bps, last_trade_at = EXCLUDED.last_trade_at,
    updated_at = EXCLUDED.updated_at
"""


def incident_row(m: dict[str, Any]) -> dict[str, Any]:
    return {
        **m,
        "opened_at": ts(m["opened_ms"]),
        "updated_at": ts(m["updated_ms"]),
        "resolved_at": ts(m.get("resolved_ms")),
        "details": Jsonb(m.get("details", {})),
    }


def health_rows(h: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    at = ts(h["at_ms"])
    sources = [
        {
            **s,
            "last_message_at": ts(s["last_message_ms"]),
            "last_trade_at": ts(s["last_trade_ms"]),
            "updated_at": at,
        }
        for s in h["sources"]
    ]
    symbols = [
        {**s, "last_trade_at": ts(s["last_trade_ms"]), "updated_at": at} for s in h["symbols"]
    ]
    return sources, symbols


class StoreService:
    def __init__(self, settings: Settings) -> None:
        self.s = settings
        self.conn = psycopg.connect(settings.database_url)
        apply_schema(self.conn)
        self.consumer = Consumer(
            {
                "bootstrap.servers": settings.kafka_bootstrap,
                "group.id": "feedwatch-store",
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
            }
        )
        self._running = True

    def write(self, batch: list[tuple[str, dict[str, Any]]]) -> None:
        incidents = [incident_row(v) for t, v in batch if t == self.s.topic_incidents]
        metrics = [v for t, v in batch if t == self.s.topic_metrics]
        healths = [v for t, v in batch if t == self.s.topic_health]
        with self.conn.cursor() as cur:
            if incidents:
                cur.executemany(UPSERT_INCIDENT, incidents)
            if metrics:
                cur.executemany(UPSERT_METRIC, metrics)
            if healths:  # only the newest snapshot matters
                sources, symbols = health_rows(healths[-1])
                if sources:
                    cur.executemany(UPSERT_SOURCE, sources)
                if symbols:
                    cur.executemany(UPSERT_SYMBOL, symbols)
        self.conn.commit()

    def run(self) -> None:
        if threading.current_thread() is threading.main_thread():  # signals: main thread only
            signal.signal(signal.SIGTERM, lambda *_: self.stop())
        self.consumer.subscribe([self.s.topic_incidents, self.s.topic_metrics, self.s.topic_health])
        try:
            while self._running:
                msgs = self.consumer.consume(num_messages=1000, timeout=1.0)
                batch = [
                    (m.topic(), json.loads(m.value())) for m in msgs if not m.error() and m.value()
                ]
                if batch:
                    self.write(batch)
                    self.consumer.commit(asynchronous=False)
        finally:
            self.consumer.close()
            self.conn.close()

    def stop(self) -> None:
        self._running = False


def run(settings: Settings) -> None:
    StoreService(settings).run()
