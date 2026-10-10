"""The metrics job (Quix Streams): one-minute OHLCV, VWAP and latency per source+symbol.

- **Event time.** The window a trade belongs to comes from the exchange's timestamp
  (``timestamp_extractor``), not from when Kafka received it.
- **Grace period.** A window stays open 5 s after it ends, so slightly late trades still
  count. Trades later than that are logged and dropped.
- **Repartition.** Input is keyed by symbol (so the detector can compare exchanges); here
  we re-key by source+symbol with ``group_by`` so each feed gets its own windows.
- **State.** Window state lives in RocksDB with a changelog topic in Kafka, so a restarted
  job resumes mid-window instead of losing the minute.
"""

from __future__ import annotations

import logging
from typing import Any

from quixstreams import Application

from feedwatch.config import Settings
from feedwatch.metrics import aggregate

log = logging.getLogger(__name__)

WINDOW_MS = 60_000
GRACE_MS = 5_000


def event_time(value: Any, _headers: Any, timestamp: int, _type: Any) -> int:
    if isinstance(value, dict):
        return int(value.get("event_ms") or value.get("ingest_ms") or timestamp)
    return timestamp


def on_late(
    value: Any,
    key: Any,
    timestamp_ms: int,
    late_by_ms: int,
    start: int,
    end: int,
    store_name: str,
    topic: str,
    partition: int,
    offset: int,
) -> bool:
    log.info("late trade dropped: %s late by %d ms", key, late_by_ms)
    return False  # we logged it ourselves


def build(settings: Settings) -> Application:
    app = Application(
        broker_address=settings.kafka_bootstrap,
        consumer_group="feedwatch-metrics",
        auto_offset_reset="earliest",
        state_dir="state/metrics",
    )
    source = app.topic(
        settings.topic_events, value_deserializer="json", timestamp_extractor=event_time
    )
    sink = app.topic(settings.topic_metrics, value_serializer="json", key_serializer="str")

    sdf = app.dataframe(source)
    sdf = sdf.filter(lambda v: v.get("kind") == "trade" and not v.get("snapshot"))
    sdf = sdf.group_by(lambda v: f"{v['source']}|{v['symbol']}", name="source_symbol")
    sdf = (
        sdf.tumbling_window(duration_ms=WINDOW_MS, grace_ms=GRACE_MS, on_late=on_late)
        .reduce(reducer=aggregate.reduce, initializer=aggregate.init)
        .final()
    )
    sdf = sdf.apply(lambda w: aggregate.finish(w["value"], w["start"]))
    sdf.to_topic(sink, key=lambda row: f"{row['source']}|{row['symbol']}")
    return app


def run(settings: Settings) -> None:
    build(settings).run()
