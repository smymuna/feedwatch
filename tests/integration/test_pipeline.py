"""End to end through real Kafka and PostgreSQL: a recorded session with the demo fault
drill goes into Kafka, the detector service turns it into incidents, and the store service
writes them to the database.

Needs Kafka on FEEDWATCH_KAFKA_BOOTSTRAP and PostgreSQL on FEEDWATCH_DATABASE_URL
(``docker compose up -d kafka db``). Run with ``pytest -m integration``. Each run uses its
own topic names, so it never touches the real pipeline's data.
"""

from __future__ import annotations

import json
import threading
import time
import uuid
from pathlib import Path

import psycopg
import pytest
from confluent_kafka import Consumer

from feedwatch.config import Settings
from feedwatch.detect.service import DetectorService
from feedwatch.kafka import JsonProducer, ensure_topics
from feedwatch.replay import replay, scenarios
from feedwatch.store.writer import StoreService

pytestmark = pytest.mark.integration

ROOT = Path(__file__).parents[2]


@pytest.fixture
def settings() -> Settings:
    run = uuid.uuid4().hex[:8]
    return Settings(
        topic_events=f"it.{run}.events",
        topic_dlq=f"it.{run}.dlq",
        topic_incidents=f"it.{run}.incidents",
        topic_health=f"it.{run}.health",
        topic_metrics=f"it.{run}.metrics",
    )


def test_drill_flows_from_kafka_to_the_database(settings: Settings) -> None:
    assert len(ensure_topics(settings)) == 5

    detector = DetectorService(settings)
    thread = threading.Thread(target=detector.run, daemon=True)
    thread.start()
    time.sleep(6)  # let the consumer join its group before we produce

    events = replay.with_ticks(
        scenarios.inject(
            replay.read_recording(ROOT / "samples/session"),
            scenarios.load(ROOT / "scenarios/drill.json").faults,
        )
    )
    producer = JsonProducer(settings, "integration-test")
    sent = 0
    for event in replay.rebase(events, int(time.time() * 1000)):
        producer.send_event(event)
        sent += 1
    assert producer.flush(30) == 0 and producer.failed == 0

    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "group.id": f"it-reader-{uuid.uuid4().hex[:6]}",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([settings.topic_incidents])
    latest: dict[str, dict[str, object]] = {}
    deadline = time.time() + 90
    wanted = {"trade_gap", "price_divergence", "depeg", "feed_silent", "outlier_print"}
    while time.time() < deadline and not wanted <= {str(v["type"]) for v in latest.values()}:
        for msg in consumer.consume(500, timeout=1.0):
            if not msg.error() and msg.value():
                value = json.loads(msg.value())
                latest[value["id"]] = value
    consumer.close()
    detector.stop()
    thread.join(timeout=15)

    types = {str(v["type"]) for v in latest.values()}
    assert wanted <= types, f"missing {wanted - types} after {sent} events"
    gap = next(v for v in latest.values() if v["type"] == "trade_gap" and v["source"] == "coinbase")
    assert gap["details"]["missing_trades"] == 51  # type: ignore[index]

    # The store writes the same incidents into PostgreSQL.
    store = StoreService(settings)
    store.write([(settings.topic_incidents, v) for v in latest.values()])
    with psycopg.connect(settings.database_url) as conn:
        rows = conn.execute(
            "SELECT type, count(*) FROM incidents WHERE id = ANY(%s) GROUP BY type",
            (list(latest),),
        ).fetchall()
        conn.execute("DELETE FROM incidents WHERE id = ANY(%s)", (list(latest),))
    store.conn.close()
    store.consumer.close()
    assert {r[0] for r in rows} == types
