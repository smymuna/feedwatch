"""Kafka plumbing shared by every service: topic setup and a small JSON producer."""

from __future__ import annotations

import json
import logging
from typing import Any

from confluent_kafka import KafkaError, Message, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from feedwatch.config import Settings
from feedwatch.models import Event, message_key, to_message

log = logging.getLogger(__name__)

EVENT_PARTITIONS = 3


def topic_specs(s: Settings) -> list[NewTopic]:
    """Topic layout. See docs/architecture.md for why each topic looks like this."""
    day = str(24 * 3600 * 1000)
    return [
        # All exchange data, keyed by symbol so one symbol's trades from every exchange stay
        # in order in one partition. Kept 3 days so services can rebuild state on restart.
        NewTopic(s.topic_events, EVENT_PARTITIONS, 1, config={"retention.ms": str(3 * int(day))}),
        NewTopic(s.topic_dlq, 1, 1, config={"retention.ms": str(7 * int(day))}),
        # Latest version of each incident (key = incident id), kept by compaction.
        NewTopic(s.topic_incidents, 1, 1, config={"cleanup.policy": "compact"}),
        NewTopic(s.topic_health, 1, 1, config={"retention.ms": str(3600 * 1000)}),
        NewTopic(s.topic_metrics, 1, 1, config={"retention.ms": str(30 * int(day))}),
    ]


def ensure_topics(s: Settings) -> list[str]:
    """Create any missing topics; returns the names that were created."""
    admin = AdminClient({"bootstrap.servers": s.kafka_bootstrap})
    existing = set(admin.list_topics(timeout=15).topics)
    wanted = [t for t in topic_specs(s) if t.topic not in existing]
    if not wanted:
        return []
    for name, future in admin.create_topics(wanted).items():
        try:
            future.result()
        except Exception as exc:  # another service created it first
            if "TOPIC_ALREADY_EXISTS" not in str(exc):
                raise
        log.info("created topic %s", name)
    return [t.topic for t in wanted]


class JsonProducer:
    """An idempotent, batched producer. Delivery failures are counted and logged, never
    silently dropped."""

    def __init__(self, s: Settings, client_id: str) -> None:
        self.settings = s
        self.failed = 0
        self.delivered = 0
        self._producer = Producer(
            {
                "bootstrap.servers": s.kafka_bootstrap,
                "client.id": client_id,
                "enable.idempotence": True,  # no duplicates from producer retries
                "acks": "all",
                "linger.ms": 20,
                "compression.type": "zstd",
            }
        )
        self._partitions: dict[str, int] = {}

    def _on_delivery(self, err: KafkaError | None, _msg: Message) -> None:
        if err is None:
            self.delivered += 1
        else:
            self.failed += 1
            log.error("delivery failed: %s", err)

    def partitions(self, topic: str) -> int:
        if topic not in self._partitions:
            meta = self._producer.list_topics(topic, timeout=10).topics[topic]
            self._partitions[topic] = max(1, len(meta.partitions))
        return self._partitions[topic]

    def send(
        self, topic: str, key: str, value: dict[str, Any], partition: int | None = None
    ) -> None:
        kwargs: dict[str, Any] = {
            "key": key.encode(),
            "value": json.dumps(value).encode(),
            "on_delivery": self._on_delivery,
        }
        if partition is not None:
            kwargs["partition"] = partition
        while True:
            try:
                self._producer.produce(topic, **kwargs)
                break
            except BufferError:  # local queue full: let it drain, then retry
                self._producer.poll(0.5)
        self._producer.poll(0)

    def send_event(self, event: Event) -> None:
        """Events about a symbol go to that symbol's partition. Events that aren't about a
        symbol (clock ticks, connection changes, per-connection heartbeats) go to *every*
        partition, so each detector instance sees time pass and sources come and go."""
        topic = self.settings.topic_events
        key = message_key(event)
        value = to_message(event)
        if key.startswith("_"):
            for p in range(self.partitions(topic)):
                self.send(topic, key, value, partition=p)
        else:
            self.send(topic, key, value)

    def flush(self, timeout: float = 10) -> int:
        return int(self._producer.flush(timeout))
