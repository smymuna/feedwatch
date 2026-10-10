"""The detector service: Kafka in, incidents and health out.

Delivery guarantees
    At-least-once. Offsets are committed only after the incidents and health produced from
    those events are acknowledged by Kafka. Incidents carry a stable ``id`` and the
    incidents topic is compacted on it, so a re-delivered update is harmless.

State on restart
    The detector's state is short-lived (the longest window is a few minutes), so instead
    of checkpointing it we rebuild it from the log: on start each partition is rewound to
    ``WARMUP`` before its committed offset and those events are replayed *silently*
    (incidents are recomputed but not re-published). Then normal processing resumes.
    docs/adr/0003-detector-state-from-the-log.md explains the trade-off.
"""

from __future__ import annotations

import json
import logging
import signal
import threading
import time
from typing import Any

from confluent_kafka import OFFSET_INVALID, Consumer, TopicPartition

from feedwatch.config import Settings
from feedwatch.detect.detector import Detector
from feedwatch.kafka import JsonProducer
from feedwatch.models import from_message

log = logging.getLogger(__name__)

WARMUP_MS = 15 * 60 * 1000
HEALTH_EVERY_MS = 1000


class DetectorService:
    def __init__(self, settings: Settings) -> None:
        self.s = settings
        self.detector = Detector(["coinbase", "kraken"], settings.thresholds)
        self.producer = JsonProducer(settings, "feedwatch-detector")
        self.consumer = Consumer(
            {
                "bootstrap.servers": settings.kafka_bootstrap,
                "group.id": "feedwatch-detector",
                "enable.auto.commit": False,
                "auto.offset.reset": "latest",
            }
        )
        self._silent_until: dict[int, int] = {}  # partition -> first offset to publish from
        self._last_health = 0
        self._running = True
        self.bad_messages = 0

    def _on_assign(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        """Rewind each partition by WARMUP_MS so the detector rebuilds its state."""
        committed = consumer.committed(partitions, timeout=10)
        since = int(time.time() * 1000) - WARMUP_MS
        rewound = consumer.offsets_for_times(
            [TopicPartition(p.topic, p.partition, since) for p in partitions], timeout=10
        )
        for tp, back in zip(committed, rewound, strict=True):
            if tp.offset != OFFSET_INVALID and back.offset not in (OFFSET_INVALID, -1):
                self._silent_until[tp.partition] = tp.offset
                tp.offset = min(back.offset, tp.offset)
        consumer.assign(committed)
        log.info(
            "assigned %s (warm-up until %s)", [p.partition for p in committed], self._silent_until
        )

    def _handle(self, value: bytes, partition: int, offset: int) -> None:
        try:
            event = from_message(json.loads(value))
        except (ValueError, json.JSONDecodeError) as exc:
            self.bad_messages += 1
            self.producer.send(
                self.s.topic_dlq,
                "detector",
                {"error": str(exc), "payload": value.decode(errors="replace")},
            )
            return
        changes = self.detector.process(event)
        warming = offset < self._silent_until.get(partition, -1)
        if not warming:
            for inc in changes:
                self.producer.send(self.s.topic_incidents, inc.id, inc.to_message())
                log.info(
                    "%s %s: %s",
                    inc.severity.upper(),
                    "OPEN" if inc.is_open else "RESOLVED",
                    inc.summary,
                )

    def _publish_health(self) -> None:
        now = self.detector.now
        if now - self._last_health < HEALTH_EVERY_MS:
            return
        self._last_health = now
        health: dict[str, Any] = self.detector.health()
        health["open_incidents"] = len(self.detector.open)
        self.producer.send(self.s.topic_health, "health", health)

    def run(self) -> None:
        if threading.current_thread() is threading.main_thread():  # signals: main thread only
            signal.signal(signal.SIGTERM, lambda *_: self.stop())
        self.consumer.subscribe([self.s.topic_events], on_assign=self._on_assign)
        try:
            while self._running:
                msgs = self.consumer.consume(num_messages=500, timeout=0.5)
                for msg in msgs:
                    if msg.error():
                        log.warning("consumer error: %s", msg.error())
                        continue
                    self._handle(msg.value(), msg.partition(), msg.offset())
                self._publish_health()
                if msgs:
                    self.producer.flush()  # outputs acknowledged ...
                    self.consumer.commit(asynchronous=False)  # ... before offsets move on
        finally:
            self.producer.flush()
            self.consumer.close()

    def stop(self) -> None:
        self._running = False


def run(settings: Settings) -> None:
    DetectorService(settings).run()
