# 0005: Quix Streams for windows, plain Python for detection

**Status:** accepted · 10 Oct 2026

## Context

Candidates for stream processing: Apache Flink (PyFlink), Spark Structured Streaming,
Kafka Streams (Java), Quix Streams (Python, Kafka-native), or a hand-written consumer.

The work has two shapes:
1. **Keyed windowed aggregation**: one-minute bars per exchange and coin, with event time,
   grace periods, state that survives restarts, and repartitioning. This is exactly what
   stream processors are for.
2. **Detection rules**: they need timers (silence), comparisons *across* keys (Coinbase vs
   Kraken for one coin), and custom incident lifecycles. Frameworks make these awkward
   (cross-key joins, processing-time timers).

## Decision

- Quix Streams for the metrics job: `group_by` to re-key, `tumbling_window` with
  `grace_ms`, `reduce`, state in RocksDB with a Kafka changelog. It's pure Python, light
  enough for a laptop and CI, and the concepts map one-to-one to Flink and Spark (windows,
  watermarks/grace, keyed state, changelogs).
- A plain consumer for the detector, with the rules in a framework-free `Detector` class.
  Keying `market.events` by symbol puts both exchanges' data for a coin in one partition,
  so the cross-exchange comparison needs no join.

## Consequences

- The detection logic is unit-tested without Kafka, and the same code runs live, in
  replays and in backtests.
- Two processing styles to understand instead of one; documented here and in
  `docs/architecture.md`.
- Spark or Flink would be a reasonable choice at much higher volume. The `Detector` class
  could move behind either unchanged.
