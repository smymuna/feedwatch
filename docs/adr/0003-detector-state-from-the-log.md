# 0003: Rebuild the detector's state from the log instead of checkpointing it

**Status:** accepted · 10 Oct 2026

## Context

The detector keeps state in memory: last trade id per feed, the last 50 prices, one-minute
windows, open incidents. If it restarts with empty state, it would miss a gap across the
restart, re-open incidents that were already open, and have no price history.

Options:
1. Checkpoint state to a store (RocksDB with a changelog, as Quix Streams and Flink do).
2. Rebuild state by re-reading recent events from Kafka.

## Decision

Option 2. The longest window the detector needs is a few minutes, and `market.events`
keeps 3 days. On start the detector rewinds each partition 15 minutes before its committed
offset and replays that span **silently**: incidents are recomputed but not re-published.
Then it publishes normally.

## Consequences

- No extra state store or changelog topic; the detector stays plain Python and stays
  deterministic.
- Restart takes as long as replaying 15 minutes of events (well under a second at current
  volumes; the detector processes about 170,000 events per second).
- If state ever needed hours of history, this would get slow and option 1 would be
  better. The metrics job does use option 1 (Quix Streams state + changelog), because its
  windows are owned by the framework.
