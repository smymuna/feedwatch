# 0002: Event time for prices, processing time for the feed

**Status:** accepted · 10 Oct 2026

## Context

Each trade has two times: when the exchange executed it (event time) and when we received
it (processing time). They differ by network latency, reconnects and batching, and events
can arrive out of order.

## Decision

- Rules about **prices** (divergence, depeg, bad prints) and the one-minute metrics use
  **event time**. A trade belongs to the minute it happened in, whenever it arrives.
- Rules about **the feed** (silence, latency) use **processing time**. "We've heard nothing
  for 10 s" is about arrival.
- The ingest service emits a `tick` every second so processing time advances even when no
  exchange sends anything. Otherwise a silent feed could never be detected, because the
  detector only runs when a message arrives.
- The metrics job keeps each window open 5 s past its end (grace period) for late trades.

## Consequences

- Replays are deterministic: the detector never reads the wall clock, only event and
  arrival timestamps from the data, so the same input always gives the same incidents.
- Trade history (no arrival times, no heartbeats) can only exercise the price rules; the
  feed rules are switched off for it (`feed_rules=False`).
- Latency depends on clock sync between us and the exchange (see Limitations).
