# 0004: At-least-once delivery with idempotent writes

**Status:** accepted · 10 Oct 2026

## Context

Kafka gives at-most-once, at-least-once or (with transactions) exactly-once. Exactly-once
across Kafka *and* PostgreSQL needs a two-phase pattern that adds complexity everywhere.

## Decision

At-least-once everywhere, and make every write safe to repeat:

- Producers are idempotent (`enable.idempotence=true`, `acks=all`), so broker retries don't
  duplicate messages.
- Consumers commit offsets only **after** their outputs are durable: the detector after
  Kafka acknowledges its incidents, the store after the database transaction commits.
- Every database write is an upsert on a natural key: incident id; symbol + exchange +
  minute; exchange. Incident updates only overwrite an older version
  (`WHERE incidents.updated_at <= EXCLUDED.updated_at`).
- `feed.incidents` is compacted by incident id.

## Consequences

A crash can re-deliver a few messages, which overwrite the same rows with the same values:
exactly-once *results* without distributed transactions. The integration test replays a
full session through Kafka into PostgreSQL to check it.
