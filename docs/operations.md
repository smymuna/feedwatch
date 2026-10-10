# Operations

## Running

| Command | What |
|---|---|
| `make up` | live pipeline, dashboard on http://localhost:8000 |
| `make demo` | fresh stack replaying the sample session with six faults |
| `make logs` | follow the ingest and detector logs |
| `make down` | stop (add `-v` to `docker compose down` to also delete data) |

From the host, Kafka is on `localhost:29092` and PostgreSQL on `localhost:55432`
(user, password and database: `feedwatch`). The odd ports avoid clashing with local
installs.

## Health at a glance

- Dashboard header: "All feeds healthy" or the number of open incidents.
- `GET /api/health`: 200 with the time of the latest health snapshot. If that time stops
  moving, the detector or the store service is down.
- `GET /api/feeds`: per exchange: connected, messages/s, latency p95, gaps.

## Runbook

**Feed silent (critical).** Check `docker compose logs ingest` for reconnect messages. The
ingest service reconnects on its own with backoff up to 60 s. If the exchange is fine
(check its status page) but we stay silent, restart ingest: `docker compose restart ingest`.
Expect a *missing trades* incident when data returns (Kraken does not replay trades missed
during a disconnect).

**Missing trades.** One gap after a reconnect is expected. Repeated gaps while connected
mean we are dropping messages: check the dead-letter topic and the ingest logs for parse
errors. Missed trades can be fetched afterwards from the exchanges' REST APIs (for Kraken,
`feedwatch history`).

**Price divergence.** Compare both exchanges' public websites. If one exchange is
genuinely different (a halt, a squeeze), there's nothing to fix in the pipeline. If our
price is wrong but theirs isn't, it's our bug: replay the raw recording to reproduce it.

**Depeg.** Real market events. The incident says whether more than one exchange confirms
it. Nothing to fix in the pipeline; it's the signal people use this for.

**High latency.** Usually network or exchange load. If every source is late at once,
check the host's CPU and clock (`timedatectl` / NTP); clock skew shifts latency directly.

## Inspecting Kafka

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic feed.incidents --from-beginning
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --all-groups      # lag per consumer
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic market.events.dlq --from-beginning
```

## Querying the database

```bash
psql postgresql://feedwatch:feedwatch@localhost:55432/feedwatch
```

```sql
-- incidents in the last day
SELECT type, severity, symbol, source, opened_at, resolved_at, summary
FROM incidents WHERE opened_at > now() - interval '1 day' ORDER BY opened_at DESC;

-- USDT's peg minute by minute on both exchanges
SELECT bucket, source, vwap, trades FROM metrics_1m
WHERE symbol = 'USDT-USD' AND bucket > now() - interval '1 hour' ORDER BY bucket, source;
```

## Configuration

All settings are environment variables prefixed `FEEDWATCH_` (see `.env.example`),
including every detection threshold, e.g. `FEEDWATCH_THRESHOLDS__DEPEG_WARN_PCT=0.5`.
