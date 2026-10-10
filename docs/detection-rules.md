# Detection rules

All rules live in `src/feedwatch/detect/detector.py`. Thresholds are settings
(`FEEDWATCH_THRESHOLDS__<NAME>`, defaults in `src/feedwatch/config.py`), so they can be
tuned without code changes. Every rule has unit tests with hand-built event sequences, and
all of them are checked against the real March 2023 USDC data and the healthy 5-minute
live session (which must produce no incidents).

## How incidents behave

- An incident **opens**, may **change severity**, and **resolves**. Each change is
  published to `feed.incidents` under a stable id, so consumers keep the latest version.
- An unchanged open incident is re-published at most every 30 s (to refresh its details),
  not on every trade.
- Bursty problems (missing trades, bad prints) are grouped: one incident per exchange and
  coin, counting occurrences, which resolves after a minute without new ones.
- **Hysteresis:** a critical incident steps down to warning only below 75 % of the critical
  level, so a price hovering at the threshold doesn't flip back and forth. (Without this,
  the March 2023 replay produced a dozen severity changes in one hour.)

## Feed silent · critical

| Setting | Default |
|---|---|
| `silent_after_s` | 10 |

No message of any kind (trade, heartbeat, connection event) from an exchange for 10 s of
processing time. Both exchanges send heartbeats every second, so 10 s of silence is a real
problem even when the market is quiet. The ingest service's once-a-second clock tick lets
the rule fire while nothing at all is arriving. Resolves on the next message from that
exchange.

## Missing trades · warning, critical from 1,000 trades

Trade ids are consecutive per coin on both exchanges (checked on the captured data).

- A trade whose id jumps ahead (101, then 105) means 102–104 never arrived.
- A Coinbase heartbeat whose `last_trade_id` stays ahead of the last trade we received for
  **two** consecutive heartbeats means trades were lost even though the connection looks
  healthy. Two, not one, because a heartbeat can briefly overtake the trade just before it.
- A range that continues the previous one is the same gap, so a 20-second outage seen one
  heartbeat at a time is reported as **one gap of 51 trades**, not ten small ones.
- Repeated ids (after a reconnect) are counted as duplicates and ignored, not reported.
- The `last_match` snapshot Coinbase sends on subscribe sets the starting id.

## Price divergence · warning, critical above 3× the limit

| Setting | Default |
|---|---|
| `divergence_bps_major` | 50 (BTC, ETH, SOL) |
| `divergence_bps_stable` | 30 (stablecoins) |
| `divergence_hold_s` | 10 |
| `price_fresh_s` | 20 |

Each exchange's price is the median of its last five trades, which ignores a single odd
print. Prices are compared only when **both** exchanges traded in the last 20 s. A stale
price from a thinly traded pair is not a divergence. The difference must last 10 s
(event time) before an incident opens, and it resolves below 80 % of the limit.

For stablecoins the incident names the exchange further from $1 as the likely bad source.

Why 50 bps: in the healthy sample session the two exchanges differ by a median of under
1 bp for BTC, ETH and SOL, and never more than 6.5 bps; USDT never more than 0.4 bps. So
50 bps (30 for stablecoins) leaves a wide margin above normal and still catches a bad feed
within seconds.

## Depeg · warning from 0.5 %, critical from 2 %

| Setting | Default |
|---|---|
| `depeg_window_s` | 60 |
| `depeg_min_trades` | 3 |
| `depeg_warn_pct` / `depeg_critical_pct` | 0.5 / 2.0 |
| `depeg_resolve_pct` / `depeg_resolve_hold_s` | 0.3 / 60 |

For each stablecoin (USDT, USDC, DAI, PYUSD, USDS) and exchange: the volume-weighted
average price of the last 60 s, if there were at least 3 trades. A depeg opens when
**every** exchange with enough data is at least 0.5 % from $1. If only one exchange is off
peg, the other is the problem, so it's a divergence instead. The incident says whether it
was *confirmed* by more than one exchange. It resolves only after a full minute within
0.3 %.

On the March 2023 data this gives an early warning on Friday night and one depeg incident
that goes critical within three hours, steps down 11 minutes after the official backstop
announcement, and resolves on Monday.

## Bad print · info

| Setting | Default |
|---|---|
| `outlier_window` | 50 trades |
| `outlier_min_bps` | 50 |
| `outlier_mad_k` | 10 |

A trade is *suspect* when it is more than 50 bps from the median of the last 50 trades
**and** more than 10 median absolute deviations away (MAD scales with how volatile the coin
is right now). It only becomes a bad print if the **next two trades snap back** near the
median. If the market follows it, it was a real move. The rule also pauses for a coin
while it has an open depeg or divergence, because wild prints are part of that incident.
(Before these two refinements, the March 2023 replay produced over 100 false "bad prints"
during the crash.)

## High latency · warning

| Setting | Default |
|---|---|
| `latency_p95_ms` | 3000 |
| `latency_window_s` | 60 |

The 95th percentile of (arrival time − exchange time) over the last minute, from at least
20 trades. It resolves below 70 % of the threshold. Snapshot trades are excluded. The rule
depends on our clock being in sync with the exchange's (NTP); a few milliseconds of skew
can make latency look slightly negative.
