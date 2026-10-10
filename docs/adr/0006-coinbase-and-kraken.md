# 0006: Coinbase and Kraken as sources (not Binance)

**Status:** accepted · 10 Oct 2026

## Context

The project needs free, public, real-time trade feeds that work without an account, from
at least two exchanges so their prices can be compared.

## Decision

Coinbase Exchange (`matches` + `heartbeat`) and Kraken (WebSocket v2 `trade`).

- Both are public and unauthenticated, and reachable from the US and the EU.
- Both number trades consecutively per coin, which makes gaps provable.
- Coinbase's per-product heartbeat carries the latest trade id, so even trades lost on a
  healthy-looking connection are detected.
- Kraken has full public trade history, so real incidents (the March 2023 USDC depeg) can
  be replayed and used as tests.

Binance was rejected: it refuses US and many cloud IP addresses (HTTP 451), which would
break the pipeline for its developer, its CI and some reviewers.

## Consequences

- Five symbols overlap (BTC, ETH, SOL, USDT, USDS) for cross-checking; USDC, DAI and PYUSD
  are Kraken-only, so their depegs are marked "unconfirmed" by a second source.
- Adding an exchange means one parser class plus a test file of captured messages.
