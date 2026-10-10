# 0001: Monitor the data, not the market

**Status:** accepted · 10 Oct 2026

## Context

A streaming project on market data usually computes candles or signals, which answers no
one's question and drifts towards trading. The people who actually suffer from market
data are the ones who *consume* it: apps, brokers, price sites and risk teams, whose
problem is that feeds fail quietly (stalls, gaps, bad prints, disagreeing sources).

## Decision

Build a data quality monitor. Its one question: *can I trust the price data I'm receiving
right now, and if not, which source is broken and since when?* Stablecoin depegs are the
headline use case because they matter to anyone holding them and are easy to verify on
real history (USDC, March 2023).

## Consequences

- Every feature must answer "is the data trustworthy?"; analytics that don't (candles for
  their own sake) are out of scope. The one-minute metrics exist for latency and peg
  history.
- No orders, no accounts, no keys, no advice. Stated in the README and the dashboard.
- It builds on the author's data quality work (DQLabs, an AI data quality pipeline).
