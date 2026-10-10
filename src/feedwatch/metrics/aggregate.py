"""One-minute market metrics per source and symbol: the reducer used by the Quix Streams
job, as plain functions so the maths is tested without Kafka.

The window state must be JSON-serialisable (Quix keeps it in RocksDB), so decimals are
carried as strings and latencies as a capped sample list.
"""

from __future__ import annotations

import random
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from feedwatch.detect.windows import percentile

LATENCY_SAMPLES = 512  # keep a bounded, uniform sample for percentiles


def init(trade: dict[str, Any]) -> dict[str, Any]:
    price = trade["price"]
    return {
        "source": trade["source"],
        "symbol": trade["symbol"],
        "trades": 1,
        "volume": trade["size"],
        "notional": str(Decimal(price) * Decimal(trade["size"])),
        "open": price,
        "high": price,
        "low": price,
        "close": price,
        "first_ms": trade["event_ms"],
        "last_ms": trade["event_ms"],
        "latency": [trade["ingest_ms"] - trade["event_ms"]],
    }


def reduce(agg: dict[str, Any], trade: dict[str, Any]) -> dict[str, Any]:
    price, size, t = Decimal(trade["price"]), Decimal(trade["size"]), trade["event_ms"]
    agg["trades"] += 1
    agg["volume"] = str(Decimal(agg["volume"]) + size)
    agg["notional"] = str(Decimal(agg["notional"]) + price * size)
    agg["high"] = str(max(Decimal(agg["high"]), price))
    agg["low"] = str(min(Decimal(agg["low"]), price))
    if t < agg["first_ms"]:  # trades can arrive slightly out of order
        agg["first_ms"], agg["open"] = t, trade["price"]
    if t >= agg["last_ms"]:
        agg["last_ms"], agg["close"] = t, trade["price"]
    lat = trade["ingest_ms"] - t
    samples: list[int] = agg["latency"]
    if len(samples) < LATENCY_SAMPLES:
        samples.append(lat)
    else:  # reservoir sampling keeps a uniform sample of the whole minute
        j = random.randrange(agg["trades"])
        if j < LATENCY_SAMPLES:
            samples[j] = lat
    return agg


def finish(agg: dict[str, Any], window_start_ms: int) -> dict[str, Any]:
    volume = Decimal(agg["volume"])
    vwap = Decimal(agg["notional"]) / volume if volume > 0 else Decimal(agg["close"])
    lats = [float(x) for x in agg["latency"]]
    p50, p95 = percentile(lats, 50), percentile(lats, 95)
    return {
        "bucket": datetime.fromtimestamp(window_start_ms / 1000, UTC).isoformat(),
        "source": agg["source"],
        "symbol": agg["symbol"],
        "trades": agg["trades"],
        "volume": str(volume),
        "vwap": str(vwap.quantize(Decimal("1e-10")).normalize()),
        "open": agg["open"],
        "high": agg["high"],
        "low": agg["low"],
        "close": agg["close"],
        "latency_p50_ms": None if p50 is None else round(p50),
        "latency_p95_ms": None if p95 is None else round(p95),
    }
