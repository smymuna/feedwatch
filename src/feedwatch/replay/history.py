"""Download historical trades from Kraken's public REST API, to replay real incidents.

Kraken keeps its full public trade history: ``GET /0/public/Trades?pair=..&since=<ns>``
returns up to 1,000 trades per call plus a cursor for the next page. Each trade has the
same consecutive ``trade_id`` as the live feed, so a replay exercises every rule.

We pause between calls to stay well inside Kraken's public rate limits.
"""

from __future__ import annotations

import csv
import gzip
import time
from collections.abc import Iterator
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import httpx

from feedwatch.models import Trade

API = "https://api.kraken.com/0/public/Trades"
FIELDS = ["trade_id", "time_ms", "price", "volume", "side"]


def fetch_kraken_trades(
    pair: str,
    start: datetime,
    end: datetime,
    *,
    pause_s: float = 1.1,
    client: httpx.Client | None = None,
) -> Iterator[dict[str, str]]:
    """Yield trades for a Kraken REST pair (e.g. ``USDCUSD``) between two UTC datetimes."""
    since = int(start.timestamp() * 1e9)
    end_ms = int(end.timestamp() * 1000)
    own = client is None
    http = client or httpx.Client(
        timeout=30, headers={"User-Agent": "feedwatch (portfolio project)"}
    )
    try:
        while True:
            for attempt in range(5):
                body = http.get(API, params={"pair": pair, "since": since, "count": 1000}).json()
                if not body.get("error"):
                    break
                if any("Too many requests" in e for e in body["error"]):
                    time.sleep(5 * (attempt + 1))
                    continue
                raise RuntimeError(f"Kraken error: {body['error']}")
            result = body["result"]
            rows = next(v for k, v in result.items() if k != "last")
            for price, volume, ts, side, _ord, _misc, trade_id in rows:
                time_ms = int(Decimal(str(ts)) * 1000)
                if time_ms > end_ms:
                    return
                yield {
                    "trade_id": str(trade_id),
                    "time_ms": str(time_ms),
                    "price": price,
                    "volume": volume,
                    "side": "buy" if side == "b" else "sell",
                }
            nxt = int(result["last"])
            if not rows or nxt == since:
                return
            since = nxt
            time.sleep(pause_s)
    finally:
        if own:
            http.close()


def save(rows: Iterator[dict[str, str]], path: Path) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with gzip.open(path, "wt", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
            count += 1
    return count


def load(path: Path, symbol: str, source: str = "kraken") -> Iterator[Trade]:
    """Read a saved file back as trades. Historical data has no arrival time, so
    ``ingest_ms`` equals ``event_ms`` (latency rules see zero delay)."""
    with gzip.open(path, "rt", newline="") as f:
        for row in csv.DictReader(f):
            t = int(row["time_ms"])
            yield Trade(
                source=source,
                symbol=symbol,
                trade_id=int(row["trade_id"]),
                price=Decimal(row["price"]),
                size=Decimal(row["volume"]),
                side=row["side"],
                event_ms=t,
                ingest_ms=t,
            )
