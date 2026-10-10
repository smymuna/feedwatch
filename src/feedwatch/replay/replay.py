"""Turn recordings into event streams, optionally with faults, for Kafka or offline checks.

Inputs
    - raw recordings from ``feedwatch ingest --record`` (``data/raw/<source>/...jsonl.gz``),
      parsed with the same parsers as the live service, both exchanges merged in time order;
    - historical trades from ``feedwatch history`` (``*.csv.gz``).

Clock ticks are added every second of replay time, as the live ingest service does.
"""

from __future__ import annotations

import gzip
import heapq
import json
import time
from collections.abc import Iterable, Iterator
from dataclasses import replace
from pathlib import Path

from feedwatch.models import Event, Heartbeat, Tick, Trade
from feedwatch.sources.base import Source
from feedwatch.sources.coinbase import Coinbase
from feedwatch.sources.kraken import Kraken

PARSERS: dict[str, Source] = {"coinbase": Coinbase(products=[]), "kraken": Kraken(symbols=[])}


def read_recording(root: str | Path) -> Iterator[Event]:
    """Every recorded raw message under ``root``, parsed, merged across sources by time."""
    streams = []
    for source_dir in sorted(Path(root).iterdir()):
        parser = PARSERS.get(source_dir.name)
        if parser is None or not source_dir.is_dir():
            continue
        streams.append(_read_source(sorted(source_dir.rglob("*.jsonl.gz")), parser))
    yield from heapq.merge(*streams, key=lambda e: e.ingest_ms)


def _read_source(files: list[Path], parser: Source) -> Iterator[Event]:
    for path in files:
        with gzip.open(path, "rt") as f:
            for line in f:
                rec = json.loads(line)
                yield from parser.parse(rec["raw"], int(rec["ingest_ms"]))


def with_ticks(events: Iterable[Event], every_ms: int = 1000) -> Iterator[Event]:
    """Insert a clock tick each second of replay time (and through quiet periods)."""
    next_tick: int | None = None
    for event in events:
        if next_tick is None:
            next_tick = event.ingest_ms + every_ms
        while event.ingest_ms >= next_tick:
            yield Tick(next_tick)
            next_tick += every_ms
        yield event


def rebase(events: Iterable[Event], start_ms: int) -> Iterator[Event]:
    """Shift every timestamp so the replay starts at ``start_ms`` (e.g. now), so a replayed
    session looks live on the dashboard."""
    offset: int | None = None
    for event in events:
        offset = start_ms - event.ingest_ms if offset is None else offset
        if isinstance(event, Trade) or (
            isinstance(event, Heartbeat) and event.event_ms is not None
        ):
            event = replace(event, event_ms=(event.event_ms or 0) + offset)
        yield replace(event, ingest_ms=event.ingest_ms + offset)


def paced(events: Iterable[Event], speed: float) -> Iterator[Event]:
    """Release events in (scaled) real time. ``speed`` 1 = real time, 10 = ten times faster,
    0 = as fast as possible."""
    if speed <= 0:
        yield from events
        return
    first: int | None = None
    wall0 = time.monotonic()
    for event in events:
        first = event.ingest_ms if first is None else first
        due = wall0 + (event.ingest_ms - first) / 1000 / speed
        delay = due - time.monotonic()
        if delay > 0:
            time.sleep(delay)
        yield event
