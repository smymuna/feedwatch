"""The ingest service: one long-lived WebSocket per exchange, normalised into events.

For each source it connects, subscribes and streams messages, reconnecting with
exponential backoff (1 s, 2 s, 4 s ... capped at 60 s, with jitter so many clients don't
reconnect in lockstep). Every connection change is itself an event, so the detector knows
the difference between "the exchange went quiet" and "we lost the connection".

A message that can't be parsed goes to the dead-letter topic with the error, rather
than crashing the service or being dropped silently.
"""

from __future__ import annotations

import asyncio
import contextlib
import gzip
import json
import logging
import random
import signal
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import IO, Any

import websockets

from feedwatch.models import Connection, Event, Tick
from feedwatch.sources.base import Source

log = logging.getLogger(__name__)

Emit = Callable[[Event], None]
DeadLetter = Callable[[str, str, Exception], None]


def now_ms() -> int:
    return time.time_ns() // 1_000_000


class RawRecorder:
    """Optionally keep every raw message, gzipped per source and hour, so any session can
    be replayed later (``feedwatch replay``)."""

    def __init__(self, root: str) -> None:
        self.root = Path(root)
        self._files: dict[str, tuple[str, IO[str]]] = {}

    def write(self, source: str, ingest_ms: int, raw: str | bytes) -> None:
        hour = datetime.fromtimestamp(ingest_ms / 1000, UTC).strftime("%Y-%m-%d/%H")
        current = self._files.get(source)
        if current is None or current[0] != hour:
            if current:
                current[1].close()
            path = self.root / source / f"{hour}.jsonl.gz"
            path.parent.mkdir(parents=True, exist_ok=True)
            current = (hour, gzip.open(path, "at"))  # noqa: SIM115 (kept open across writes)
            self._files[source] = current
        text = raw.decode() if isinstance(raw, bytes) else raw
        current[1].write(json.dumps({"ingest_ms": ingest_ms, "raw": text}) + "\n")

    def close(self) -> None:
        for _, f in self._files.values():
            f.close()


async def run_source(
    source: Source,
    emit: Emit,
    dead_letter: DeadLetter,
    stop: asyncio.Event,
    recorder: RawRecorder | None = None,
) -> None:
    backoff = 1.0
    while not stop.is_set():
        try:
            async with websockets.connect(
                source.url, open_timeout=15, ping_interval=20, ping_timeout=20, max_size=2**22
            ) as ws:
                emit(Connection(source.name, "connected", now_ms()))
                for msg in source.subscribe_messages():
                    await ws.send(json.dumps(msg))
                backoff = 1.0
                log.info("%s: connected and subscribed", source.name)
                async for raw in ws:
                    ingest = now_ms()
                    if recorder:
                        recorder.write(source.name, ingest, raw)
                    try:
                        events = source.parse(raw, ingest)
                    except Exception as exc:  # malformed or unexpected message
                        text = raw.decode() if isinstance(raw, bytes) else raw
                        dead_letter(source.name, text, exc)
                        continue
                    for event in events:
                        emit(event)
                    if stop.is_set():
                        break
            reason = "closed"
        except (OSError, TimeoutError, websockets.WebSocketException) as exc:
            reason = repr(exc)
        emit(Connection(source.name, "disconnected", now_ms(), detail=reason))
        if stop.is_set():
            break
        delay = min(60.0, backoff) * random.uniform(0.5, 1.5)
        backoff *= 2
        log.warning("%s: disconnected (%s), retrying in %.1f s", source.name, reason, delay)
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=delay)


async def run_clock(emit: Emit, stop: asyncio.Event, every_s: float = 1.0) -> None:
    """A tick every second, so time-based rules run even when every exchange is silent."""
    while not stop.is_set():
        emit(Tick(now_ms()))
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=every_s)


async def run(
    sources: list[Source],
    emit: Emit,
    dead_letter: DeadLetter,
    recorder: RawRecorder | None = None,
    on_tick: Callable[[], Any] | None = None,
) -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)

    def clock_emit(event: Event) -> None:
        emit(event)
        if on_tick:
            on_tick()

    tasks = [asyncio.create_task(run_source(s, emit, dead_letter, stop, recorder)) for s in sources]
    tasks.append(asyncio.create_task(run_clock(clock_emit, stop)))
    try:
        await asyncio.gather(*tasks)
    finally:
        if recorder:
            recorder.close()
