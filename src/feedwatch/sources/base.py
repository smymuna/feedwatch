"""What every exchange source provides: where to connect, what to subscribe to, and how
to turn its raw messages into our events. Parsing is pure (no I/O) so it's easy to test
against captured real messages."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, Protocol

from feedwatch.models import Event

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


class Source(Protocol):
    name: str
    url: str

    def subscribe_messages(self) -> list[dict[str, Any]]: ...

    def parse(self, raw: str | bytes, ingest_ms: int) -> list[Event]: ...


def loads(raw: str | bytes) -> Any:
    """JSON with exact decimals: Kraken sends prices as JSON numbers, and we never want
    them to pass through a float."""
    return json.loads(raw, parse_float=Decimal)


def iso_to_ms(value: str) -> int:
    """'2026-10-10T18:48:45.197352Z' -> epoch milliseconds (exact integer maths)."""
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return (dt - _EPOCH) // timedelta(milliseconds=1)
