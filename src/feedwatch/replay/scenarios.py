"""Fault injection: replay a real recorded session with something deliberately broken.

Production incidents are rare, so to show (and test) that the pipeline catches them we
take a normal session and break it on purpose, e.g. "Kraken goes silent from 60 s to 90 s"
or "Coinbase prices drift 1 % high". A scenario is a small JSON file in ``scenarios/``.

Times are seconds from the start of the replay.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, replace
from decimal import Decimal
from pathlib import Path
from typing import Literal

from feedwatch.models import Event, Trade

FaultKind = Literal["drop_trades", "silence", "shift_price", "delay", "spike"]


@dataclass(frozen=True)
class Fault:
    kind: FaultKind
    source: str
    start_s: float
    end_s: float
    symbol: str | None = None
    pct: float = 0.0  # shift_price / spike: move prices by this many percent
    ms: int = 0  # delay: hold messages back by this long

    def active(self, rel_s: float, event: Event) -> bool:
        if not (self.start_s <= rel_s < self.end_s):
            return False
        if getattr(event, "source", None) != self.source:
            return False
        return self.symbol is None or getattr(event, "symbol", None) == self.symbol


@dataclass(frozen=True)
class Scenario:
    name: str
    description: str
    faults: list[Fault]


def load(path: str | Path) -> Scenario:
    data = json.loads(Path(path).read_text())
    return Scenario(
        name=data["name"],
        description=data.get("description", ""),
        faults=[Fault(**f) for f in data["faults"]],
    )


def inject(events: Iterable[Event], faults: list[Fault]) -> Iterator[Event]:
    """Apply faults to an ordered event stream. The first event's time is t = 0."""
    t0: int | None = None
    spiked: set[int] = set()
    for event in events:
        t0 = event.ingest_ms if t0 is None else t0
        rel = (event.ingest_ms - t0) / 1000
        out: Event | None = event
        for i, f in enumerate(faults):
            if out is None or not f.active(rel, out):
                continue
            if f.kind == "silence" or (f.kind == "drop_trades" and isinstance(out, Trade)):
                out = None
            elif f.kind == "shift_price" and isinstance(out, Trade):
                out = replace(out, price=out.price * (1 + Decimal(str(f.pct)) / 100))
            elif f.kind == "spike" and isinstance(out, Trade) and i not in spiked:
                spiked.add(i)
                out = replace(out, price=out.price * (1 + Decimal(str(f.pct)) / 100))
            elif f.kind == "delay":
                out = replace(out, ingest_ms=out.ingest_ms + f.ms)
        if out is not None:
            yield out
