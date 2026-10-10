"""Small time-window helpers used by the detection rules. All times are epoch ms."""

from __future__ import annotations

from collections import deque
from decimal import Decimal
from statistics import median


class TradeWindow:
    """Trades in the last ``span_ms`` of event time, with a volume-weighted average price."""

    def __init__(self, span_ms: int) -> None:
        self.span_ms = span_ms
        self._items: deque[tuple[int, Decimal, Decimal]] = deque()
        self._notional = Decimal(0)
        self._volume = Decimal(0)

    def add(self, ts: int, price: Decimal, size: Decimal) -> None:
        self._items.append((ts, price, size))
        self._notional += price * size
        self._volume += size
        self.expire(ts)

    def expire(self, now: int) -> None:
        cutoff = now - self.span_ms
        while self._items and self._items[0][0] < cutoff:
            _, price, size = self._items.popleft()
            self._notional -= price * size
            self._volume -= size

    def __len__(self) -> int:
        return len(self._items)

    def vwap(self) -> Decimal | None:
        if not self._items or self._volume <= 0:
            return None
        return self._notional / self._volume

    def last_ts(self) -> int | None:
        return self._items[-1][0] if self._items else None


class RateCounter:
    """How many things happened in the last ``span_ms`` (processing time)."""

    def __init__(self, span_ms: int = 10_000) -> None:
        self.span_ms = span_ms
        self._ts: deque[int] = deque()

    def add(self, ts: int) -> None:
        self._ts.append(ts)

    def per_second(self, now: int) -> float:
        cutoff = now - self.span_ms
        while self._ts and self._ts[0] < cutoff:
            self._ts.popleft()
        return len(self._ts) / (self.span_ms / 1000)


class ValueWindow:
    """Numeric samples in the last ``span_ms``, for percentiles."""

    def __init__(self, span_ms: int) -> None:
        self.span_ms = span_ms
        self._items: deque[tuple[int, float]] = deque()

    def add(self, ts: int, value: float) -> None:
        self._items.append((ts, value))

    def values(self, now: int) -> list[float]:
        cutoff = now - self.span_ms
        while self._items and self._items[0][0] < cutoff:
            self._items.popleft()
        return [v for _, v in self._items]


def percentile(values: list[float], q: float) -> float | None:
    """Nearest-rank percentile, q in [0, 100]."""
    if not values:
        return None
    ordered = sorted(values)
    rank = max(0, min(len(ordered) - 1, round(q / 100 * len(ordered) + 0.5) - 1))
    return ordered[rank]


def median_and_mad(values: list[Decimal]) -> tuple[Decimal, Decimal]:
    """Median and median absolute deviation (a spread measure that ignores outliers)."""
    med = median(values)
    mad = median(abs(v - med) for v in values)
    return med, mad
