"""The detection engine: turns the event stream into incidents and a live health view.

It is deliberately plain Python with no Kafka or framework imports:

- deterministic: the same events always give the same incidents, so replaying a recorded
  session (or the March 2023 USDC depeg) is an exact test;
- fast to test: rules are exercised with hand-built event sequences;
- portable: ``feedwatch.detect.service`` wires it to Kafka, but it could sit behind Flink,
  Spark or a plain consumer unchanged.

Time. Rules about *market prices* (divergence, depeg, outliers) use **event time**, when the
exchange says a trade happened. Rules about *the feed itself* (silence, latency) use
**processing time**, when we received something, because "we heard nothing for 10 s" is
about arrival. The once-a-second ``Tick`` from the ingest service moves processing time
forward even when every exchange is quiet.

The rules are documented with their reasoning in docs/detection-rules.md.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Iterable
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from feedwatch.config import Thresholds
from feedwatch.detect.windows import (
    RateCounter,
    TradeWindow,
    ValueWindow,
    median_and_mad,
    percentile,
)
from feedwatch.models import (
    Connection,
    Event,
    Heartbeat,
    Incident,
    IncidentType,
    Severity,
    Tick,
    Trade,
    base_asset,
    is_stablecoin,
)

ONE = Decimal(1)
BURST_QUIET_MS = 60_000  # a gap/outlier burst resolves after a minute without new hits
REEMIT_MS = 30_000  # re-publish an unchanged open incident at most this often
OUTLIER_MIN_HISTORY = 20
OUTLIER_CONFIRM = 2  # trades back near the median that confirm a bad print
DEESCALATE = 0.75  # a critical incident steps down to warning only below 75 % of the level

IncidentKey = tuple[IncidentType, str | None, str | None]


@dataclass
class SourceState:
    connected: bool = False
    last_message_ms: int | None = None
    last_trade_ms: int | None = None
    last_error: str = ""
    messages: RateCounter = field(default_factory=RateCounter)
    trades: RateCounter = field(default_factory=RateCounter)
    latency: ValueWindow = field(default_factory=lambda: ValueWindow(60_000))
    gaps_total: int = 0
    missing_total: int = 0
    duplicates_total: int = 0


@dataclass
class FeedState:
    """One source's view of one symbol."""

    window: TradeWindow
    recent: deque[Decimal]
    last_trade_id: int | None = None
    heartbeats_ahead: int = 0
    last_price: Decimal | None = None
    last_event_ms: int | None = None
    pending_outlier: dict[str, Any] | None = None


class Detector:
    def __init__(
        self,
        sources: Iterable[str],
        thresholds: Thresholds | None = None,
        *,
        feed_rules: bool = True,
    ) -> None:
        """``feed_rules=False`` turns off the rules about the connection itself (silence,
        latency), for replaying trade history, which has no heartbeats or arrival times."""
        self.t = thresholds or Thresholds()
        self.feed_rules = feed_rules
        self.now = 0
        self.started_ms: int | None = None
        self.sources: dict[str, SourceState] = {s: SourceState() for s in sources}
        latency_ms = int(self.t.latency_window_s * 1000)
        for st in self.sources.values():
            st.latency = ValueWindow(latency_ms)
        self.feeds: dict[tuple[str, str], FeedState] = {}
        self.open: dict[IncidentKey, Incident] = {}
        self._emitted_ms: dict[str, int] = {}
        self._changes: list[Incident] = []
        self._div_since: dict[str, int] = {}
        self._div_bps: dict[str, float] = {}
        self._peg_ok_since: dict[str, int] = {}

    # ---------------------------------------------------------------- entry point
    def process(self, event: Event) -> list[Incident]:
        """Feed one event; returns the incidents that opened, changed or resolved."""
        self._changes = []
        self.now = max(self.now, event.ingest_ms)
        if self.started_ms is None:
            self.started_ms = self.now
        match event:
            case Trade():
                self._on_trade(event)
            case Heartbeat():
                self._touch(event.source)
                self._on_heartbeat(event)
            case Connection():
                self._on_connection(event)
            case Tick():
                pass
        self._time_rules()
        return self._changes

    # ---------------------------------------------------------------- events
    def _source(self, name: str) -> SourceState:
        return self.sources.setdefault(name, SourceState())

    def _feed(self, source: str, symbol: str) -> FeedState:
        key = (source, symbol)
        if key not in self.feeds:
            self.feeds[key] = FeedState(
                window=TradeWindow(int(self.t.depeg_window_s * 1000)),
                recent=deque(maxlen=self.t.outlier_window),
            )
        return self.feeds[key]

    def _touch(self, source: str) -> None:
        st = self._source(source)
        st.last_message_ms = self.now
        st.connected = True
        st.messages.add(self.now)
        self._resolve((IncidentType.FEED_SILENT, source, None), "Messages are flowing again.")

    def _on_connection(self, ev: Connection) -> None:
        st = self._source(ev.source)
        if ev.state == "disconnected":
            st.connected = False
        elif ev.state == "error":
            st.last_error = ev.detail
        else:
            self._touch(ev.source)

    def _on_trade(self, tr: Trade) -> None:
        self._touch(tr.source)
        st = self._source(tr.source)
        fs = self._feed(tr.source, tr.symbol)
        self._check_sequence(tr, fs)
        if tr.snapshot:  # an old trade re-sent on subscribe: it sets the sequence baseline only
            return
        st.trades.add(self.now)
        st.last_trade_ms = self.now
        st.latency.add(self.now, float(tr.ingest_ms - tr.event_ms))

        self._check_outlier(tr, fs)
        fs.recent.append(tr.price)
        fs.window.add(tr.event_ms, tr.price, tr.size)
        fs.last_price = tr.price
        fs.last_event_ms = tr.event_ms
        self._check_divergence(tr.symbol, tr.event_ms)
        if is_stablecoin(tr.symbol):
            self._check_depeg(tr.symbol, tr.event_ms)

    def _on_heartbeat(self, hb: Heartbeat) -> None:
        """Coinbase heartbeats carry the product's latest trade id. If it stays ahead of the
        last trade we received for two heartbeats, those trades never reached us."""
        if hb.symbol is None or hb.last_trade_id is None:
            return
        fs = self._feed(hb.source, hb.symbol)
        if fs.last_trade_id is None:
            fs.last_trade_id = hb.last_trade_id
            return
        if hb.last_trade_id > fs.last_trade_id:
            fs.heartbeats_ahead += 1
            if fs.heartbeats_ahead >= 2:
                self._record_gap(hb.source, hb.symbol, fs.last_trade_id + 1, hb.last_trade_id)
                fs.last_trade_id = hb.last_trade_id
                fs.heartbeats_ahead = 0
        else:
            fs.heartbeats_ahead = 0

    # ---------------------------------------------------------------- rules on trades
    def _check_sequence(self, tr: Trade, fs: FeedState) -> None:
        if tr.trade_id is None:
            return
        last = fs.last_trade_id
        if last is None or tr.trade_id == last + 1:
            fs.last_trade_id = tr.trade_id
        elif tr.trade_id > last + 1:
            self._record_gap(tr.source, tr.symbol, last + 1, tr.trade_id - 1)
            fs.last_trade_id = tr.trade_id
        else:  # already seen (e.g. replayed after a reconnect): ignore, but count it
            self._source(tr.source).duplicates_total += 1
        fs.heartbeats_ahead = 0

    def _record_gap(self, source: str, symbol: str, first: int, last: int) -> None:
        missing = last - first + 1
        st = self._source(source)
        st.missing_total += missing
        key = (IncidentType.TRADE_GAP, source, symbol)
        inc = self.open.get(key)
        # Heartbeats reveal one outage a second at a time; a range that continues the last
        # one is the same gap, not a new one.
        contiguous = inc is not None and inc.details["last_gap"][1] + 1 == first
        if contiguous:
            assert inc is not None
            first = inc.details["last_gap"][0]
        else:
            st.gaps_total += 1
        gaps = (inc.details["gaps"] if inc else 0) + (0 if contiguous else 1)
        total = (inc.details["missing_trades"] if inc else 0) + missing
        severity = Severity.CRITICAL if total >= 1000 else Severity.WARNING
        self._upsert(
            key,
            severity,
            f"{total:,} {symbol} trades missing from {source} in {gaps} gap(s)",
            {
                "gaps": gaps,
                "missing_trades": total,
                "last_gap": [first, last],
                "last_hit_ms": self.now,
            },
            force=True,
        )

    def _check_outlier(self, tr: Trade, fs: FeedState) -> None:
        """A bad print is a trade far from the recent median that the market *ignores*: the
        next trades go straight back. If the next trades follow it, it was a real move (a
        crash, a squeeze) and nothing is reported. So a suspect trade waits for
        ``OUTLIER_CONFIRM`` more trades before it becomes an incident."""
        pending = fs.pending_outlier
        if pending is not None:
            ref = Decimal(pending["median"])
            back = abs(tr.price - ref) / ref * 10_000 < Decimal(str(self.t.outlier_min_bps / 2))
            if not back:  # the market moved with it: not a bad print
                fs.pending_outlier = None
            else:
                pending["confirmations"] += 1
                if pending["confirmations"] >= OUTLIER_CONFIRM:
                    fs.pending_outlier = None
                    self._report_outlier(tr.source, tr.symbol, pending)
                return
        if len(fs.recent) < OUTLIER_MIN_HISTORY or self._dislocated(tr.symbol):
            return
        med, mad = median_and_mad(list(fs.recent))
        if med <= 0:
            return
        dev = abs(tr.price - med)
        dev_bps = float(dev / med * 10_000)
        if dev_bps > self.t.outlier_min_bps and dev > Decimal(str(self.t.outlier_mad_k)) * mad:
            fs.pending_outlier = {
                "price": str(tr.price),
                "median": str(med),
                "deviation_bps": round(dev_bps, 1),
                "trade_id": tr.trade_id,
                "confirmations": 0,
            }

    def _dislocated(self, symbol: str) -> bool:
        """While a symbol is depegged or exchanges disagree on it, wild prints are expected
        and are part of that incident, so the bad-print rule pauses for it."""
        return (IncidentType.DEPEG, None, symbol) in self.open or (
            IncidentType.PRICE_DIVERGENCE,
            None,
            symbol,
        ) in self.open

    def _report_outlier(self, source: str, symbol: str, suspect: dict[str, Any]) -> None:
        key = (IncidentType.OUTLIER_PRINT, source, symbol)
        inc = self.open.get(key)
        count = (inc.details["count"] if inc else 0) + 1
        self._upsert(
            key,
            Severity.INFO,
            f"{count} {symbol} trade(s) on {source} far from the market, which ignored them",
            {
                "count": count,
                "last_price": suspect["price"],
                "median": suspect["median"],
                "deviation_bps": suspect["deviation_bps"],
                "trade_id": suspect["trade_id"],
                "last_hit_ms": self.now,
            },
            force=True,
        )

    def _fresh_prices(self, symbol: str, at_ms: int) -> dict[str, Decimal]:
        """Each source's recent price (median of its last 5 trades) if it traded recently."""
        fresh_ms = int(self.t.price_fresh_s * 1000)
        out: dict[str, Decimal] = {}
        for (source, sym), fs in self.feeds.items():
            if sym != symbol or fs.last_event_ms is None or not fs.recent:
                continue
            if at_ms - fs.last_event_ms <= fresh_ms:
                last5 = list(fs.recent)[-5:]
                out[source] = sorted(last5)[len(last5) // 2]
        return out

    def _check_divergence(self, symbol: str, at_ms: int) -> None:
        prices = self._fresh_prices(symbol, at_ms)
        key = (IncidentType.PRICE_DIVERGENCE, None, symbol)
        if len(prices) < 2:
            self._div_bps.pop(symbol, None)
            return
        hi, lo = max(prices.values()), min(prices.values())
        mid = (hi + lo) / 2
        bps = float((hi - lo) / mid * 10_000)
        self._div_bps[symbol] = bps
        limit = (
            self.t.divergence_bps_stable if is_stablecoin(symbol) else self.t.divergence_bps_major
        )
        if bps > limit:
            since = self._div_since.setdefault(symbol, at_ms)
            if at_ms - since >= self.t.divergence_hold_s * 1000:
                likely = None
                if is_stablecoin(symbol):  # the source further from $1 is the odd one out
                    likely = max(prices, key=lambda s: abs(prices[s] - ONE))
                current = self.open.get(key)
                critical = bps > 3 * limit or (
                    current is not None
                    and current.severity is Severity.CRITICAL
                    and bps > DEESCALATE * 3 * limit
                )
                self._upsert(
                    key,
                    Severity.CRITICAL if critical else Severity.WARNING,
                    f"{symbol} differs by {bps:.0f} bps between "
                    + " and ".join(f"{s} ({p:.6g})" for s, p in sorted(prices.items())),
                    {
                        "bps": round(bps, 1),
                        "prices": {s: str(p) for s, p in prices.items()},
                        "limit_bps": limit,
                        "likely_source": likely,
                        "since_ms": since,
                    },
                )
        elif bps < 0.8 * limit:
            self._div_since.pop(symbol, None)
            self._resolve(key, f"{symbol} prices agree again ({bps:.0f} bps).")

    def _check_depeg(self, symbol: str, at_ms: int) -> None:
        devs: dict[str, float] = {}
        vwaps: dict[str, Decimal] = {}
        for (source, sym), fs in self.feeds.items():
            if sym != symbol:
                continue
            fs.window.expire(at_ms)
            vwap = fs.window.vwap()
            if vwap is not None and len(fs.window) >= self.t.depeg_min_trades:
                vwaps[source] = vwap
                devs[source] = float((vwap - ONE) * 100)
        if not devs:
            return
        key = (IncidentType.DEPEG, None, symbol)
        smallest = min(abs(d) for d in devs.values())
        coin = base_asset(symbol)
        if smallest >= self.t.depeg_warn_pct:  # every source with data agrees it's off-peg
            self._peg_ok_since.pop(symbol, None)
            inc = self.open.get(key)
            low = min(vwaps.values())
            if inc and "low" in inc.details:
                low = min(low, Decimal(inc.details["low"]))
            worst = max(devs, key=lambda s: abs(devs[s]))
            where = " and ".join(sorted(devs))
            # hysteresis: escalate at the critical level, step down only well below it
            was_critical = inc is not None and inc.severity is Severity.CRITICAL
            critical = smallest >= self.t.depeg_critical_pct or (
                was_critical and smallest >= DEESCALATE * self.t.depeg_critical_pct
            )
            self._upsert(
                key,
                Severity.CRITICAL if critical else Severity.WARNING,
                f"{coin} trading at ${vwaps[worst]:.4f} ({devs[worst]:+.2f} %) on {where}",
                {
                    "deviation_pct": {s: round(d, 3) for s, d in devs.items()},
                    "vwap": {s: str(v) for s, v in vwaps.items()},
                    "low": str(low),
                    "confirmed": len(devs) >= 2,
                },
            )
        elif key in self.open:
            if max(abs(d) for d in devs.values()) < self.t.depeg_resolve_pct:
                since = self._peg_ok_since.setdefault(symbol, at_ms)
                if at_ms - since >= self.t.depeg_resolve_hold_s * 1000:
                    self._peg_ok_since.pop(symbol, None)
                    self._resolve(key, f"{coin} back within {self.t.depeg_resolve_pct} % of $1.")
            else:
                self._peg_ok_since.pop(symbol, None)

    # ---------------------------------------------------------------- time-based rules
    def _time_rules(self) -> None:
        silent_ms = self.t.silent_after_s * 1000
        for name, st in self.sources.items() if self.feed_rules else ():
            last = st.last_message_ms if st.last_message_ms is not None else self.started_ms
            if last is not None and self.now - last > silent_ms:
                key = (IncidentType.FEED_SILENT, name, None)
                if key not in self.open:
                    self._upsert(
                        key,
                        Severity.CRITICAL,
                        f"No messages from {name} for over {self.t.silent_after_s:.0f} s",
                        {
                            "silent_since_ms": last,
                            "connected": st.connected,
                            "last_error": st.last_error,
                        },
                    )
            values = st.latency.values(self.now)
            p95 = percentile(values, 95) if len(values) >= 20 else None
            key = (IncidentType.HIGH_LATENCY, name, None)
            if p95 is not None and p95 > self.t.latency_p95_ms:
                self._upsert(
                    key,
                    Severity.WARNING,
                    f"{name} data is arriving late: p95 {p95 / 1000:.1f} s",
                    {"p95_ms": round(p95), "samples": len(values)},
                )
            elif p95 is not None and p95 < 0.7 * self.t.latency_p95_ms:
                self._resolve(key, f"{name} latency back to normal (p95 {p95:.0f} ms).")
        for open_key, inc in list(self.open.items()):
            if inc.type in (IncidentType.TRADE_GAP, IncidentType.OUTLIER_PRINT) and (
                self.now - int(inc.details.get("last_hit_ms", inc.updated_ms)) > BURST_QUIET_MS
            ):
                self._resolve(open_key, "No new occurrences for a minute.")

    # ---------------------------------------------------------------- incidents
    def _upsert(
        self,
        key: IncidentKey,
        severity: Severity,
        summary: str,
        details: dict[str, Any],
        *,
        force: bool = False,
    ) -> None:
        inc = self.open.get(key)
        if inc is None:
            itype, source, symbol = key
            inc = Incident(
                id=f"{itype.value}:{source or '*'}:{symbol or '*'}:{self.now}",
                type=itype,
                severity=severity,
                source=source,
                symbol=symbol,
                opened_ms=self.now,
                updated_ms=self.now,
                summary=summary,
                details=details,
            )
            self.open[key] = inc
            self._emit(inc)
            return
        changed = inc.severity != severity
        inc.severity, inc.summary, inc.details = severity, summary, details
        inc.updated_ms = self.now
        if changed or force or self.now - self._emitted_ms.get(inc.id, 0) >= REEMIT_MS:
            self._emit(inc)

    def _resolve(self, key: IncidentKey, summary: str) -> None:
        inc = self.open.pop(key, None)
        if inc is None:
            return
        inc.resolved_ms = inc.updated_ms = self.now
        inc.details = {**inc.details, "resolution": summary}
        self._emit(inc)

    def _emit(self, inc: Incident) -> None:
        self._emitted_ms[inc.id] = self.now
        snapshot = Incident(**{k: getattr(inc, k) for k in Incident.__dataclass_fields__})
        snapshot.details = dict(inc.details)
        self._changes.append(snapshot)

    # ---------------------------------------------------------------- health view
    def health(self) -> dict[str, Any]:
        """A snapshot for dashboards: one entry per source and per source+symbol."""
        now = self.now
        sources = []
        for name, st in sorted(self.sources.items()):
            values = st.latency.values(now)
            p95 = percentile(values, 95)
            silent = (IncidentType.FEED_SILENT, name, None) in self.open
            sources.append(
                {
                    "source": name,
                    "connected": st.connected and not silent,
                    "last_message_ms": st.last_message_ms,
                    "last_trade_ms": st.last_trade_ms,
                    "messages_per_s": round(st.messages.per_second(now), 2),
                    "trades_per_s": round(st.trades.per_second(now), 2),
                    "latency_p95_ms": None if p95 is None else round(p95),
                    "gaps_total": st.gaps_total,
                    "missing_trades_total": st.missing_total,
                }
            )
        symbols = []
        for (source, symbol), fs in sorted(self.feeds.items(), key=lambda kv: (kv[0][1], kv[0][0])):
            if fs.last_event_ms is None:
                continue
            fs.window.expire(now)
            vwap = fs.window.vwap()
            symbols.append(
                {
                    "symbol": symbol,
                    "source": source,
                    "last_price": None if fs.last_price is None else str(fs.last_price),
                    "vwap_60s": None if vwap is None else str(vwap),
                    "trades_60s": len(fs.window),
                    "peg_dev_pct": (
                        round(float((vwap - ONE) * 100), 4)
                        if vwap is not None and is_stablecoin(symbol)
                        else None
                    ),
                    "divergence_bps": (
                        None if symbol not in self._div_bps else round(self._div_bps[symbol], 2)
                    ),
                    "last_trade_ms": fs.last_event_ms,
                }
            )
        return {"at_ms": now, "sources": sources, "symbols": symbols}
