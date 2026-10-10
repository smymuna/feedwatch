"""Detection rules, exercised with hand-built event sequences (times in ms)."""

from __future__ import annotations

from decimal import Decimal

import pytest

from feedwatch.config import Thresholds
from feedwatch.detect.detector import Detector
from feedwatch.models import Connection, Heartbeat, Incident, IncidentType, Severity, Tick, Trade

T0 = 1_700_000_000_000


def trade(
    source: str,
    symbol: str,
    price: str,
    t: int,
    trade_id: int | None = None,
    size: str = "1",
    delay: int = 50,
) -> Trade:
    return Trade(
        source=source,
        symbol=symbol,
        trade_id=trade_id,
        price=Decimal(price),
        size=Decimal(size),
        side="buy",
        event_ms=T0 + t,
        ingest_ms=T0 + t + delay,
    )


def run(det: Detector, events: list[object]) -> list[Incident]:
    out: list[Incident] = []
    for e in events:
        out += det.process(e)  # type: ignore[arg-type]
    return out


def of(changes: list[Incident], kind: IncidentType) -> list[Incident]:
    return [c for c in changes if c.type is kind]


# ---------------------------------------------------------------- gaps
def test_trade_id_jump_opens_a_gap_with_the_missing_count() -> None:
    det = Detector(["coinbase"])
    changes = run(
        det,
        [trade("coinbase", "BTC-USD", "60000", i * 100, 100 + i) for i in range(5)]
        + [trade("coinbase", "BTC-USD", "60000", 600, 110)],
    )
    (gap,) = of(changes, IncidentType.TRADE_GAP)
    assert gap.is_open and gap.details["missing_trades"] == 5
    assert gap.details["last_gap"] == [105, 109]
    assert det.sources["coinbase"].missing_total == 5


def test_gap_resolves_after_a_quiet_minute() -> None:
    det = Detector(["coinbase"])
    run(det, [trade("coinbase", "BTC-USD", "1", 0, 1), trade("coinbase", "BTC-USD", "1", 10, 3)])
    changes = run(det, [trade("coinbase", "BTC-USD", "1", 1_000 * s, 3 + s) for s in range(1, 70)])
    resolved = [c for c in of(changes, IncidentType.TRADE_GAP) if not c.is_open]
    assert resolved and "a minute" in resolved[0].details["resolution"]


def test_duplicates_are_counted_not_reported_as_gaps() -> None:
    det = Detector(["kraken"])
    changes = run(
        det,
        [trade("kraken", "ETH-USD", "2500", t, tid) for t, tid in [(0, 1), (1, 2), (2, 2), (3, 3)]],
    )
    assert not of(changes, IncidentType.TRADE_GAP)
    assert det.sources["kraken"].duplicates_total == 1


def test_heartbeat_reveals_trades_we_never_received() -> None:
    det = Detector(["coinbase"])
    hb = lambda t, last: Heartbeat("coinbase", "USDT-USD", last, T0 + t, T0 + t)  # noqa: E731
    changes = run(det, [trade("coinbase", "USDT-USD", "1", 0, 50), hb(1000, 53), hb(2000, 53)])
    (gap,) = of(changes, IncidentType.TRADE_GAP)
    assert gap.details["missing_trades"] == 3 and gap.details["last_gap"] == [51, 53]
    # the late trades then continue from 54 without a second gap
    assert not of(run(det, [trade("coinbase", "USDT-USD", "1", 2500, 54)]), IncidentType.TRADE_GAP)


def test_an_outage_seen_through_heartbeats_is_one_gap() -> None:
    det = Detector(["coinbase"])
    hb = lambda t, last: Heartbeat("coinbase", "BTC-USD", last, T0 + t, T0 + t)  # noqa: E731
    events: list[object] = [trade("coinbase", "BTC-USD", "1", 0, 100)]
    for s in range(1, 9):  # trades keep happening at the exchange, none reach us
        events.append(hb(s * 1000, 100 + 3 * s))
    events.append(trade("coinbase", "BTC-USD", "1", 9500, 128))
    gap = of(run(det, events), IncidentType.TRADE_GAP)[-1]
    assert gap.details["gaps"] == 1 and gap.details["missing_trades"] == 27
    assert gap.details["last_gap"] == [101, 127]
    assert det.sources["coinbase"].gaps_total == 1


def test_one_heartbeat_ahead_is_tolerated() -> None:
    det = Detector(["coinbase"])
    changes = run(
        det,
        [
            trade("coinbase", "USDT-USD", "1", 0, 50),
            Heartbeat("coinbase", "USDT-USD", 51, T0 + 900, T0 + 900),
            trade("coinbase", "USDT-USD", "1", 950, 51),
            Heartbeat("coinbase", "USDT-USD", 51, T0 + 1900, T0 + 1900),
        ],
    )
    assert not of(changes, IncidentType.TRADE_GAP)


# ---------------------------------------------------------------- silence
def test_a_silent_source_is_flagged_and_recovers() -> None:
    det = Detector(["coinbase", "kraken"], Thresholds(silent_after_s=10))
    events: list[object] = [Connection("kraken", "subscribed", T0)]
    events += [Tick(T0 + s * 1000) for s in range(1, 13)]
    changes = run(det, events)
    silent = of(changes, IncidentType.FEED_SILENT)
    assert {i.source for i in silent} == {"coinbase", "kraken"}
    assert all(i.severity is Severity.CRITICAL for i in silent)
    back = run(det, [Heartbeat("kraken", None, None, None, T0 + 13_000)])
    assert [(i.source, i.is_open) for i in back] == [("kraken", False)]
    assert det.health()["sources"][1]["connected"] is True


def test_no_silence_alert_while_messages_flow() -> None:
    det = Detector(["kraken"])
    events = [Heartbeat("kraken", None, None, None, T0 + s * 1000) for s in range(30)]
    assert not of(run(det, events), IncidentType.FEED_SILENT)


# ---------------------------------------------------------------- divergence
def test_sustained_price_divergence_between_exchanges() -> None:
    det = Detector(["coinbase", "kraken"])
    events = []
    for s in range(15):
        events += [
            trade("coinbase", "BTC-USD", "60000", s * 1000),
            trade("kraken", "BTC-USD", "60600", s * 1000 + 10),
        ]  # ~100 bps apart
    changes = of(run(det, events), IncidentType.PRICE_DIVERGENCE)
    assert changes and changes[0].is_open
    assert 99 < changes[0].details["bps"] < 100
    assert changes[0].opened_ms >= T0 + 10_000  # only after holding for 10 s
    converge = []
    for s in range(15, 21):
        converge += [
            trade("coinbase", "BTC-USD", "60000", s * 1000),
            trade("kraken", "BTC-USD", "60001", s * 1000 + 10),
        ]
    resolved = of(run(det, converge), IncidentType.PRICE_DIVERGENCE)
    assert resolved and not resolved[-1].is_open


def test_brief_divergence_is_ignored() -> None:
    det = Detector(["coinbase", "kraken"])
    events = [
        trade("coinbase", "ETH-USD", "2500", 0),
        trade("kraken", "ETH-USD", "2600", 10),
        trade("kraken", "ETH-USD", "2500", 3000),
        trade("coinbase", "ETH-USD", "2500", 3010),
    ]
    assert not of(run(det, events), IncidentType.PRICE_DIVERGENCE)


def test_stablecoin_divergence_names_the_likely_bad_source() -> None:
    det = Detector(["coinbase", "kraken"])
    events = []
    for s in range(12):
        events += [
            trade("coinbase", "USDT-USD", "0.9990", s * 1000),
            trade("kraken", "USDT-USD", "0.9940", s * 1000 + 10),
        ]
    (inc, *_) = of(run(det, events), IncidentType.PRICE_DIVERGENCE)
    assert inc.details["likely_source"] == "kraken"


def test_stale_prices_are_not_compared() -> None:
    det = Detector(["coinbase", "kraken"])
    events = [trade("kraken", "SOL-USD", "120", 0)]
    events += [trade("coinbase", "SOL-USD", "110", 61_000 + s * 1000) for s in range(15)]
    assert not of(run(det, events), IncidentType.PRICE_DIVERGENCE)


# ---------------------------------------------------------------- depeg
def stable_minute(
    det: Detector, symbol: str, price: str, start_s: int, sources: list[str], seconds: int = 60
) -> list[Incident]:
    events = [
        trade(src, symbol, price, (start_s + s) * 1000 + i, size="1000")
        for s in range(seconds)
        for i, src in enumerate(sources)
    ]
    return run(det, events)


def test_confirmed_depeg_escalates_and_resolves() -> None:
    det = Detector(["coinbase", "kraken"])
    both = ["coinbase", "kraken"]
    assert not of(stable_minute(det, "USDT-USD", "1.0000", 0, both), IncidentType.DEPEG)

    warn = of(stable_minute(det, "USDT-USD", "0.9900", 60, both), IncidentType.DEPEG)
    assert warn[-1].severity is Severity.WARNING and warn[-1].details["confirmed"] is True

    crit = of(stable_minute(det, "USDT-USD", "0.9700", 120, both), IncidentType.DEPEG)
    assert crit[-1].severity is Severity.CRITICAL
    assert crit[-1].details["low"].startswith("0.97")
    assert crit[-1].id == warn[0].id  # the same incident, escalated

    back = of(stable_minute(det, "USDT-USD", "1.0000", 180, both, seconds=150), IncidentType.DEPEG)
    assert back and not back[-1].is_open


def test_single_source_depeg_is_marked_unconfirmed() -> None:
    det = Detector(["kraken"])
    incs = of(stable_minute(det, "USDC-USD", "0.9800", 0, ["kraken"]), IncidentType.DEPEG)
    assert incs and incs[-1].details["confirmed"] is False


def test_one_exchange_off_peg_is_divergence_not_depeg() -> None:
    det = Detector(["coinbase", "kraken"])
    events = []
    for s in range(60):
        events += [
            trade("coinbase", "USDT-USD", "1.0000", s * 1000, size="1000"),
            trade("kraken", "USDT-USD", "0.9900", s * 1000 + 5, size="1000"),
        ]
    changes = run(det, events)
    assert not of(changes, IncidentType.DEPEG)
    assert of(changes, IncidentType.PRICE_DIVERGENCE)


def test_majors_are_never_checked_for_a_peg() -> None:
    det = Detector(["kraken"])
    assert not of(stable_minute(det, "BTC-USD", "0.5", 0, ["kraken"]), IncidentType.DEPEG)


# ---------------------------------------------------------------- outliers and latency
def test_a_bad_print_is_flagged() -> None:
    det = Detector(["kraken"])
    events = [trade("kraken", "ETH-USD", str(2500 + (i % 3)), i * 100, i) for i in range(30)]
    events.append(trade("kraken", "ETH-USD", "2575", 3100, 30))  # +3 %
    events += [
        trade("kraken", "ETH-USD", "2501", 3200, 31),
        trade("kraken", "ETH-USD", "2500", 3300, 32),
    ]
    (inc,) = of(run(det, events), IncidentType.OUTLIER_PRINT)
    assert inc.severity is Severity.INFO and inc.details["trade_id"] == 30


def test_a_real_move_is_not_a_bad_print() -> None:
    det = Detector(["kraken"])
    events = [trade("kraken", "USDC-USD", str(1 - (i % 3) / 10_000), i * 100, i) for i in range(30)]
    # the price drops 3 % and stays there: a real move, like March 2023
    events += [trade("kraken", "USDC-USD", "0.97", 3100 + i * 100, 30 + i) for i in range(10)]
    assert not of(run(det, events), IncidentType.OUTLIER_PRINT)


def test_normal_noise_is_not_an_outlier() -> None:
    det = Detector(["kraken"])
    events = [trade("kraken", "ETH-USD", str(2500 + (i % 7) * 0.5), i * 100) for i in range(80)]
    assert not of(run(det, events), IncidentType.OUTLIER_PRINT)


def test_high_latency_opens_and_resolves() -> None:
    det = Detector(["coinbase"], Thresholds(latency_p95_ms=3000))
    slow = [trade("coinbase", "BTC-USD", "1", i * 200, delay=5000) for i in range(30)]
    (inc,) = of(run(det, slow), IncidentType.HIGH_LATENCY)
    assert inc.details["p95_ms"] == 5000
    fast = [trade("coinbase", "BTC-USD", "1", 70_000 + i * 200, delay=40) for i in range(30)]
    assert not of(run(det, fast), IncidentType.HIGH_LATENCY)[-1].is_open


def test_snapshot_trades_set_the_baseline_but_are_not_latency() -> None:
    det = Detector(["coinbase"], Thresholds(latency_p95_ms=3000))
    old = Trade(
        "coinbase",
        "USDS-USD",
        900,
        Decimal("1"),
        Decimal("1"),
        "buy",
        T0 - 900_000,
        T0,
        snapshot=True,
    )
    live = [trade("coinbase", "USDS-USD", "1", i * 200, 901 + i) for i in range(30)]
    changes = run(det, [old, *live])
    assert not of(changes, IncidentType.HIGH_LATENCY)
    assert not of(changes, IncidentType.TRADE_GAP)  # 901 follows the snapshot's 900


# ---------------------------------------------------------------- health and incident hygiene
def test_health_snapshot_reports_sources_and_symbols() -> None:
    det = Detector(["coinbase", "kraken"])
    run(
        det,
        [trade("coinbase", "USDT-USD", "0.999", 0, 1), trade("kraken", "USDT-USD", "1.001", 5, 9)],
    )
    health = det.health()
    assert [s["source"] for s in health["sources"]] == ["coinbase", "kraken"]
    usdt = {s["source"]: s for s in health["symbols"] if s["symbol"] == "USDT-USD"}
    assert usdt["coinbase"]["peg_dev_pct"] == pytest.approx(-0.1)
    assert usdt["kraken"]["divergence_bps"] == pytest.approx(20.0, abs=0.1)


def test_unchanged_open_incidents_are_not_republished_every_event() -> None:
    det = Detector(["coinbase", "kraken"])
    events = []
    for s in range(25):
        events += [
            trade("coinbase", "BTC-USD", "60000", s * 1000),
            trade("kraken", "BTC-USD", "60600", s * 1000 + 10),
        ]
    published = of(run(det, events), IncidentType.PRICE_DIVERGENCE)
    assert len(published) == 1  # opened once; re-published only every 30 s
