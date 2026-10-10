"""Fault injection, replay helpers, message round-trips and the one-minute aggregation."""

from __future__ import annotations

import gzip
import json
from decimal import Decimal
from pathlib import Path

import pytest

from feedwatch.metrics import aggregate
from feedwatch.models import (
    Connection,
    Heartbeat,
    Tick,
    Trade,
    from_message,
    message_key,
    to_message,
)
from feedwatch.replay import history, replay, scenarios

ROOT = Path(__file__).parent.parent.parent


def tr(src: str, sym: str, price: str, t_ms: int, tid: int = 1) -> Trade:
    return Trade(src, sym, tid, Decimal(price), Decimal("2"), "buy", t_ms, t_ms + 30)


# ---------------------------------------------------------------- messages
@pytest.mark.parametrize(
    "event",
    [
        tr("kraken", "USDC-USD", "0.99897", 1000),
        Heartbeat("coinbase", "BTC-USD", 42, 1000, 1001),
        Heartbeat("kraken", None, None, None, 1001),
        Connection("kraken", "disconnected", 5, "timeout"),
        Tick(7),
    ],
)
def test_events_round_trip_through_json(event: object) -> None:
    msg = json.loads(json.dumps(to_message(event)))  # type: ignore[arg-type]
    assert from_message(msg) == event


def test_bad_messages_raise_value_error() -> None:
    with pytest.raises(ValueError):
        from_message({"kind": "trade", "source": "x"})
    with pytest.raises(ValueError):
        from_message({"kind": "nonsense"})


def test_message_keys_group_by_symbol() -> None:
    assert message_key(tr("kraken", "BTC-USD", "1", 0)) == "BTC-USD"
    assert message_key(tr("coinbase", "BTC-USD", "1", 0)) == "BTC-USD"
    assert message_key(Tick(1)) == "_clock"
    assert message_key(Connection("kraken", "connected", 1)) == "_kraken"


# ---------------------------------------------------------------- scenarios
def test_every_shipped_scenario_loads() -> None:
    files = sorted((ROOT / "scenarios").glob("*.json"))
    assert len(files) >= 6
    for path in files:
        sc = scenarios.load(path)
        assert sc.name and sc.faults


def test_silence_drops_every_message_from_the_source_in_the_window() -> None:
    events = [Heartbeat("kraken", None, None, None, s * 1000) for s in range(10)]
    events += [Heartbeat("coinbase", "BTC-USD", 1, s * 1000, s * 1000) for s in range(10)]
    events.sort(key=lambda e: e.ingest_ms)
    fault = scenarios.Fault("silence", "kraken", 3, 6)
    out = list(scenarios.inject(events, [fault]))
    kraken = [e.ingest_ms for e in out if e.source == "kraken"]  # type: ignore[union-attr]
    assert kraken == [0, 1000, 2000, 6000, 7000, 8000, 9000]
    assert sum(e.source == "coinbase" for e in out) == 10  # type: ignore[union-attr]


def test_shift_spike_drop_and_delay() -> None:
    events = [tr("kraken", "ETH-USD", "100", s * 1000, s) for s in range(6)]
    shifted = list(
        scenarios.inject(
            events, [scenarios.Fault("shift_price", "kraken", 2, 4, "ETH-USD", pct=1.5)]
        )
    )
    assert [e.price for e in shifted] == [
        Decimal(p)
        for p in ["100", "100", "101.5", "101.5", "100", "100"]  # type: ignore[union-attr]
    ]
    spiked = list(scenarios.inject(events, [scenarios.Fault("spike", "kraken", 1, 9, pct=-5)]))
    assert [e.price for e in spiked].count(Decimal("95")) == 1  # type: ignore[union-attr]
    dropped = list(scenarios.inject(events, [scenarios.Fault("drop_trades", "kraken", 0, 3)]))
    assert [e.trade_id for e in dropped] == [3, 4, 5]  # type: ignore[union-attr]
    delayed = list(scenarios.inject(events, [scenarios.Fault("delay", "kraken", 0, 2, ms=6000)]))
    assert delayed[0].ingest_ms - delayed[0].event_ms == 6030  # type: ignore[union-attr]


def test_other_symbols_are_untouched() -> None:
    events = [tr("kraken", "BTC-USD", "100", 1000)]
    out = list(
        scenarios.inject(
            events, [scenarios.Fault("shift_price", "kraken", 0, 9, "ETH-USD", pct=50)]
        )
    )
    assert out == events


# ---------------------------------------------------------------- replay helpers
def test_ticks_fill_quiet_periods() -> None:
    events = [tr("kraken", "BTC-USD", "1", 0), tr("kraken", "BTC-USD", "1", 3500)]
    out = list(replay.with_ticks(events))
    assert [e.ingest_ms for e in out if isinstance(e, Tick)] == [1030, 2030, 3030]


def test_rebase_moves_all_timestamps_together() -> None:
    events = [tr("kraken", "BTC-USD", "1", 1000), tr("kraken", "BTC-USD", "1", 4000)]
    out = list(replay.rebase(events, start_ms=1_000_000))
    assert [e.ingest_ms for e in out] == [1_000_000, 1_003_000]
    assert [e.event_ms for e in out] == [999_970, 1_002_970]  # type: ignore[union-attr]


def test_recordings_are_parsed_and_merged_in_time_order(tmp_path: Path) -> None:
    (tmp_path / "coinbase" / "2026-10-10").mkdir(parents=True)
    (tmp_path / "kraken" / "2026-10-10").mkdir(parents=True)
    cb = (
        '{"type":"match","trade_id":5,"side":"buy","size":"1","price":"60000",'
        '"product_id":"BTC-USD","time":"2026-10-10T18:00:00Z"}'
    )
    with gzip.open(tmp_path / "coinbase/2026-10-10/18.jsonl.gz", "wt") as f:
        f.write(json.dumps({"ingest_ms": 2000, "raw": cb}) + "\n")
    with gzip.open(tmp_path / "kraken/2026-10-10/18.jsonl.gz", "wt") as f:
        f.write(json.dumps({"ingest_ms": 1000, "raw": '{"channel":"heartbeat"}'}) + "\n")
    events = list(replay.read_recording(tmp_path))
    assert [e.source for e in events] == ["kraken", "coinbase"]  # type: ignore[union-attr]


def test_history_files_round_trip(tmp_path: Path) -> None:
    rows = iter(
        [
            {
                "trade_id": "7",
                "time_ms": "1678478415150",
                "price": "0.9999",
                "volume": "30.5",
                "side": "sell",
            }
        ]
    )
    path = tmp_path / "h.csv.gz"
    assert history.save(rows, path) == 1
    (trade,) = history.load(path, "USDC-USD")
    assert trade.price == Decimal("0.9999") and trade.ingest_ms == trade.event_ms


# ---------------------------------------------------------------- one-minute metrics
def test_minute_aggregation_handles_out_of_order_trades() -> None:
    msgs = [
        to_message(tr("coinbase", "BTC-USD", p, t))
        for p, t in [("100", 10_000), ("103", 20_000), ("99", 5_000), ("101", 50_000)]
    ]
    agg = aggregate.init(msgs[0])
    for m in msgs[1:]:
        agg = aggregate.reduce(agg, m)
    row = aggregate.finish(agg, window_start_ms=0)
    assert row["trades"] == 4
    assert (row["open"], row["close"]) == ("99", "101")  # earliest and latest by event time
    assert (row["high"], row["low"]) == ("103", "99")
    assert row["vwap"] == "100.75" and row["volume"] == "8"
    assert row["latency_p95_ms"] == 30
    assert row["bucket"] == "1970-01-01T00:00:00+00:00"


def test_latency_samples_are_capped() -> None:
    first = to_message(tr("kraken", "BTC-USD", "1", 0))
    agg = aggregate.init(first)
    for i in range(2000):
        agg = aggregate.reduce(agg, to_message(tr("kraken", "BTC-USD", "1", i)))
    assert len(agg["latency"]) == aggregate.LATENCY_SAMPLES and agg["trades"] == 2001
