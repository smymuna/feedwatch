"""Regression test on real data: Kraken's USDC/USD trades from 10 to 13 March 2023, when
USDC lost its peg after Silicon Valley Bank failed (269,430 trades, samples/history/).

Whatever we change in the rules, this event must still produce exactly one early warning
and one depeg that escalates to critical, with the timeline below."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from feedwatch.detect.detector import Detector
from feedwatch.models import Incident, IncidentType, Severity
from feedwatch.replay import history, replay

FILE = Path(__file__).parents[2] / "samples/history/kraken_USDCUSD_2023-03-10_2023-03-13.csv.gz"


def utc(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, UTC)


@pytest.fixture(scope="module")
def updates() -> list[Incident]:
    det = Detector(["kraken"], feed_rules=False)
    out: list[Incident] = []
    for event in replay.with_ticks(history.load(FILE, "USDC-USD")):
        out += det.process(event)
    assert not det.open
    return out


def test_the_event_is_two_depeg_incidents_and_nothing_else(updates: list[Incident]) -> None:
    latest = {u.id: u for u in updates}
    assert [i.type for i in latest.values()] == [IncidentType.DEPEG, IncidentType.DEPEG]


def test_early_warning_on_friday_night(updates: list[Incident]) -> None:
    first = updates[0]
    assert first.type is IncidentType.DEPEG and first.severity is Severity.WARNING
    assert (
        datetime(2023, 3, 10, 21, 30, tzinfo=UTC)
        < utc(first.opened_ms)
        < datetime(2023, 3, 10, 22, 30, tzinfo=UTC)
    )


def test_the_depeg_goes_critical_and_recovers_after_the_backstop(updates: list[Incident]) -> None:
    main = [u for u in updates if u.id == updates[-1].id]
    critical = [u for u in main if u.severity is Severity.CRITICAL]
    assert critical and utc(critical[0].updated_ms) < datetime(2023, 3, 11, 4, tzinfo=UTC)
    assert Decimal(main[-1].details["low"]) < Decimal("0.90")  # 60 s VWAP low
    end = main[-1]
    assert not end.is_open
    assert (
        datetime(2023, 3, 13, 6, tzinfo=UTC)
        < utc(end.resolved_ms or 0)
        < datetime(2023, 3, 13, 18, tzinfo=UTC)
    )
