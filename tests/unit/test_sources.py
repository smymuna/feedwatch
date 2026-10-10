"""Parsers, tested on 25 seconds of real messages captured from both exchanges
(tests/fixtures/*_live.jsonl, 10 Oct 2026)."""

from __future__ import annotations

import itertools
from decimal import Decimal
from pathlib import Path

import pytest

from feedwatch.models import Connection, Heartbeat, Trade
from feedwatch.sources.base import iso_to_ms
from feedwatch.sources.coinbase import Coinbase
from feedwatch.sources.kraken import Kraken, canonical

FIXTURES = Path(__file__).parent.parent / "fixtures"


def lines(name: str) -> list[str]:
    return (FIXTURES / name).read_text().splitlines()


def test_iso_to_ms_is_exact() -> None:
    assert iso_to_ms("2026-10-10T18:48:45.197352Z") == 1791658125197
    assert iso_to_ms("1970-01-01T00:00:01Z") == 1000


def test_coinbase_session_parses_into_trades_heartbeats_and_subscription() -> None:
    cb = Coinbase(products=["BTC-USD"])
    events = [e for raw in lines("coinbase_live.jsonl") for e in cb.parse(raw, ingest_ms=1)]
    trades = [e for e in events if isinstance(e, Trade)]
    beats = [e for e in events if isinstance(e, Heartbeat)]
    assert len(trades) == 114  # 109 matches + 5 last_match snapshots
    assert len(beats) == 125
    assert any(isinstance(e, Connection) and e.state == "subscribed" for e in events)
    assert {t.symbol for t in trades} >= {"BTC-USD", "ETH-USD", "USDT-USD"}
    assert all(t.price > 0 and t.size > 0 and t.trade_id for t in trades)


def test_coinbase_reports_the_taker_side() -> None:
    raw = (
        '{"type":"match","trade_id":7,"side":"buy","size":"1.5","price":"0.99897",'
        '"product_id":"USDT-USD","sequence":1,"time":"2026-10-10T18:48:45.197352Z"}'
    )
    (trade,) = Coinbase(products=[]).parse(raw, ingest_ms=5)
    assert isinstance(trade, Trade)
    assert trade.side == "sell"  # the maker bought, so the taker sold
    assert trade.price == Decimal("0.99897")
    assert trade.event_ms == 1791658125197 and trade.ingest_ms == 5


def test_coinbase_trade_ids_are_consecutive_within_a_product() -> None:
    cb = Coinbase(products=[])
    ids: dict[str, list[int]] = {}
    for raw in lines("coinbase_live.jsonl"):
        for e in cb.parse(raw, ingest_ms=0):
            if isinstance(e, Trade) and e.trade_id is not None:
                ids.setdefault(e.symbol, []).append(e.trade_id)
    btc = ids["BTC-USD"]
    assert btc == sorted(btc)
    assert all(b - a == 1 for a, b in itertools.pairwise(btc[1:]))


def test_coinbase_error_becomes_a_connection_event() -> None:
    (event,) = Coinbase(products=[]).parse('{"type":"error","message":"Failed to subscribe"}', 1)
    assert isinstance(event, Connection) and event.state == "error"


def test_kraken_session_parses_trades_heartbeats_and_subscriptions() -> None:
    kr = Kraken(symbols=["BTC/USD"])
    events = [e for raw in lines("kraken_live.jsonl") for e in kr.parse(raw, ingest_ms=1)]
    trades = [e for e in events if isinstance(e, Trade)]
    assert trades and all("/" not in t.symbol for t in trades)
    assert sum(isinstance(e, Heartbeat) for e in events) == 25
    assert sum(isinstance(e, Connection) and e.state == "subscribed" for e in events) == 7


def test_kraken_prices_never_pass_through_float() -> None:
    raw = (
        '{"channel":"trade","type":"update","data":[{"symbol":"USDT/USD","side":"sell",'
        '"price":0.99897,"qty":20.00000000,"ord_type":"limit","trade_id":84436363,'
        '"timestamp":"2026-10-10T18:48:56.087365Z"}]}'
    )
    (trade,) = Kraken(symbols=[]).parse(raw, ingest_ms=1)
    assert isinstance(trade, Trade)
    assert trade.price == Decimal("0.99897") and str(trade.price) == "0.99897"
    assert trade.symbol == "USDT-USD" and trade.trade_id == 84436363


def test_kraken_failed_subscription_is_an_error() -> None:
    raw = '{"method":"subscribe","success":false,"error":"Currency pair not supported XYZ/USD"}'
    (event,) = Kraken(symbols=[]).parse(raw, 1)
    assert isinstance(event, Connection) and event.state == "error"
    assert "not supported" in event.detail


@pytest.mark.parametrize("raw", ['{"type":"unknown"}', '{"channel":"book"}'])
def test_unknown_messages_are_ignored(raw: str) -> None:
    assert Coinbase(products=[]).parse(raw, 1) == []
    assert Kraken(symbols=[]).parse(raw, 1) == []


def test_canonical_symbol() -> None:
    assert canonical("PYUSD/USD") == "PYUSD-USD"
