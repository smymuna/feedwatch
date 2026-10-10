"""The API's live buffer: recent trades and forming candles per coin."""

from __future__ import annotations

from decimal import Decimal

from feedwatch.api.app import coin_sources
from feedwatch.api.live import LiveMarket
from feedwatch.models import Heartbeat, Trade, to_message

T0 = 1_791_658_200_000  # a whole minute


def msg(
    price: str,
    t: int,
    source: str = "coinbase",
    symbol: str = "BTC-USD",
    snapshot: bool = False,
    size: str = "1",
) -> dict[str, object]:
    return to_message(
        Trade(
            source,
            symbol,
            t,
            Decimal(price),
            Decimal(size),
            "buy",
            T0 + t,
            T0 + t + 20,
            snapshot=snapshot,
        )
    )


def test_candles_form_per_minute_and_source() -> None:
    m = LiveMarket()
    for price, t in [("100", 1_000), ("105", 20_000), ("98", 30_000), ("101", 50_000)]:
        m.add(msg(price, t))
    m.add(msg("102", 61_000))  # next minute
    m.add(msg("100.5", 5_000, source="kraken"))
    first, kraken, second = m.candles("BTC-USD")
    assert (first["open"], first["high"], first["low"], first["close"]) == (
        "100",
        "105",
        "98",
        "101",
    )
    assert first["trades"] == 4 and first["volume"] == "4" and first["minute_ms"] == T0
    assert kraken["source"] == "kraken" and kraken["close"] == "100.5"
    assert second["minute_ms"] == T0 + 60_000 and second["open"] == "102"


def test_out_of_order_trades_keep_open_and_close_right() -> None:
    m = LiveMarket()
    m.add(msg("101", 30_000))
    m.add(msg("100", 10_000))  # arrives late but happened first
    (c,) = m.candles("BTC-USD")
    assert (c["open"], c["close"]) == ("100", "101")


def test_old_minutes_are_dropped() -> None:
    m = LiveMarket(keep_minutes=3)
    for minute in range(6):
        m.add(msg("100", minute * 60_000))
    assert [c["minute_ms"] for c in m.candles("BTC-USD")] == [T0 + k * 60_000 for k in range(2, 6)]


def test_trades_since_a_sequence_number() -> None:
    m = LiveMarket(max_trades=3)
    for i in range(5):
        m.add(msg(str(100 + i), i * 1000))
    assert [t["price"] for t in m.trades("BTC-USD")] == ["102", "103", "104"]  # capped
    assert [t["price"] for t in m.trades("BTC-USD", since_seq=4)] == ["104"]
    assert m.last_prices("BTC-USD")["coinbase"]["price"] == "104"


def test_only_live_trades_are_kept() -> None:
    m = LiveMarket()
    m.add(msg("100", 0, snapshot=True))
    m.add(to_message(Heartbeat("coinbase", "BTC-USD", 5, T0, T0)))
    assert m.trades("BTC-USD") == [] and m.candles("BTC-USD") == []


def test_coin_list_merges_both_exchanges_and_lists_stablecoins_last() -> None:
    coins = coin_sources()
    assert coins["BTC-USD"] == ["coinbase", "kraken"]
    assert coins["USDC-USD"] == ["kraken"]
    assert list(coins)[:3] == ["BTC-USD", "ETH-USD", "SOL-USD"]
