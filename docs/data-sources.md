# Data sources

Both are public, free, and need no account or API key. Real captured messages from both
are in `tests/fixtures/` and drive the parser tests.

## Coinbase Exchange WebSocket

- Endpoint: `wss://ws-feed.exchange.coinbase.com`
  ([docs](https://docs.cdp.coinbase.com/exchange/websocket-feed/overview))
- Subscribe: `{"type": "subscribe", "product_ids": [...], "channels": ["matches", "heartbeat"]}`
- Products: `BTC-USD`, `ETH-USD`, `SOL-USD`, `USDT-USD`, `USDS-USD`. (Coinbase does not
  list USDC/USD, since USD balances there settle in USDC; DAI, PYUSD and EURC USD pairs are
  delisted.)

| Message | Becomes | Notes |
|---|---|---|
| `match` | `trade` | `trade_id` is consecutive per product; `side` is the **maker's** side, so we store the opposite (the taker's), matching Kraken |
| `last_match` | `trade` with `snapshot=true` | the latest trade, sent once on subscribe. It sets the sequence baseline but is not counted as live data (it can be minutes old) |
| `heartbeat` | `heartbeat` | every second per product, with `last_trade_id`: if it runs ahead of the last trade we received for two heartbeats, trades were lost |
| `subscriptions`, `error` | `connection` | |

```json
{"type":"match","trade_id":1104565098,"maker_order_id":"8eb3496f-…","taker_order_id":"4aa91631-…",
 "side":"buy","size":"0.00012173","price":"82966.79","product_id":"BTC-USD",
 "sequence":137524637885,"time":"2026-10-10T18:48:55.233685Z"}
```

## Kraken WebSocket API v2

- Endpoint: `wss://ws.kraken.com/v2` ([docs](https://docs.kraken.com/api/docs/websocket-v2/trade))
- Subscribe: `{"method": "subscribe", "params": {"channel": "trade", "symbol": [...], "snapshot": false}}`
- Symbols: `BTC/USD`, `ETH/USD`, `SOL/USD`, `USDT/USD`, `USDC/USD`, `DAI/USD`, `PYUSD/USD`,
  `USDS/USD` (normalised to `BTC-USD` etc.)

| Message | Becomes | Notes |
|---|---|---|
| `trade` (`update`) | `trade` | prices are JSON **numbers**: parsed straight to `Decimal`, never via float; `trade_id` is consecutive per symbol |
| `heartbeat` | `heartbeat` | per connection, no time or trade id: proves only that the connection is alive |
| `status`, `subscribe` replies | `connection` | a failed subscription becomes an `error` with Kraken's message |

```json
{"channel":"trade","type":"update","data":[{"symbol":"USDT/USD","side":"sell",
 "price":0.99897,"qty":20.00000000,"ord_type":"limit","trade_id":84436363,
 "timestamp":"2026-10-10T18:48:56.087365Z"}]}
```

## Kraken REST trade history

- `GET https://api.kraken.com/0/public/Trades?pair=USDCUSD&since=<nanoseconds>&count=1000`
- Full history, 1,000 trades per call, with a cursor for the next page. Same `trade_id`s as
  the live feed.
- `feedwatch history` pages through it with a pause between calls and backs off when Kraken
  says "Too many requests".
- `samples/history/kraken_USDCUSD_2023-03-10_2023-03-13.csv.gz`: 269,430 trades, from
  10 March 2023 12:00 UTC to 13 March 18:00 UTC.

## Why not Binance

Binance is the largest exchange, but it refuses connections from the United States and
from many cloud providers (HTTP 451, "restricted location"). The author is in the US and
GitHub's CI runners are too, so a Binance-based pipeline would fail for its own developer,
its CI, and possibly a reviewer. Coinbase and Kraken are reachable from the US and the EU.

## The sample session

`samples/session/`: five minutes recorded live from both exchanges on 10 October 2026,
19:01:36–19:06:36 UTC, exactly as received (3,779 events: 1,970 trades, 1,799 heartbeats, 10
connection events). With no faults injected it produces **no incidents**; the scenarios in
`scenarios/` each break it in one specific way.
