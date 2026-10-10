"""Read API for the dashboard (and anyone else): feed health, peg status, incidents,
minute metrics, per-coin pages, and Server-Sent Events streams.

History comes from PostgreSQL. The coin pages also follow Kafka directly (``live.py``) for
trade-by-trade updates and the candle that is still forming. Nothing here writes, so the
API can be scaled or restarted without touching the pipeline. Interactive docs at /docs.
"""

from __future__ import annotations

import asyncio
import json
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from decimal import Decimal
from importlib import resources
from pathlib import Path
from typing import Any, Literal

import psycopg
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from feedwatch import __version__
from feedwatch.api.live import LiveFeeder, LiveMarket
from feedwatch.config import get_settings
from feedwatch.models import STABLECOINS, base_asset, is_stablecoin
from feedwatch.sources.kraken import canonical

STREAM_EVERY_S = 1.0


def _encode(row: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = jsonable_encoder(
        row, custom_encoder={Decimal: lambda d: format(d.normalize(), "f")}
    )
    return out


async def ensure_schema(url: str, attempts: int = 30) -> None:
    """Create the tables if the store service hasn't yet, so a fresh start never serves
    errors. The schema is idempotent, so it's safe for both services to apply it."""
    for attempt in range(attempts):
        try:
            async with await psycopg.AsyncConnection.connect(url) as conn:
                await conn.execute(
                    resources.files("feedwatch.store").joinpath("schema.sql").read_text().encode()
                )
                await conn.commit()
            return
        except (psycopg.OperationalError, psycopg.errors.UniqueViolation):
            # the database is still starting, or the store service is creating the same table
            if attempt == attempts - 1:
                raise
            await asyncio.sleep(1)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    await ensure_schema(get_settings().database_url)
    pool = AsyncConnectionPool(
        get_settings().database_url,
        min_size=1,
        max_size=5,
        open=False,
        kwargs={"row_factory": dict_row},
    )
    await pool.open(wait=False)
    app.state.pool = pool
    app.state.market = LiveMarket()
    feeder = None
    if os.environ.get("FEEDWATCH_API_LIVE", "1") != "0":
        feeder = LiveFeeder(get_settings(), app.state.market)
        feeder.start()
    app.state.feeder = feeder
    yield
    if feeder:
        feeder.stop()
    await pool.close()


app = FastAPI(
    title="FeedWatch API",
    version=__version__,
    lifespan=lifespan,
    description="Live data quality for crypto market data feeds.",
)


async def query(
    request: Request, sql: str, params: dict[str, Any] | None = None
) -> list[dict[str, Any]]:
    pool: AsyncConnectionPool[Any] = request.app.state.pool
    async with pool.connection() as conn, conn.cursor() as cur:
        await cur.execute(sql, params or {})
        rows = await cur.fetchall()
    return [_encode(r) for r in rows]


@app.get("/api/health")
async def health(request: Request) -> dict[str, Any]:
    try:
        rows = await query(request, "SELECT max(updated_at) AS last_update FROM feed_health")
    except Exception as exc:  # the database is the only dependency
        raise HTTPException(503, f"database unavailable: {exc}") from exc
    return {"status": "ok", "version": __version__, "last_update": rows[0]["last_update"]}


@app.get("/api/feeds")
async def feeds(request: Request) -> list[dict[str, Any]]:
    """One row per exchange: connected, message rate, latency, gaps."""
    return await query(request, "SELECT * FROM feed_health ORDER BY source")


@app.get("/api/symbols")
async def symbols(request: Request) -> list[dict[str, Any]]:
    """Latest price, 60 s VWAP, peg deviation and cross-exchange divergence per feed."""
    return await query(request, "SELECT * FROM symbol_health ORDER BY symbol, source")


@app.get("/api/incidents")
async def incidents(
    request: Request,
    status: Literal["open", "resolved", "all"] = "all",
    type: str | None = None,
    symbol: str | None = None,
    limit: int = Query(50, ge=1, le=500),
) -> list[dict[str, Any]]:
    sql = "SELECT * FROM incidents WHERE (%(status)s = 'all' OR status = %(status)s)"
    if type:
        sql += " AND type = %(type)s"
    if symbol:
        sql += " AND symbol = %(symbol)s"
    sql += " ORDER BY (status = 'open') DESC, updated_at DESC LIMIT %(limit)s"
    return await query(
        request, sql, {"status": status, "type": type, "symbol": symbol, "limit": limit}
    )


@app.get("/api/metrics")
async def metrics(
    request: Request,
    symbol: str,
    source: str | None = None,
    minutes: int = Query(60, ge=1, le=7 * 24 * 60),
) -> list[dict[str, Any]]:
    """One-minute OHLCV, VWAP and latency, oldest first."""
    sql = """SELECT * FROM metrics_1m WHERE symbol = %(symbol)s
             AND (%(source)s::text IS NULL OR source = %(source)s)
             AND bucket >= now() - make_interval(mins => %(minutes)s)
             ORDER BY bucket"""
    return await query(request, sql, {"symbol": symbol, "source": source, "minutes": minutes})


async def snapshot(request: Request) -> dict[str, Any]:
    feeds_, symbols_, open_ = await asyncio.gather(
        query(request, "SELECT * FROM feed_health ORDER BY source"),
        query(request, "SELECT * FROM symbol_health ORDER BY symbol, source"),
        query(request, "SELECT * FROM incidents WHERE status = 'open' ORDER BY opened_at DESC"),
    )
    return {"feeds": feeds_, "symbols": symbols_, "open_incidents": open_}


@app.get("/api/stream")
async def stream(request: Request) -> StreamingResponse:
    """Server-Sent Events: a ``snapshot`` every second, plus ``incident`` events whenever an
    incident opens, changes or resolves."""

    async def events() -> AsyncIterator[str]:
        last_seen: str | None = datetime.now(UTC).isoformat()  # only changes from now on
        while not await request.is_disconnected():
            snap = await snapshot(request)
            yield f"event: snapshot\ndata: {json.dumps(snap)}\n\n"
            changed = await query(
                request,
                "SELECT * FROM incidents WHERE %(since)s::timestamptz IS NULL "
                "OR updated_at > %(since)s ORDER BY updated_at LIMIT 100",
                {"since": last_seen},
            )
            if changed:
                last_seen = changed[-1]["updated_at"]
                if last_seen is not None:
                    for inc in changed:
                        yield f"event: incident\ndata: {json.dumps(inc)}\n\n"
            await asyncio.sleep(STREAM_EVERY_S)

    headers = {"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
    return StreamingResponse(events(), media_type="text/event-stream", headers=headers)


# ---------------------------------------------------------------- coins
NAMES = {
    "BTC": "Bitcoin",
    "ETH": "Ether",
    "SOL": "Solana",
    "USDT": "Tether",
    "USDC": "USD Coin",
    "DAI": "Dai",
    "PYUSD": "PayPal USD",
    "USDS": "Sky Dollar",
}


def coin_sources() -> dict[str, list[str]]:
    """Every symbol we follow, and which exchanges list it."""
    s = get_settings()
    out: dict[str, list[str]] = {}
    for product in s.coinbase_products:
        out.setdefault(product, []).append("coinbase")
    for symbol in s.kraken_symbols:
        out.setdefault(canonical(symbol), []).append("kraken")
    return dict(sorted(out.items(), key=lambda kv: (base_asset(kv[0]) in STABLECOINS, kv[0])))


def known(symbol: str) -> str:
    if symbol not in coin_sources():
        raise HTTPException(404, f"unknown symbol {symbol}")
    return symbol


@app.get("/api/coins")
async def coins(request: Request) -> list[dict[str, Any]]:
    """Every coin with its live price per exchange, change over the last hour, a sparkline
    and its open incidents. Drives the overview cards."""
    market: LiveMarket = request.app.state.market
    rows, open_ = await asyncio.gather(
        query(
            request,
            """SELECT symbol, source, bucket, open, close, vwap FROM metrics_1m
                          WHERE bucket >= now() - interval '60 minutes' ORDER BY bucket""",
        ),
        query(request, "SELECT symbol, severity FROM incidents WHERE status = 'open'"),
    )
    out = []
    for symbol, sources in coin_sources().items():
        mine = [r for r in rows if r["symbol"] == symbol]
        main = next((s for s in sources if any(r["source"] == s for r in mine)), sources[0])
        series = [r for r in mine if r["source"] == main]
        change = None
        if len(series) >= 2 and Decimal(series[0]["open"]) > 0:
            change = float((Decimal(series[-1]["close"]) / Decimal(series[0]["open"]) - 1) * 100)
        out.append(
            {
                "symbol": symbol,
                "base": base_asset(symbol),
                "name": NAMES.get(base_asset(symbol), ""),
                "stablecoin": is_stablecoin(symbol),
                "sources": sources,
                "prices": market.last_prices(symbol),
                "change_60m_pct": change,
                "sparkline": [float(r["vwap"]) for r in series],
                "sparkline_source": main,
                "open_incidents": [i["severity"] for i in open_ if i["symbol"] == symbol],
            }
        )
    return out


@app.get("/api/coins/{symbol}")
async def coin(request: Request, symbol: str) -> dict[str, Any]:
    """One coin: per-exchange health, the latest minute's metrics, and its incidents."""
    known(symbol)
    health, latest, incidents_ = await asyncio.gather(
        query(
            request,
            "SELECT * FROM symbol_health WHERE symbol = %(s)s ORDER BY source",
            {"s": symbol},
        ),
        query(
            request,
            """SELECT DISTINCT ON (source) * FROM metrics_1m WHERE symbol = %(s)s
                          ORDER BY source, bucket DESC""",
            {"s": symbol},
        ),
        query(
            request,
            """SELECT * FROM incidents WHERE symbol = %(s)s
                          ORDER BY (status = 'open') DESC, updated_at DESC LIMIT 30""",
            {"s": symbol},
        ),
    )
    feeds = await query(request, "SELECT * FROM feed_health ORDER BY source")
    return {
        "symbol": symbol,
        "base": base_asset(symbol),
        "name": NAMES.get(base_asset(symbol), ""),
        "stablecoin": is_stablecoin(symbol),
        "sources": coin_sources()[symbol],
        "health": health,
        "latest_minute": latest,
        "incidents": incidents_,
        "feeds": feeds,
    }


@app.get("/api/coins/{symbol}/candles")
async def candles(
    request: Request, symbol: str, minutes: int = Query(120, ge=5, le=24 * 60)
) -> dict[str, Any]:
    """Finished one-minute candles from the database, plus the last few minutes (including
    the one still forming) from the live feed. The live ones fill the gap until the metrics
    job writes a minute, about a minute after it ends."""
    known(symbol)
    stored = await query(
        request,
        """SELECT bucket, source, open, high, low, close, volume, trades, vwap,
                  latency_p50_ms, latency_p95_ms
           FROM metrics_1m WHERE symbol = %(s)s
           AND bucket >= now() - make_interval(mins => %(m)s) ORDER BY bucket""",
        {"s": symbol, "m": minutes},
    )
    market: LiveMarket = request.app.state.market
    return {"symbol": symbol, "stored": stored, "live": market.candles(symbol)}


@app.get("/api/coins/{symbol}/stream")
async def coin_stream(request: Request, symbol: str) -> StreamingResponse:
    """Server-Sent Events for one coin, twice a second: new trades, the live candles, and
    each exchange's latest price. Every few seconds also the per-exchange health."""
    known(symbol)
    market: LiveMarket = request.app.state.market

    async def events() -> AsyncIterator[str]:
        seq = max(0, market.seq - 50)  # start with the latest 50 trades
        tick = 0
        while not await request.is_disconnected():
            new = market.trades(symbol, since_seq=seq, limit=200)
            if new:
                seq = new[-1]["seq"]
            feeder = request.app.state.feeder
            payload: dict[str, Any] = {
                "trades": new,
                "candles": market.candles(symbol),
                "prices": market.last_prices(symbol),
                "live": bool(feeder and feeder.connected),
            }
            if tick % 6 == 0:
                payload["health"] = await query(
                    request,
                    "SELECT * FROM symbol_health WHERE symbol = %(s)s ORDER BY source",
                    {"s": symbol},
                )
            yield f"event: update\ndata: {json.dumps(payload)}\n\n"
            tick += 1
            await asyncio.sleep(0.5)

    headers = {"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
    return StreamingResponse(events(), media_type="text/event-stream", headers=headers)


# The built dashboard, if present, is served from the same origin as the API.
_dist = Path(os.environ.get("FEEDWATCH_DASHBOARD_DIR", "dashboard/dist"))
if _dist.is_dir():
    app.mount("/", StaticFiles(directory=_dist, html=True), name="dashboard")
