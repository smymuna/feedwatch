"""Read API for the dashboard (and anyone else): feed health, peg status, incidents,
minute metrics, and a Server-Sent Events stream that pushes a fresh snapshot every second.

It only reads from PostgreSQL, so it can be scaled or restarted without touching the
pipeline. Interactive docs at /docs.
"""

from __future__ import annotations

import asyncio
import json
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Literal

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from feedwatch import __version__
from feedwatch.config import get_settings

STREAM_EVERY_S = 1.0


def _encode(row: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = jsonable_encoder(
        row, custom_encoder={Decimal: lambda d: format(d.normalize(), "f")}
    )
    return out


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    pool = AsyncConnectionPool(
        get_settings().database_url,
        min_size=1,
        max_size=5,
        open=False,
        kwargs={"row_factory": dict_row},
    )
    await pool.open(wait=False)
    app.state.pool = pool
    yield
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
    limit: int = Query(50, ge=1, le=500),
) -> list[dict[str, Any]]:
    sql = "SELECT * FROM incidents WHERE (%(status)s = 'all' OR status = %(status)s)"
    if type:
        sql += " AND type = %(type)s"
    sql += " ORDER BY (status = 'open') DESC, updated_at DESC LIMIT %(limit)s"
    return await query(request, sql, {"status": status, "type": type, "limit": limit})


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


# The built dashboard, if present, is served from the same origin as the API.
_dist = Path(os.environ.get("FEEDWATCH_DASHBOARD_DIR", "dashboard/dist"))
if _dist.is_dir():
    app.mount("/", StaticFiles(directory=_dist, html=True), name="dashboard")
