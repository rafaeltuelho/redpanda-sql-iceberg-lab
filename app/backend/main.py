"""NYC Taxi Time Machine API. Every query endpoint returns {data, queries} so the UI can
show the SQL that produced each view, with its latency and row count."""
import asyncio
import json
import logging
import os
import time
from collections import OrderedDict
from contextlib import asynccontextmanager
from datetime import timedelta

from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from . import queries
from .config import settings
from .db import pool, query
from .live import poller
from .timeutil import from_ms, ms, ny_now, to_history

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
HERE = os.path.dirname(os.path.abspath(__file__))
WEB_DIST = os.environ.get("WEB_DIST", os.path.join(HERE, "..", "..", "web", "dist"))
ZONES = json.load(open(os.path.join(HERE, "zones.json")))

MAX_RANGE = timedelta(days=100)


@asynccontextmanager
async def lifespan(_: FastAPI):
    await pool.open(wait=False)  # start even if Redpanda SQL is unreachable
    yield
    await pool.close()


app = FastAPI(title="NYC Taxi Time Machine", lifespan=lifespan)

# TTL + LRU cache for query endpoints. Concurrent misses for the same key share one fetch,
# and a hit marks its queries as cached so the SQL log doesn't report the old latency as new.
CACHE_MAX = 256
_cache: OrderedDict[tuple, tuple[float, dict]] = OrderedDict()
_inflight: dict[tuple, asyncio.Task] = {}


async def cached(key: tuple, ttl_s: float, fn):
    hit = _cache.get(key)
    if hit and time.monotonic() - hit[0] < ttl_s:
        _cache.move_to_end(key)
        return {**hit[1], "queries": [{**q, "cached": True} for q in hit[1].get("queries", [])]}
    task = _inflight.get(key)
    if task is None:
        task = _inflight[key] = asyncio.create_task(fn())
        task.add_done_callback(lambda _: _inflight.pop(key, None))
    value = await asyncio.shield(task)
    _cache[key] = (time.monotonic(), value)
    _cache.move_to_end(key)
    while len(_cache) > CACHE_MAX:
        _cache.popitem(last=False)
    return value


def time_range(from_: int, to: int):
    start, end = from_ms(from_), from_ms(to)
    if not start < end or end - start > MAX_RANGE:
        raise HTTPException(400, "invalid time range")
    return start, end


async def run(name: str, sql: str, params: dict | None = None):
    try:
        return await query(name, sql, params)
    except Exception as e:
        raise HTTPException(502, f"{name}: {str(e).strip()}") from e


@app.get("/api/health")
async def health():
    return {"ok": True}


@app.get("/api/meta")
async def meta():
    return {
        "history_from": ms(settings.history_from),
        "history_to": ms(settings.history_to),
        "ny_now": ms(ny_now()),
        "iceberg_lag_ms": settings.iceberg_lag_ms,
        "replay_weeks": settings.replay_weeks,
        "tables": {"trips": queries.T, "quarantine": queries.Q, "weather": queries.W},
    }


@app.get("/api/zones")
async def zones():
    return ZONES


@app.get("/api/volume")
async def volume(from_: int = Query(alias="from"), to: int = Query(), bucket_s: int = Query(3600, ge=60)):
    start, end = time_range(from_, to)

    async def fetch():
        rows, info = await run("timeline volume", queries.VOLUME,
                               {"from": start, "to": end, "bucket_us": bucket_s * 1_000_000})
        return {"data": [list(r) for r in rows], "queries": [info]}
    return await cached(("volume", from_, to, bucket_s), 600, fetch)


@app.get("/api/chunk")
async def chunk(response: Response, from_: int = Query(alias="from"), to: int = Query(),
                bucket_s: int = Query(60, ge=10), limit: int = Query(6000, le=20000),
                sample: int = Query(1, ge=1, le=60)):
    """Map frames (counts per bucket and zone) plus trips to animate, for one playback chunk.
    History doesn't change once the backfill is done, so chunks are cached for 30 min on the
    server and 10 min in the browser (short enough that a backfill still in progress shows up)."""
    start, end = time_range(from_, to)
    p = {"from": start, "to": end}

    async def fetch():
        (frames, f_info), (arcs, a_info) = await asyncio.gather(
            run("map frames", queries.FRAMES, {**p, "bucket_us": bucket_s * 1_000_000}),
            run("trip arcs", queries.ARCS, {**p, "limit": limit, "sample": sample}),
        )
        return {"frames": [[t, z, n, round(a or 0, 2)] for t, z, n, a in frames],
                "arcs": [list(r) for r in arcs], "queries": [f_info, a_info]}
    response.headers["Cache-Control"] = "public, max-age=600"
    return await cached(("chunk", from_, to, bucket_s, limit, sample), 1800, fetch)


@app.get("/api/weather")
async def weather(from_: int = Query(alias="from"), to: int = Query()):
    start, end = time_range(from_, to)

    async def fetch():
        rows, info = await run("weather", queries.WEATHER, {"from": start, "to": end})
        return {"data": [list(r) for r in rows], "queries": [info]}
    return await cached(("weather", from_, to), 300, fetch)


@app.get("/api/insights/rain")
async def rain():
    async def fetch():
        rows, info = await run("rain vs dry", queries.RAIN,
                               {"from": settings.history_from, "to": settings.history_to})
        cols = ["sky", "trips", "hours", "avg_tip_pct", "avg_duration", "avg_total"]
        return {"data": [dict(zip(cols, r)) for r in rows], "queries": [info]}
    return await cached(("rain",), 600, fetch)


@app.get("/api/insights/then-vs-now")
async def then_vs_now():
    async def fetch():
        now = ny_now()
        then = to_history(now)
        hour = timedelta(hours=1)
        rows, info = await run("then vs now", queries.THEN_VS_NOW, {
            "now_from": now - hour, "now_to": now, "then_from": then - hour, "then_to": then})
        cols = ["period", "origin", "trips", "avg_total", "avg_tip_pct", "airport_trips"]
        return {"data": [dict(zip(cols, r)) for r in rows],
                "now": [ms(now - hour), ms(now)], "then": [ms(then - hour), ms(then)],
                "queries": [info]}
    return await cached(("then-vs-now",), 10, fetch)


@app.get("/api/insights/quality")
async def quality():
    async def fetch():
        rows, info = await run("quarantine by reason", queries.QUALITY)
        return {"data": [{"reason": r, "origin": o, "trips": n} for r, o, n in rows], "queries": [info]}
    return await cached(("quality",), 30, fetch)


@app.get("/api/live")
async def live(request: Request):
    q = poller.subscribe()

    async def stream():
        try:
            while not await request.is_disconnected():
                try:
                    event = await asyncio.wait_for(q.get(), timeout=15)
                    yield f"data: {json.dumps(event)}\n\n"
                except asyncio.TimeoutError:
                    yield ": keep-alive\n\n"
        finally:
            poller.unsubscribe(q)

    return StreamingResponse(stream(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


class SurgeRequest(BaseModel):
    zone_id: int = Field(ge=1, le=263)
    count: int = Field(200, ge=1, le=1000)


@app.post("/api/surge")
async def surge(req: SurgeRequest):
    from .surge import surge as produce  # imported lazily: needs Kafka settings
    if req.zone_id not in map(int, ZONES):
        raise HTTPException(400, "unknown zone")
    try:
        sent = await asyncio.to_thread(produce, req.zone_id, req.count)
    except Exception as e:
        raise HTTPException(502, f"surge: {e}") from e
    return {"sent": sent, "zone": ZONES[str(req.zone_id)], "topic": settings.raw_topic}


if os.path.isdir(WEB_DIST):
    app.mount("/assets", StaticFiles(directory=os.path.join(WEB_DIST, "assets")), name="assets")

    @app.get("/{path:path}")
    async def spa(path: str):
        file = os.path.join(WEB_DIST, path)
        if path and os.path.isfile(file) and os.path.realpath(file).startswith(os.path.realpath(WEB_DIST)):
            return FileResponse(file)
        return FileResponse(os.path.join(WEB_DIST, "index.html"))
