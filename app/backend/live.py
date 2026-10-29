"""Shared live poller: one set of SQL queries every LIVE_POLL_S seconds, fanned out to all
SSE subscribers. Runs only while at least one browser is connected."""
import asyncio
import logging
import time
from datetime import timedelta

from . import queries
from .config import settings
from .db import query
from .timeutil import ms, ny_now

log = logging.getLogger("live")

RECENT_WINDOW = timedelta(seconds=30)
ZONE_WINDOW = timedelta(minutes=15)
SEEN_TTL_S = 120


class LivePoller:
    def __init__(self) -> None:
        self.subscribers: set[asyncio.Queue] = set()
        self.task: asyncio.Task | None = None
        self.seen: dict[tuple, float] = {}
        self.use_struct = True

    def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=10)
        self.subscribers.add(q)
        if self.task is None or self.task.done():
            self.task = asyncio.create_task(self._run())
        return q

    def unsubscribe(self, q: asyncio.Queue) -> None:
        self.subscribers.discard(q)

    async def _run(self) -> None:
        while self.subscribers:
            started = time.monotonic()
            try:
                event = await self._tick()
            except Exception as e:  # keep polling; surface the error to the UI
                log.warning("live poll failed: %s", e)
                event = {"error": str(e).strip()}
            for q in list(self.subscribers):
                if q.full():
                    q.get_nowait()  # drop the oldest event for slow clients
                q.put_nowait(event)
            await asyncio.sleep(max(0.0, settings.live_poll_s - (time.monotonic() - started)))

    async def _recent(self, since) -> tuple[list[list], dict]:
        if self.use_struct:
            try:
                rows, info = await query("live: recent trips", queries.LIVE_RECENT, {"since": since})
                return [[(r[0], r[1]), *r[2:]] for r in rows], info
            except Exception as e:
                log.warning("(redpanda) struct access failed, using fallback dedupe: %s", e)
                self.use_struct = False
        rows, info = await query("live: recent trips", queries.LIVE_RECENT_FALLBACK, {"since": since})
        return [[tuple(r), *r] for r in rows], info

    async def _tick(self) -> dict:
        now = ny_now()
        (recent, recent_info), (zones, zones_info) = await asyncio.gather(
            self._recent(now - RECENT_WINDOW),
            query("live: zone counts (15 min)", queries.LIVE_ZONES, {"since": now - ZONE_WINDOW}),
        )
        mono = time.monotonic()
        self.seen = {k: v for k, v in self.seen.items() if mono - v < SEEN_TTL_S}
        new = []
        for key, *row in recent:
            if key not in self.seen:
                self.seen[key] = mono
                new.append(row)  # [pickup_ms, dropoff_ms, pu, do, total, origin]
        newest = max((r[1] for r in recent), default=None)  # r = [key, pickup_ms, ...]
        return {
            "now": ms(now),
            "new": new,
            "zones": [[z, n] for z, n in zones],
            "freshness_ms": ms(now) - newest if newest is not None else None,
            "iceberg_lag_ms": settings.iceberg_lag_ms,
            "queries": [recent_info, zones_info],
        }


poller = LivePoller()
