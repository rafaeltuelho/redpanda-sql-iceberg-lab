"""Async connection pool to Redpanda SQL and a small query helper that times each call."""
import re
import time
from datetime import datetime

from psycopg_pool import AsyncConnectionPool

from .config import settings

pool = AsyncConnectionPool(
    settings.sql_conninfo(),
    min_size=1,
    max_size=8,
    open=False,
    timeout=10,  # fail fast in the UI when Redpanda SQL is unreachable
    # Bind parameters, but don't create server-side prepared statements
    kwargs={"autocommit": True, "prepare_threshold": None},
)


def display_sql(sql: str, params: dict) -> str:
    """The SQL with parameters inlined, for the UI's SQL panel only (never executed)."""
    def render(m: re.Match) -> str:
        v = params[m.group(1)]
        if isinstance(v, datetime):
            return f"'{v.strftime('%Y-%m-%d %H:%M:%S')}'"
        return repr(v) if isinstance(v, str) else str(v)
    flat = " ".join(line.split("--")[0] for line in sql.splitlines())
    return re.sub(r"%\((\w+)\)s", render, " ".join(flat.split())).replace("%%", "%")


async def query(name: str, sql: str, params: dict | None = None) -> tuple[list[tuple], dict]:
    """Run a query; return (rows, info) where info carries the SQL, latency and row count."""
    params = params or {}
    start = time.perf_counter()
    async with pool.connection() as conn:
        cur = await conn.execute(sql, params)
        rows = await cur.fetchall()
    info = {"name": name, "sql": display_sql(sql, params),
            "ms": round((time.perf_counter() - start) * 1000, 1), "rows": len(rows)}
    return rows, info
