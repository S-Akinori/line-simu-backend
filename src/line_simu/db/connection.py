import socket
import urllib.parse

import asyncpg

from line_simu.config import settings

pool: asyncpg.Pool | None = None


def _resolve_ipv4(hostname: str) -> str:
    """Resolve hostname to an IPv4 address.

    uvloop's async DNS resolver may return IPv6 addresses first even when IPv6
    routing is unavailable. Using socket.getaddrinfo with AF_INET forces IPv4
    resolution at startup (sync call is acceptable here).
    """
    info = socket.getaddrinfo(hostname, None, socket.AF_INET)
    return info[0][4][0]


async def init_db_pool() -> None:
    global pool
    parsed = urllib.parse.urlparse(settings.database_url)
    ipv4_host = _resolve_ipv4(parsed.hostname)
    pool = await asyncpg.create_pool(
        host=ipv4_host,
        port=parsed.port or 5432,
        user=parsed.username,
        password=urllib.parse.unquote(parsed.password or ""),
        database=parsed.path.lstrip("/"),
        min_size=2,
        max_size=10,
        ssl="require",
    )


async def close_db_pool() -> None:
    if pool:
        await pool.close()


async def get_pool() -> asyncpg.Pool:
    assert pool is not None, "DB pool not initialized"
    return pool
