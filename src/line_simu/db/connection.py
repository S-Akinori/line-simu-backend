import socket
import urllib.parse

import asyncpg

from line_simu.config import settings

pool: asyncpg.Pool | None = None


def _make_ipv4_dsn(dsn: str) -> str:
    """Replace the hostname in a DSN with its resolved IPv4 address.

    uvloop's async DNS resolver may return IPv6 addresses first even when IPv6
    routing is unavailable, causing ENETUNREACH. Resolving to IPv4 at startup
    (sync getaddrinfo is acceptable here) avoids the issue.
    Falls back to the original DSN if resolution fails.
    """
    parsed = urllib.parse.urlparse(dsn)
    if not parsed.hostname:
        return dsn
    try:
        info = socket.getaddrinfo(parsed.hostname, None, socket.AF_INET)
        ipv4 = info[0][4][0]
        netloc = parsed.netloc.replace(parsed.hostname, ipv4, 1)
        return urllib.parse.urlunparse(parsed._replace(netloc=netloc))
    except OSError:
        return dsn


async def init_db_pool() -> None:
    global pool
    pool = await asyncpg.create_pool(
        dsn=_make_ipv4_dsn(settings.database_url),
        min_size=2,
        max_size=10,
    )


async def close_db_pool() -> None:
    if pool:
        await pool.close()


async def get_pool() -> asyncpg.Pool:
    assert pool is not None, "DB pool not initialized"
    return pool
