import asyncpg

from line_simu.config import settings

pool: asyncpg.Pool | None = None


async def init_db_pool() -> None:
    global pool
    pool = await asyncpg.create_pool(
        dsn=settings.database_url,
        min_size=2,
        max_size=10,
    )


async def close_db_pool() -> None:
    if pool:
        await pool.close()


async def get_pool() -> asyncpg.Pool:
    assert pool is not None, "DB pool not initialized"
    return pool
