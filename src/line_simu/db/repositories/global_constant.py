from line_simu.db.connection import get_pool


async def get_all_active_global_constants() -> dict[str, float]:
    """Return all active global constants as {name: value} mapping."""
    pool = await get_pool()
    rows = await pool.fetch(
        "SELECT name, value FROM global_constants WHERE is_active = true"
    )
    return {row["name"]: float(row["value"]) for row in rows}
