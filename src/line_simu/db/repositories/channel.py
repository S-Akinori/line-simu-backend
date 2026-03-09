from uuid import UUID

from line_simu.db.connection import get_pool


class LineChannel:
    id: UUID
    name: str
    channel_id: str
    channel_secret: str
    channel_access_token: str
    admin_line_group_id: str | None
    gas_webhook_url: str | None
    webhook_path: str
    start_keywords: list[str]
    # keyword -> route_id (preferred) or start_question_id (legacy)
    start_keyword_routes: dict[str, UUID]
    is_active: bool

    def __init__(self, **kwargs: object) -> None:
        for key, value in kwargs.items():
            setattr(self, key, value)


async def _load_keyword_routes(pool, channel_id: UUID) -> dict[str, UUID]:
    rows = await pool.fetch(
        """SELECT keyword, route_id, start_question_id FROM start_keyword_routes
           WHERE line_channel_id = $1""",
        channel_id,
    )
    # Prefer route_id; fall back to start_question_id for backward compat
    return {
        row["keyword"]: row["route_id"] or row["start_question_id"]
        for row in rows
        if row["route_id"] is not None or row["start_question_id"] is not None
    }


async def get_channel_by_webhook_path(webhook_path: str) -> LineChannel | None:
    pool = await get_pool()
    row = await pool.fetchrow(
        """SELECT id, name, channel_id, channel_secret, channel_access_token,
                  admin_line_group_id, gas_webhook_url, webhook_path, start_keywords, is_active
           FROM line_channels
           WHERE webhook_path = $1 AND is_active = true""",
        webhook_path,
    )
    if not row:
        return None
    channel = LineChannel(**dict(row), start_keyword_routes={})
    channel.start_keyword_routes = await _load_keyword_routes(pool, channel.id)
    return channel


async def get_all_channels() -> list[LineChannel]:
    pool = await get_pool()
    rows = await pool.fetch(
        """SELECT id, name, channel_id, channel_secret, channel_access_token,
                  admin_line_group_id, gas_webhook_url, webhook_path, start_keywords, is_active
           FROM line_channels
           WHERE is_active = true
           ORDER BY created_at ASC"""
    )
    return [LineChannel(**dict(row), start_keyword_routes={}) for row in rows]
