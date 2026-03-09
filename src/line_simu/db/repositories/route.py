import json
from uuid import UUID

from line_simu.db.connection import get_pool


class Route:
    def __init__(
        self,
        id,
        channel_id,
        name,
        description,
        sort_order,
        created_at,
        updated_at,
        **_kwargs,
    ):
        self.id = id
        self.channel_id = channel_id
        self.name = name
        self.description = description
        self.sort_order = sort_order
        self.created_at = created_at
        self.updated_at = updated_at


class RouteConnection:
    def __init__(self, id, from_route_id, to_route_id, conditions, sort_order, **_kwargs):
        self.id = id
        self.from_route_id = from_route_id
        self.to_route_id = to_route_id
        self.conditions = conditions if isinstance(conditions, list) else json.loads(conditions)
        self.sort_order = sort_order


async def get_routes_for_channel(channel_id: UUID) -> list[Route]:
    pool = await get_pool()
    rows = await pool.fetch(
        "SELECT * FROM routes WHERE channel_id = $1 ORDER BY sort_order ASC, created_at ASC",
        channel_id,
    )
    return [Route(**dict(row)) for row in rows]


async def get_route_by_id(route_id: UUID) -> Route | None:
    pool = await get_pool()
    row = await pool.fetchrow("SELECT * FROM routes WHERE id = $1", route_id)
    return Route(**dict(row)) if row else None


async def get_first_route(channel_id: UUID) -> Route | None:
    pool = await get_pool()
    row = await pool.fetchrow(
        "SELECT * FROM routes WHERE channel_id = $1 ORDER BY sort_order ASC, created_at ASC LIMIT 1",
        channel_id,
    )
    return Route(**dict(row)) if row else None


async def get_first_route_question(route_id: UUID):
    """Return the first Question in the route (lowest sort_order)."""
    from line_simu.db.repositories.question import _row_to_question  # noqa: PLC0415

    pool = await get_pool()
    row = await pool.fetchrow(
        """SELECT q.* FROM questions q
           JOIN route_questions rq ON rq.question_id = q.id
           WHERE rq.route_id = $1 AND q.is_active = true
           ORDER BY rq.sort_order ASC
           LIMIT 1""",
        route_id,
    )
    return _row_to_question(row) if row else None


async def get_next_route_question(route_id: UUID, current_question_id: UUID):
    """Return the next active Question in the route after current_question_id."""
    from line_simu.db.repositories.question import _row_to_question  # noqa: PLC0415

    pool = await get_pool()
    current_order = await pool.fetchval(
        "SELECT sort_order FROM route_questions WHERE route_id = $1 AND question_id = $2",
        route_id,
        current_question_id,
    )
    if current_order is None:
        return None
    row = await pool.fetchrow(
        """SELECT q.* FROM questions q
           JOIN route_questions rq ON rq.question_id = q.id
           WHERE rq.route_id = $1 AND rq.sort_order > $2 AND q.is_active = true
           ORDER BY rq.sort_order ASC
           LIMIT 1""",
        route_id,
        current_order,
    )
    return _row_to_question(row) if row else None


async def get_route_connections(from_route_id: UUID) -> list[RouteConnection]:
    pool = await get_pool()
    rows = await pool.fetch(
        "SELECT * FROM route_connections WHERE from_route_id = $1 ORDER BY sort_order ASC",
        from_route_id,
    )
    return [RouteConnection(**dict(row)) for row in rows]
