import json
from uuid import UUID

from line_simu.db.connection import get_pool
from line_simu.schemas.session import Session


async def get_or_create_session(line_user_id_uuid: UUID) -> Session:
    """Get an active session or create a new one."""
    pool = await get_pool()

    # Try to find an existing in-progress session
    row = await pool.fetchrow(
        """SELECT * FROM sessions
           WHERE line_user_id = $1 AND status = 'in_progress'
           ORDER BY created_at DESC
           LIMIT 1""",
        line_user_id_uuid,
    )
    if row is not None:
        return _row_to_session(row)

    # Create a new session
    row = await pool.fetchrow(
        """INSERT INTO sessions (line_user_id, status)
           VALUES ($1, 'in_progress')
           RETURNING *""",
        line_user_id_uuid,
    )
    return _row_to_session(row)


async def update_session_question(
    session_id: UUID,
    question_id: UUID,
) -> None:
    pool = await get_pool()
    await pool.execute(
        """UPDATE sessions
           SET current_question_id = $2, updated_at = now()
           WHERE id = $1""",
        session_id,
        question_id,
    )


async def update_session_route_and_question(
    session_id: UUID,
    route_id: UUID,
    question_id: UUID,
) -> None:
    pool = await get_pool()
    await pool.execute(
        """UPDATE sessions
           SET current_route_id = $2, current_question_id = $3, updated_at = now()
           WHERE id = $1""",
        session_id,
        route_id,
        question_id,
    )


async def complete_session(session_id: UUID, result: dict) -> None:
    pool = await get_pool()
    await pool.execute(
        """UPDATE sessions
           SET status = 'completed',
               result = $2::jsonb,
               completed_at = now(),
               updated_at = now()
           WHERE id = $1""",
        session_id,
        json.dumps(result),
    )


async def reset_and_create_session(line_user_id_uuid: UUID) -> Session:
    """Abandon any in-progress session and create a fresh one."""
    pool = await get_pool()
    await pool.execute(
        """UPDATE sessions
           SET status = 'abandoned',
               abandoned_at = now(),
               updated_at = now()
           WHERE line_user_id = $1 AND status = 'in_progress'""",
        line_user_id_uuid,
    )
    row = await pool.fetchrow(
        """INSERT INTO sessions (line_user_id, status)
           VALUES ($1, 'in_progress')
           RETURNING *""",
        line_user_id_uuid,
    )
    return _row_to_session(row)


async def abandon_session(session_id: UUID) -> None:
    pool = await get_pool()
    await pool.execute(
        """UPDATE sessions
           SET status = 'abandoned',
               abandoned_at = now(),
               updated_at = now()
           WHERE id = $1""",
        session_id,
    )


def _row_to_session(row) -> Session:
    """Convert an asyncpg Record to a Session, handling JSONB fields."""
    data = dict(row)
    if isinstance(data.get("result"), str):
        data["result"] = json.loads(data["result"])
    return Session(**data)
