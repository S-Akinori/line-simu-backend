import logging
from uuid import UUID

from line_simu.db.repositories.answer import get_session_answers_by_key
from line_simu.db.repositories.route import (
    get_first_route_question,
    get_next_route_question,
    get_route_connections,
)
from line_simu.engine.condition import check_display_conditions
from line_simu.schemas.question import Question
from line_simu.schemas.session import Session

logger = logging.getLogger(__name__)


async def resolve_next(
    session: Session,
    current_question: Question,
    current_route_id: UUID,
    channel_id: UUID,
) -> tuple[UUID, Question] | None:
    """Determine the next (route_id, question) to present.

    1. Try next question within the current route.
    2. If route is exhausted, evaluate route_connections and enter the first matching route.
    3. Return None when the flow is complete (no matching connections).
    """
    # 1. Next question in the same route
    next_q = await get_next_route_question(current_route_id, current_question.id)
    if next_q is not None:
        return (current_route_id, next_q)

    # 2. End of route — evaluate connections
    session_answers = await get_session_answers_by_key(session.id)
    connections = await get_route_connections(current_route_id)

    for conn in connections:
        # Empty conditions = unconditional fallback; otherwise check the condition groups
        if check_display_conditions(conn.conditions, session_answers):
            first_q = await get_first_route_question(conn.to_route_id)
            if first_q is not None:
                return (conn.to_route_id, first_q)
            logger.warning("Route %s has no active questions, skipping", conn.to_route_id)

    return None  # Flow complete
