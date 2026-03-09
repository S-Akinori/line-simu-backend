from uuid import UUID

from line_simu.db.connection import get_pool
from line_simu.schemas.answer import Answer


async def save_answer(
    session_id: UUID,
    question_id: UUID,
    value: str,
    numeric: float | None = None,
) -> None:
    """Save or upsert an answer for a session/question pair."""
    pool = await get_pool()
    await pool.execute(
        """INSERT INTO answers (session_id, question_id, answer_value, answer_numeric)
           VALUES ($1, $2, $3, $4)
           ON CONFLICT (session_id, question_id) DO UPDATE
             SET answer_value = $3, answer_numeric = $4, answered_at = now()""",
        session_id,
        question_id,
        value,
        numeric,
    )


async def get_session_answers(session_id: UUID) -> list[Answer]:
    pool = await get_pool()
    rows = await pool.fetch(
        "SELECT * FROM answers WHERE session_id = $1 ORDER BY answered_at ASC",
        session_id,
    )
    return [Answer(**dict(row)) for row in rows]


async def get_session_answers_by_key(session_id: UUID) -> dict[str, str]:
    """Get all answers for a session keyed by question_key."""
    pool = await get_pool()
    rows = await pool.fetch(
        """SELECT q.question_key, a.answer_value
           FROM answers a
           JOIN questions q ON q.id = a.question_id
           WHERE a.session_id = $1""",
        session_id,
    )
    return {row["question_key"]: row["answer_value"] for row in rows}


async def get_session_answers_with_labels(session_id: UUID) -> dict[str, dict]:
    """Get all answers for a session keyed by question_key, including question label.

    For choice questions (image_carousel / button), the stored value is the option's
    internal value; this query resolves it to the option's display label instead.
    For free_text questions, answer_value is used as-is.
    """
    pool = await get_pool()
    rows = await pool.fetch(
        """SELECT q.question_key,
                  q.content AS question_label,
                  COALESCE(qo.label, a.answer_value) AS display_value
           FROM answers a
           JOIN questions q ON q.id = a.question_id
           LEFT JOIN question_options qo
             ON qo.question_id = a.question_id AND qo.value = a.answer_value
           WHERE a.session_id = $1""",
        session_id,
    )
    return {
        row["question_key"]: {"label": row["question_label"], "value": row["display_value"]}
        for row in rows
    }


async def get_answer_by_question_key(
    session_id: UUID,
    question_key: str,
) -> Answer | None:
    pool = await get_pool()
    row = await pool.fetchrow(
        """SELECT a.*
           FROM answers a
           JOIN questions q ON q.id = a.question_id
           WHERE a.session_id = $1 AND q.question_key = $2""",
        session_id,
        question_key,
    )
    return Answer(**dict(row)) if row else None
