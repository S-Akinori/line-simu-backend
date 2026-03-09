import json
from uuid import UUID

from line_simu.db.connection import get_pool
from line_simu.schemas.question import Question, QuestionOption


async def get_question_by_id(question_id: UUID) -> Question | None:
    pool = await get_pool()
    row = await pool.fetchrow(
        "SELECT * FROM questions WHERE id = $1 AND is_active = true",
        question_id,
    )
    return _row_to_question(row) if row else None


async def get_question_by_key(question_key: str, channel_id: UUID) -> Question | None:
    pool = await get_pool()
    row = await pool.fetchrow(
        """SELECT * FROM questions
           WHERE question_key = $1 AND line_channel_id = $2 AND is_active = true""",
        question_key,
        channel_id,
    )
    return _row_to_question(row) if row else None


async def get_first_active_question(channel_id: UUID) -> Question | None:
    pool = await get_pool()
    row = await pool.fetchrow(
        """SELECT * FROM questions
           WHERE line_channel_id = $1 AND is_active = true
           ORDER BY sort_order ASC
           LIMIT 1""",
        channel_id,
    )
    return _row_to_question(row) if row else None


async def get_next_question_by_order(current_order: int, channel_id: UUID) -> Question | None:
    pool = await get_pool()
    row = await pool.fetchrow(
        """SELECT * FROM questions
           WHERE sort_order > $1 AND line_channel_id = $2 AND is_active = true
           ORDER BY sort_order ASC
           LIMIT 1""",
        current_order,
        channel_id,
    )
    return _row_to_question(row) if row else None


async def get_question_options(question_id: UUID) -> list[QuestionOption]:
    pool = await get_pool()
    rows = await pool.fetch(
        """SELECT * FROM question_options
           WHERE question_id = $1
           ORDER BY sort_order ASC""",
        question_id,
    )
    return [QuestionOption(**dict(row)) for row in rows]


async def get_option_by_value(question_id: UUID, value: str) -> QuestionOption | None:
    pool = await get_pool()
    row = await pool.fetchrow(
        """SELECT * FROM question_options
           WHERE question_id = $1 AND value = $2""",
        question_id,
        value,
    )
    return QuestionOption(**dict(row)) if row else None


async def get_questions_after_order(current_order: int, channel_id: UUID) -> list[Question]:
    pool = await get_pool()
    rows = await pool.fetch(
        """SELECT * FROM questions
           WHERE sort_order > $1 AND line_channel_id = $2 AND is_active = true
           ORDER BY sort_order ASC""",
        current_order,
        channel_id,
    )
    return [_row_to_question(row) for row in rows]


def _row_to_question(row) -> Question:
    data = dict(row)
    if isinstance(data.get("conditions"), str):
        data["conditions"] = json.loads(data["conditions"])
    if isinstance(data.get("display_conditions"), str):
        data["display_conditions"] = json.loads(data["display_conditions"])
    if isinstance(data.get("validation"), str):
        data["validation"] = json.loads(data["validation"])
    return Question(**data)
