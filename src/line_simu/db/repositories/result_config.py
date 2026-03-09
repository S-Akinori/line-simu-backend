import json
from uuid import UUID

from line_simu.db.connection import get_pool


def _row_to_config(row) -> "ResultDisplayConfig":
    data = dict(row)
    cond = data.get("condition")
    if isinstance(cond, str):
        cond = json.loads(cond)
    if isinstance(cond, dict):
        # Normalize: display_conditions expect list[dict], not single dict
        cond = [cond]
    data["condition"] = cond
    return ResultDisplayConfig(**data)


class ResultDisplayConfig:
    id: UUID
    line_channel_id: UUID
    name: str
    trigger_question_id: UUID | None
    trigger_route_id: UUID | None
    intro_message: str
    body_template: str | None
    closing_message: str | None
    display_order: int
    is_active: bool
    condition: list | None

    def __init__(self, **kwargs: object) -> None:
        for key, value in kwargs.items():
            setattr(self, key, value)
        if not hasattr(self, "condition"):
            self.condition = None
        if not hasattr(self, "body_template"):
            self.body_template = None


async def get_configs_triggered_by_question(question_id: UUID) -> list[ResultDisplayConfig]:
    """Backward-compat: configs triggered by a specific question answer."""
    pool = await get_pool()
    rows = await pool.fetch(
        """SELECT * FROM result_display_configs
           WHERE trigger_question_id = $1 AND is_active = true
           ORDER BY display_order ASC""",
        question_id,
    )
    return [_row_to_config(row) for row in rows]


async def get_configs_triggered_by_route(route_id: UUID) -> list[ResultDisplayConfig]:
    """Return configs that should fire when the given route completes."""
    pool = await get_pool()
    rows = await pool.fetch(
        """SELECT * FROM result_display_configs
           WHERE trigger_route_id = $1 AND is_active = true
           ORDER BY display_order ASC""",
        route_id,
    )
    return [_row_to_config(row) for row in rows]


async def get_end_configs(channel_id: UUID) -> list[ResultDisplayConfig]:
    """Return configs that fire at the very end (no trigger set)."""
    pool = await get_pool()
    rows = await pool.fetch(
        """SELECT * FROM result_display_configs
           WHERE line_channel_id = $1
             AND trigger_question_id IS NULL
             AND trigger_route_id IS NULL
             AND is_active = true
           ORDER BY display_order ASC""",
        channel_id,
    )
    return [_row_to_config(row) for row in rows]
