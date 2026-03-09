import json

from line_simu.db.connection import get_pool


class Formula:
    def __init__(
        self,
        id,
        name,
        description,
        expression,
        variables,
        result_label,
        display_order,
        is_active,
        created_at,
        updated_at,
        result_format,
        value_unit="円",
        value_scale=1,
        value_decimals=0,
        condition=None,
        **_kwargs,
    ):
        self.id = id
        self.name = name
        self.description = description
        self.expression = expression
        self.variables = variables
        self.result_label = result_label
        self.display_order = display_order
        self.is_active = is_active
        self.created_at = created_at
        self.updated_at = updated_at
        self.result_format = result_format
        self.value_unit = value_unit
        self.value_scale = float(value_scale)
        self.value_decimals = int(value_decimals)
        self.condition = condition


async def get_active_formulas(channel_id) -> list[Formula]:
    pool = await get_pool()
    rows = await pool.fetch(
        """SELECT * FROM formulas
           WHERE (line_channel_id = $1 OR line_channel_id IS NULL) AND is_active = true
           ORDER BY display_order ASC""",
        channel_id,
    )
    return [_row_to_formula(row) for row in rows]


async def get_formula_by_name(name: str, channel_id) -> Formula | None:
    pool = await get_pool()
    row = await pool.fetchrow(
        """SELECT * FROM formulas
           WHERE name = $1 AND (line_channel_id = $2 OR line_channel_id IS NULL) AND is_active = true""",
        name,
        channel_id,
    )
    return _row_to_formula(row) if row else None


def _row_to_formula(row) -> Formula:
    data = dict(row)
    if isinstance(data.get("variables"), str):
        data["variables"] = json.loads(data["variables"])
    return Formula(**data)
