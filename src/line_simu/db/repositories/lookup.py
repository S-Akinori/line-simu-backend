import json

from line_simu.db.connection import get_pool


class LookupEntry:
    def __init__(self, id, lookup_table_id, key_values, result_value, created_at):
        self.id = id
        self.lookup_table_id = lookup_table_id
        self.key_values = key_values
        self.result_value = result_value
        self.created_at = created_at


async def find_lookup_entry(
    table_name: str,
    key_values: dict[str, str],
) -> LookupEntry | None:
    """Find a lookup entry by table name and key values using JSONB containment."""
    pool = await get_pool()
    row = await pool.fetchrow(
        """SELECT le.*
           FROM lookup_entries le
           JOIN lookup_tables lt ON lt.id = le.lookup_table_id
           WHERE lt.table_name = $1
             AND le.key_values @> $2::jsonb""",
        table_name,
        json.dumps(key_values),
    )
    if row is None:
        return None
    data = dict(row)
    if isinstance(data.get("key_values"), str):
        data["key_values"] = json.loads(data["key_values"])
    return LookupEntry(**data)
