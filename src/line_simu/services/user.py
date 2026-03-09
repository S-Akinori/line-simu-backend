import logging
from uuid import UUID

from line_simu.db.connection import get_pool

logger = logging.getLogger(__name__)


async def upsert_line_user(
    line_user_id: str,
    channel_id: UUID,
    display_name: str | None,
) -> UUID:
    """Create or update a LINE user for a specific channel, returning the internal UUID."""
    pool = await get_pool()
    row = await pool.fetchrow(
        """INSERT INTO line_users (line_user_id, line_channel_id, display_name, is_following)
           VALUES ($1, $2, $3, true)
           ON CONFLICT (line_user_id, line_channel_id) DO UPDATE
             SET display_name = COALESCE($3, line_users.display_name),
                 is_following = true,
                 unfollowed_at = NULL,
                 updated_at = now()
           RETURNING id""",
        line_user_id,
        channel_id,
        display_name,
    )
    return row["id"]


async def mark_user_unfollowed(line_user_id: str, channel_id: UUID) -> None:
    """Mark a LINE user as unfollowed for a specific channel."""
    pool = await get_pool()
    await pool.execute(
        """UPDATE line_users
           SET is_following = false, unfollowed_at = now(), updated_at = now()
           WHERE line_user_id = $1 AND line_channel_id = $2""",
        line_user_id,
        channel_id,
    )


async def get_line_user_uuid(line_user_id: str, channel_id: UUID) -> UUID | None:
    """Get internal UUID for a LINE user + channel combination."""
    pool = await get_pool()
    row = await pool.fetchrow(
        "SELECT id FROM line_users WHERE line_user_id = $1 AND line_channel_id = $2",
        line_user_id,
        channel_id,
    )
    return row["id"] if row else None


async def get_line_user_display_name(line_user_id: str, channel_id: UUID) -> str | None:
    """Get display name for a LINE user."""
    pool = await get_pool()
    row = await pool.fetchrow(
        "SELECT display_name FROM line_users WHERE line_user_id = $1 AND line_channel_id = $2",
        line_user_id,
        channel_id,
    )
    return row["display_name"] if row else None
