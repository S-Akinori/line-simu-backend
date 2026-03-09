import logging

from linebot.v3.messaging import PushMessageRequest

from line_simu.db.connection import get_pool
from line_simu.db.repositories.session import abandon_session
from line_simu.line.client import get_messaging_api
from line_simu.line.messages import build_text_message
from line_simu.services.notification import (
    notify_admin_abandonment,
    save_notification_record,
)
from line_simu.services.template import render_template

logger = logging.getLogger(__name__)

INACTIVITY_THRESHOLD_HOURS = 48
MAX_REMINDER_COUNT = 2


async def send_reminder_message(
    line_user_id: str,
    session_id: str,
    message_template: str,
    variables: dict[str, str] | None = None,
) -> None:
    """Send a reminder message to a LINE user."""
    try:
        text = render_template(message_template, variables or {})
        api = get_messaging_api()
        await api.push_message(
            PushMessageRequest(
                to=line_user_id,
                messages=[build_text_message(text)],
            )
        )
        # Update reminder tracking
        pool = await get_pool()
        await pool.execute(
            """UPDATE sessions
               SET last_reminder_sent_at = now(),
                   reminder_count = reminder_count + 1,
                   updated_at = now()
               WHERE id = $1::uuid""",
            session_id,
        )
        logger.info("Reminder sent to %s for session %s", line_user_id, session_id)
    except Exception:
        logger.exception(
            "Failed to send reminder to %s for session %s",
            line_user_id,
            session_id,
        )


async def send_admin_notification(
    session_id: str,
    notification_type: str,
) -> None:
    """Send an admin notification about a session event."""
    try:
        await save_notification_record(session_id, notification_type)
    except Exception:
        logger.exception(
            "Failed to save admin notification for session %s", session_id
        )


async def check_registration_deliveries() -> None:
    """Send step delivery messages to users based on days since registration."""
    pool = await get_pool()

    configs = await pool.fetch(
        """SELECT sdc.id, sdc.line_channel_id, sdc.message_template, sdc.delay_hours,
                  lc.channel_access_token, lc.name AS channel_name
           FROM step_delivery_configs sdc
           JOIN line_channels lc ON lc.id = sdc.line_channel_id
           WHERE sdc.trigger = 'registration_delay'
             AND sdc.is_active = true
             AND sdc.line_channel_id IS NOT NULL"""
    )

    for config in configs:
        config_id = config["id"]
        channel_id = config["line_channel_id"]
        delay_hours = float(config["delay_hours"])
        message_template = config["message_template"]
        channel_access_token = config["channel_access_token"]
        channel_name = config["channel_name"] or ""

        # Find users who: belong to this channel, are still following,
        # followed at least delay_hours ago, and haven't received this config yet
        users = await pool.fetch(
            """SELECT lu.id, lu.line_user_id, lu.display_name
               FROM line_users lu
               WHERE lu.line_channel_id = $1
                 AND lu.is_following = true
                 AND lu.followed_at <= now() - ($2 * interval '1 hour')
                 AND NOT EXISTS (
                   SELECT 1 FROM step_delivery_sends sds
                   WHERE sds.config_id = $3
                     AND sds.line_user_id = lu.id
                 )""",
            channel_id,
            delay_hours,
            config_id,
        )

        for user in users:
            line_user_id = user["line_user_id"]
            lu_id = user["id"]
            variables = {
                "display_name": user["display_name"] or "",
                "channel_name": channel_name,
            }
            try:
                text = render_template(message_template, variables)
                api = get_messaging_api(channel_access_token)
                await api.push_message(
                    PushMessageRequest(
                        to=line_user_id,
                        messages=[build_text_message(text)],
                    )
                )
                await pool.execute(
                    """INSERT INTO step_delivery_sends (config_id, line_user_id)
                       VALUES ($1, $2) ON CONFLICT DO NOTHING""",
                    config_id,
                    lu_id,
                )
                logger.info(
                    "Registration delivery sent: config=%s user=%s",
                    config_id,
                    line_user_id,
                )
            except Exception:
                logger.exception(
                    "Failed to send registration delivery to %s for config %s",
                    line_user_id,
                    config_id,
                )


async def check_inactive_sessions() -> None:
    """Check for sessions that have been inactive and send reminders or abandon."""
    pool = await get_pool()

    # Find in-progress sessions inactive for threshold hours
    rows = await pool.fetch(
        f"""SELECT s.id, s.line_user_id AS line_user_uuid, s.reminder_count,
                   lu.line_user_id
            FROM sessions s
            JOIN line_users lu ON lu.id = s.line_user_id
            WHERE s.status = 'in_progress'
              AND lu.is_following = true
              AND s.updated_at < now() - interval '{INACTIVITY_THRESHOLD_HOURS} hours'
            ORDER BY s.updated_at ASC""",
    )

    for row in rows:
        session_id = str(row["id"])
        line_user_id = row["line_user_id"]
        reminder_count = row["reminder_count"]

        if reminder_count >= MAX_REMINDER_COUNT:
            # Abandon session after max reminders
            await abandon_session(row["id"])
            from line_simu.schemas.session import Session

            await send_admin_notification(session_id, "session_abandoned")
            logger.info("Session %s abandoned after %d reminders", session_id, reminder_count)
        else:
            # Send reminder
            await send_reminder_message(
                line_user_id,
                session_id,
                "回答がまだ完了していません。続きはこちらから回答できます。",
            )
