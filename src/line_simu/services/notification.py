import logging
from uuid import UUID

from linebot.v3.messaging import PushMessageRequest

from line_simu.db.connection import get_pool
from line_simu.db.repositories.channel import LineChannel
from line_simu.line.client import get_messaging_api
from line_simu.line.messages import build_text_message
from line_simu.schemas.session import Session
from line_simu.services.email import send_email

logger = logging.getLogger(__name__)


async def notify_admin_completion(
    session: Session, result: dict, channel: LineChannel
) -> None:
    """Notify admins when a user completes a simulation."""
    result_lines = []
    for _name, item in result.items():
        label = item.get("label", _name)
        if item.get("error"):
            result_lines.append(f"  {label}: エラー")
        else:
            result_lines.append(
                f"  {label}: {item.get('formatted', item.get('value', 'N/A'))}"
            )

    text = (
        f"[シミュレーション完了] {channel.name}\n"
        f"セッションID: {session.id}\n"
        f"結果:\n" + "\n".join(result_lines)
    )

    await _push_admin_message(text, channel)
    await send_email(
        subject=f"[LINE Simu] シミュレーション完了 - {channel.name}",
        body=text,
    )
    await save_notification_record(str(session.id), "session_completed")


async def notify_admin_abandonment(session: Session, channel: LineChannel) -> None:
    """Notify admins when a session is abandoned."""
    text = (
        f"[セッション放棄] {channel.name}\n"
        f"セッションID: {session.id}\n"
        f"リマインダー送信回数: {session.reminder_count}"
    )
    await _push_admin_message(text, channel)
    await send_email(
        subject=f"[LINE Simu] セッション放棄 - {channel.name}",
        body=text,
    )
    await save_notification_record(str(session.id), "session_abandoned")


async def save_notification_record(session_id: str, notification_type: str) -> None:
    pool = await get_pool()
    await pool.execute(
        """INSERT INTO admin_notifications
             (session_id, notification_type, status, sent_at)
           VALUES ($1::uuid, $2::notification_type, 'sent', now())""",
        session_id,
        notification_type,
    )


async def _push_admin_message(text: str, channel: LineChannel) -> None:
    """Push a notification to the admin LINE group configured for this channel."""
    if not channel.admin_line_group_id:
        logger.warning(
            "admin_line_group_id not configured for channel %s, skipping",
            channel.name,
        )
        return

    try:
        api = get_messaging_api(channel.channel_access_token)
        await api.push_message(
            PushMessageRequest(
                to=channel.admin_line_group_id,
                messages=[build_text_message(text)],
            )
        )
    except Exception:
        logger.exception("Failed to send admin notification for channel %s", channel.name)
