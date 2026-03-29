import logging
from uuid import UUID

from linebot.v3.messaging import PushMessageRequest

from line_simu.db.connection import get_pool
from line_simu.db.repositories.answer import get_session_answers_with_labels
from line_simu.db.repositories.channel import LineChannel
from line_simu.line.client import get_messaging_api
from line_simu.line.messages import build_text_message
from line_simu.schemas.session import Session
from line_simu.services.email import send_email

logger = logging.getLogger(__name__)


async def _get_admin_line_user_ids(channel_id: UUID) -> list[str]:
    """Get LINE user IDs of active admin/super_admin profiles for a channel."""
    pool = await get_pool()
    rows = await pool.fetch(
        """SELECT p.line_notify_user_id
           FROM profiles p
           WHERE p.is_active = true
             AND p.line_notify_user_id IS NOT NULL
             AND (
               p.role = 'super_admin'
               OR (
                 p.role = 'admin'
                 AND EXISTS (
                   SELECT 1 FROM profile_channels pc
                   WHERE pc.profile_id = p.id AND pc.channel_id = $1
                 )
               )
             )""",
        channel_id,
    )
    return [row["line_notify_user_id"] for row in rows]


async def notify_admin_completion(
    session: Session, result: dict, channel: LineChannel
) -> None:
    """Notify admins when a user completes a simulation."""
    # Build answers section
    answers = await get_session_answers_with_labels(session.id)
    answer_lines = [
        f"  {item['label']}: {item['value']}"
        for item in answers.values()
    ]

    # Build results section
    result_lines = []
    for _name, item in result.items():
        label = item.get("label", _name)
        if item.get("error"):
            result_lines.append(f"  {label}: エラー")
        else:
            result_lines.append(
                f"  {label}: {item.get('formatted', item.get('value', 'N/A'))}"
            )

    parts = [
        f"[シミュレーション完了] {channel.name}",
        f"セッションID: {session.id}",
    ]
    if answer_lines:
        parts.append("回答内容:\n" + "\n".join(answer_lines))
    parts.append("計算結果:\n" + "\n".join(result_lines))

    text = "\n".join(parts)

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
    """Push a notification to all admin LINE users configured for this channel."""
    user_ids = await _get_admin_line_user_ids(channel.id)
    if not user_ids:
        logger.warning(
            "No admin LINE user IDs configured for channel %s, skipping",
            channel.name,
        )
        return

    api = get_messaging_api(channel.channel_access_token)
    for line_user_id in user_ids:
        try:
            await api.push_message(
                PushMessageRequest(
                    to=line_user_id,
                    messages=[build_text_message(text)],
                )
            )
        except Exception:
            logger.exception(
                "Failed to send admin notification to %s for channel %s",
                line_user_id,
                channel.name,
            )
