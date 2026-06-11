import logging
from uuid import UUID

from line_simu.db.connection import get_pool
from line_simu.db.repositories.answer import get_session_answers_with_labels
from line_simu.db.repositories.channel import LineChannel
from line_simu.schemas.session import Session
from line_simu.services.email import send_email

logger = logging.getLogger(__name__)


async def _get_admin_emails(channel_id: UUID) -> list[str]:
    """Get email addresses of active super_admin / channel-assigned admin profiles."""
    pool = await get_pool()
    rows = await pool.fetch(
        """SELECT p.email
           FROM profiles p
           WHERE p.is_active = true
             AND p.email IS NOT NULL
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
    return [row["email"] for row in rows]


def _build_answer_lines(answers: dict[str, dict]) -> list[str]:
    return [
        f"  {item['label']}: {item['value']}"
        for item in answers.values()
        if item.get("label") and item.get("value") is not None
    ]


def _build_result_lines(result: dict) -> list[str]:
    lines = []
    for _name, item in result.items():
        label = item.get("label", _name)
        if item.get("error"):
            lines.append(f"  {label}: エラー")
        else:
            lines.append(f"  {label}: {item.get('formatted', item.get('value', 'N/A'))}")
    return lines


_SEP = "─" * 40


async def notify_admin_completion(
    session: Session, result: dict, channel: LineChannel
) -> None:
    """Notify admins (and optionally the user) when a simulation is completed."""
    try:
        answers = await get_session_answers_with_labels(session.id)
        answer_lines = _build_answer_lines(answers)
        result_lines = _build_result_lines(result)

        # 1. User email — independent step; failure does not block admin notification
        user_email = (answers.get("email") or {}).get("value")
        if user_email:
            try:
                user_parts = [
                    f"【{channel.name}】シミュレーション結果",
                    _SEP,
                ]
                if answer_lines:
                    user_parts += ["■ ご回答内容", *answer_lines, ""]
                user_parts += ["■ 計算結果", *result_lines]
                await send_email(
                    subject=f"【{channel.name}】シミュレーション結果",
                    body="\n".join(user_parts),
                    to=user_email,
                )
                logger.info(
                    "User email sent to %s for session %s", user_email, session.id
                )
            except Exception:
                logger.exception(
                    "Failed to send user email to %s for session %s",
                    user_email,
                    session.id,
                )

        # 2. Admin emails (super_admin + channel-assigned admins)
        admin_parts = [
            f"■ チャンネル: {channel.name}",
            f"■ セッションID: {session.id}",
            _SEP,
        ]
        if answer_lines:
            admin_parts += ["【回答内容】", *answer_lines, ""]
        admin_parts += ["【計算結果】", *result_lines]
        admin_text = "\n".join(admin_parts)

        admin_emails = await _get_admin_emails(channel.id)
        if not admin_emails:
            logger.warning(
                "No admin emails configured for channel %s, skipping admin notification",
                channel.name,
            )
        for admin_email in admin_emails:
            try:
                await send_email(
                    subject=f"[LINE Simu] シミュレーション完了 - {channel.name}",
                    body=admin_text,
                    to=admin_email,
                )
            except Exception:
                logger.exception(
                    "Failed to send admin email to %s for session %s",
                    admin_email,
                    session.id,
                )

        # 3. Persist notification record
        await save_notification_record(
            str(session.id), "session_completed", line_user_id=session.line_user_id
        )
    except Exception:
        logger.exception(
            "notify_admin_completion failed for session %s channel %s",
            session.id,
            channel.name,
        )


async def save_notification_record(
    session_id: str,
    notification_type: str,
    line_user_id: UUID | None = None,
) -> None:
    pool = await get_pool()
    await pool.execute(
        """INSERT INTO admin_notifications
             (session_id, notification_type, status, sent_at, line_user_id)
           VALUES ($1::uuid, $2::notification_type, 'sent', now(), $3)""",
        session_id,
        notification_type,
        line_user_id,
    )
