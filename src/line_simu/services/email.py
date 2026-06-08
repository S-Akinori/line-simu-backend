import asyncio
import logging
import smtplib
from email.mime.text import MIMEText

from line_simu.config import settings

logger = logging.getLogger(__name__)


def _send_smtp(subject: str, body: str, to: str) -> None:
    """Synchronous SMTP send (runs in a thread via asyncio.to_thread)."""
    from_addr = settings.smtp_from or settings.smtp_user or ""
    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = to

    with smtplib.SMTP(settings.smtp_host, settings.smtp_port) as smtp:
        smtp.ehlo()
        smtp.starttls()
        if settings.smtp_user and settings.smtp_password:
            smtp.login(settings.smtp_user, settings.smtp_password)
        smtp.sendmail(from_addr, [to], msg.as_string())


async def send_email(subject: str, body: str, to: str | None = None) -> None:
    """Send an email. If `to` is omitted, sends to admin_email.

    No-op if SMTP is not configured or no recipient is available.
    Raises on SMTP error so callers can handle user-vs-admin send independently.
    """
    recipient = to or settings.admin_email
    if not settings.smtp_host or not recipient:
        return
    await asyncio.to_thread(_send_smtp, subject, body, recipient)
