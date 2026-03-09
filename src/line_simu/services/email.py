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


async def send_email(subject: str, body: str) -> None:
    """Send an email to admin_email. No-op if SMTP is not configured."""
    if not settings.smtp_host or not settings.admin_email:
        return
    try:
        await asyncio.to_thread(_send_smtp, subject, body, settings.admin_email)
    except Exception:
        logger.exception("Failed to send email notification")
