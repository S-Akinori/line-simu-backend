from datetime import datetime, timedelta, timezone
from uuid import UUID

from line_simu.scheduler.setup import get_scheduler
from line_simu.scheduler.tasks import send_reminder_message


def schedule_reminder(
    session_id: UUID,
    line_user_id: str,
    delay_hours: float,
    message_template: str,
) -> None:
    """Schedule a reminder message to be sent after a delay."""
    scheduler = get_scheduler()
    run_date = datetime.now(timezone.utc) + timedelta(hours=delay_hours)

    scheduler.add_job(
        send_reminder_message,
        trigger="date",
        run_date=run_date,
        id=f"reminder_{session_id}_{line_user_id}",
        args=[line_user_id, str(session_id), message_template],
        replace_existing=True,
    )


def cancel_session_reminders(session_id: UUID) -> None:
    """Cancel all pending reminders for a session."""
    scheduler = get_scheduler()
    jobs = scheduler.get_jobs()
    prefix = f"reminder_{session_id}_"
    for job in jobs:
        if job.id.startswith(prefix):
            job.remove()
